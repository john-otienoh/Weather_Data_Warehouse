#!/usr/bin/env python3
"""
  Weather Data Warehouse Pipeline - Generation Stage

  Fetches hourly weather data for 5 Kenyan cities from Open-Meteo,
  validates each city's records, and writes to PostgreSQL.
  Falls back to a timestamped CSV file if no DATABASE_URL is set.


  USAGE
  -----
  Standard hourly run (today's data):
      python weather_ingest.py

  Historical backfill (last 31 days):
      python weather_ingest.py --mode backfill

  Dry run (fetch + validate only, no writes):
      python weather_ingest.py --dry-run

  Via environment variable (for Airflow):
      INGEST_MODE=backfill python weather_ingest.py
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ── Logging setup ─────────────────────────────────────────────────────────────
# Prints messages with a timestamp so you can see exactly what happened when.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── City configuration ────────────────────────────────────────────────────────
# Add or remove cities here. Latitude/longitude from Google Maps.
CITIES = [
    {"name": "Nairobi",  "lat": -1.2833, "lon": 36.8167},
    {"name": "Mombasa",  "lat": -4.0547, "lon": 39.6636},
    {"name": "Kisumu",   "lat": -0.0861, "lon": 34.7289},
    {"name": "Nakuru",   "lat": -0.3072, "lon": 36.0722},
    {"name": "Eldoret",  "lat":  0.5204, "lon": 35.2699},
]

# ── Weather variables to collect ──────────────────────────────────────────────
# Full list at https://open-meteo.com/en/docs#hourly-variables
HOURLY_VARS = [
    "temperature_2m",        # air temperature at 2 metres height (°C)
    "relative_humidity_2m",  # relative humidity (%)
    "apparent_temperature",  # feels-like temperature (°C)
    "precipitation",         # total precipitation mm
    "rain",                  # rain-only mm
    "weather_code",          # WMO weather code (0=clear, 61=rain, 95=thunder...)
    "surface_pressure",      # surface pressure hPa
    "cloud_cover",           # cloud cover %
    "wind_speed_10m",        # wind speed at 10 m (m/s)
    "wind_direction_10m",    # wind direction degrees
    "wind_gusts_10m",        # wind gusts (m/s)
    "visibility",            # visibility in metres
]

# Safe name→index map: if you reorder HOURLY_VARS the extraction won't break
VAR_MAP = {name: i for i, name in enumerate(HOURLY_VARS)}

API_URL = "https://api.open-meteo.com/v1/forecast"

# ── Explicit column order for the staging → silver upsert ────────────────────
# Used by write_to_db() so the INSERT/SELECT never relies on column position.
# This list intentionally excludes "id" — that column is BIGSERIAL and is
# generated automatically by PostgreSQL on insert.
SILVER_COLUMNS = [
    "city", "recorded_at", "fetched_at",
    "temp_celsius", "apparent_temp", "humidity_pct",
    "precipitation_mm", "rain_mm", "weather_code",
    "pressure_hpa", "cloud_cover_pct",
    "wind_speed_mps", "wind_direction_deg", "wind_gusts_mps",
    "visibility_m",
]


# ── Step 1: Build the API client ──────────────────────────────────────────────
def build_client() -> openmeteo_requests.Client:
    """
    Creates an API client with:
      - Caching: saves results locally for 30 min (avoids hitting API twice)
      - Retry:   automatically retries on network failures (up to 5 times)
    """
    cache_dir = Path(__file__).parent / ".openmeteo_cache"
    cache_dir.mkdir(exist_ok=True)

    session = requests_cache.CachedSession(
        str(cache_dir / "cache"), expire_after=1800
    )
    retry_session = retry(session, retries=5, backoff_factor=0.2)
    log.info("API client ready (cache: %s)", cache_dir)
    return openmeteo_requests.Client(session=retry_session)


# ── Step 2: Build API parameters ─────────────────────────────────────────────
def build_params(mode: str = "live") -> dict:
    """
    mode='live'     → fetch today's 24 hourly readings (one full day)
    mode='backfill' → fetch the last 31 days (used to seed the database)
    """
    params = {
        "latitude":  [c["lat"] for c in CITIES],
        "longitude": [c["lon"] for c in CITIES],
        "hourly":    HOURLY_VARS,
        "timezone":  "auto",  # Open-Meteo detects Africa/Nairobi from coordinates
    }
    if mode == "backfill":
        params["past_days"] = 31
        log.info("Mode: BACKFILL — fetching 31 days of history")
    else:
        params["forecast_days"] = 1
        log.info("Mode: LIVE — fetching today (24 rows per city)")
    return params


# ── Step 3: Call the API ──────────────────────────────────────────────────────
def fetch_weather(client, params: dict) -> list:
    """Calls Open-Meteo and returns one response object per city."""
    log.info("Calling Open-Meteo for %d cities ...", len(CITIES))
    responses = client.weather_api(API_URL, params=params)
    log.info("Received %d responses", len(responses))
    return responses


# ── Step 4: Parse one city response into a DataFrame ─────────────────────────
def parse_response(response, city: dict, fetched_at: datetime) -> pd.DataFrame:
    """
    Converts the API response for one city into a clean pandas DataFrame.
    Each row = one hour of data for that city.
    """
    hourly = response.Hourly()

    # Use VAR_MAP so the extraction never silently breaks if order changes
    def get(var_name: str):
        return hourly.Variables(VAR_MAP[var_name]).ValuesAsNumpy()

    # Build the timestamps for each hourly reading
    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    # Column order here matches SILVER_COLUMNS exactly (minus "id").
    # Keeping these in sync is good practice, even though write_to_db()
    # no longer depends on it for correctness.
    df = pd.DataFrame({
        "city":               city["name"],
        "recorded_at":        times,
        "fetched_at":         fetched_at,          # when we pulled this data
        "temp_celsius":       get("temperature_2m"),
        "apparent_temp":      get("apparent_temperature"),
        "humidity_pct":       get("relative_humidity_2m"),
        "precipitation_mm":   get("precipitation"),
        "rain_mm":            get("rain"),
        "weather_code":       get("weather_code"),
        "pressure_hpa":       get("surface_pressure"),
        "cloud_cover_pct":    get("cloud_cover"),
        "wind_speed_mps":     get("wind_speed_10m"),
        "wind_direction_deg": get("wind_direction_10m"),
        "wind_gusts_mps":     get("wind_gusts_10m"),
        "visibility_m":       get("visibility"),
    })

    log.info("  [%s] %d rows parsed", city["name"], len(df))
    return df


# ── Step 5: Validate the data ─────────────────────────────────────────────────
def validate(df: pd.DataFrame, city_name: str) -> bool:
    """
    Quick sanity checks before writing to the database.
    Returns False if something looks wrong (empty or all nulls).
    """
    if df.empty:
        log.error("  [%s] EMPTY — skipping", city_name)
        return False

    nulls = df["temp_celsius"].isna().sum()
    if nulls > 20:
        log.warning("  [%s] %d null temperatures", city_name, nulls)

    out_of_range = (~df["temp_celsius"].dropna().between(-5, 45)).sum()
    if out_of_range:
        log.warning("  [%s] %d suspicious temperatures", city_name, out_of_range)

    log.info("  [%s] validation OK", city_name)
    return True


# ── Step 6: Write to Bronze then Silver ──────────────────────────────────────
def write_to_db(df: pd.DataFrame, engine) -> None:
    """
    SILVER: insert parsed data to a staging table, then upsert into the
    real silver.weather_readings table.

    FIX: silver.weather_readings has an "id BIGSERIAL PRIMARY KEY" column
    that the staging table does NOT have (df.to_sql() only creates columns
    present in the DataFrame). A bare "INSERT ... SELECT *" maps columns by
    POSITION — so "city" (text, position 1 in staging) would land in the
    "id" slot (bigint, position 1 in the real table) and crash with
    DatatypeMismatch. Naming every column explicitly avoids this entirely,
    and keeps working even if either table's column order ever changes.

    ON CONFLICT (city, recorded_at) DO NOTHING = safe to re-run without
    creating duplicates.
    """
    # Write to staging (creates/replaces the table each time)
    df.to_sql(
        "weather_readings_staging",
        schema="silver",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )

    columns_sql = ", ".join(SILVER_COLUMNS)

    # Upsert from staging into the real silver table — explicit columns only
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO silver.weather_readings ({columns_sql})
            SELECT {columns_sql}
            FROM silver.weather_readings_staging
            ON CONFLICT (city, recorded_at) DO NOTHING;

            DROP TABLE IF EXISTS silver.weather_readings_staging;
        """))

    log.info("  Wrote %d rows to silver.weather_readings", len(df))


def write_to_csv(df: pd.DataFrame) -> None:
    """Fallback: save to CSV if no DATABASE_URL is set."""
    # FIX: datetime.utcnow() is deprecated since Python 3.12.
    # Use timezone-aware datetime.now(timezone.utc) instead.
    path = f"data/raw/weather_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log.info("Saved to %s", path)


# ── Main: wires everything together ──────────────────────────────────────────
def main(mode: str = "live", dry_run: bool = False) -> None:
    log.info("===== Weather Ingest START [mode=%s | dry_run=%s] =====", mode, dry_run)

    fetched_at = datetime.now(timezone.utc)
    client     = build_client()
    params     = build_params(mode)

    try:
        responses = fetch_weather(client, params)
    except Exception as e:
        log.error("API call failed: %s", e)
        sys.exit(1)

    all_dfs, failed = [], []

    for i, response in enumerate(responses):
        city = CITIES[i]
        try:
            df = parse_response(response, city, fetched_at)
            if validate(df, city["name"]):
                all_dfs.append(df)
        except Exception as e:
            log.error("  [%s] parse failed: %s", city["name"], e)
            failed.append(city["name"])

    if failed:
        log.warning("Skipped cities: %s", failed)

    if not all_dfs:
        log.error("No data collected. Exiting.")
        sys.exit(1)

    combined = pd.concat(all_dfs, ignore_index=True)
    log.info("Total: %d rows | cities: %d", len(combined), len(all_dfs))

    if dry_run:
        log.info("DRY RUN — not writing. Sample:")
        print(combined.head(5).to_string(index=False))
        return

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        try:
            engine = create_engine(db_url, pool_pre_ping=True)
            write_to_db(combined, engine)
        except Exception as e:
            log.error("DB write failed: %s — saving to CSV", e)
            write_to_csv(combined)
    else:
        log.warning("DATABASE_URL not set — writing to CSV")
        write_to_csv(combined)

    log.info("===== Weather Ingest DONE =====")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather ingest — Stage 1")
    parser.add_argument("--mode",    choices=["live", "backfill"], default="live")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(mode=args.mode, dry_run=args.dry_run)