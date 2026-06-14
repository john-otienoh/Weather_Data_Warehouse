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

# Imports 
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

try:
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    
# Load env variables
load_dotenv()

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Project Configuration
CITIES = [
    {"name": "Nairobi", "lat": -1.2833, "lon": 36.8167},
    {"name": "Mombasa", "lat": -4.0547, "lon": 39.6636},
    {"name": "Kisumu",  "lat": -0.0861, "lon": 34.7289},
    {"name": "Nakuru",  "lat": -0.3072, "lon": 36.0722},
    {"name": "Eldoret", "lat":  0.5204, "lon": 35.2699},
]

API_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "weather_code",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m", 
    "visibility",
]

VAR_MAP = {
    name: i for i, name in enumerate(HOURLY_VARS)
}

# Validation Bounds
TEMP_MIN_C = -5.0
TEMP_MAX_C = 45.0
EXPECTED_ROWS_PER_DAY = 24

def build_client() -> openmeteo_requests.Client:
    """
    Return an Open-Meteo API client with local caching and retry logic.

    Cache TTL: 1800 s (30 min) — suitable for development.
    For production Airflow runs, set expire_after=-1 to disable caching,
    since each scheduled run should always fetch fresh data.
    """
    cache_dir = Path(__file__).resolve().parent / ".openmeteo_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_session = requests_cache.CachedSession(
        str(cache_dir / "cache"),
        expire_after=1800,
    )
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    log.info("API client initialised | cache: %s | TTL: 1800 s", cache_dir)
    return openmeteo_requests.Client(session=retry_session)

def build_params(mode: str="live") -> dict:
    """
    Build the Open-Meteo request parameter dict.

    mode="live"     → forecast_days=1  (fetch today's 24 hourly readings)
    mode="backfill" → past_days=31     (fetch the last 31 days of history)
    """
    params = {
        "latitude":  [c["lat"] for c in CITIES],
        "longitude": [c["lon"] for c in CITIES],
        "hourly":    HOURLY_VARS,
        # "timezone":  "auto",
        "timezone": "Africa/Nairobi",
    }
    if mode == "backfill":
        params["past_days"] = 31
        log.info("Mode: BACKFILL — pulling 31 days of historical data")
    else:
        params["forecast_days"] = 1
        log.info("Mode: LIVE — pulling today's hourly forecast (24 rows per city)")

    return params

def fetch_weather(client: openmeteo_requests.Client, params: dict) -> list:
    """
    Call the Open-Meteo weather API.
    Returns a list of response objects (one per city, same order as CITIES).
    """
    log.info("Calling Open-Meteo for %d cities ...", len(CITIES))
    responses = client.weather_api(API_URL, params=params)
    log.info("API call complete — %d responses received", len(responses))
    return responses


def parse_response(
    response,
    city: dict,
    fetched_at: datetime,
) -> pd.DataFrame:
    """
    Converts the API response for one city into a clean pandas DataFrame.
    Each row = one hour of data for that city.
    """
    hourly = response.Hourly()

    # Named extraction 
    def extract(var_name: str):
        return hourly.Variables(VAR_MAP[var_name]).ValuesAsNumpy()
    
    date_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    hourly_data_dict = {
        "city":               city["name"],
        "recorded_at":        date_range,
        "fetched_at":         fetched_at,
        "temp_celsius":       extract("temperature_2m"),
        "apparent_temp":      extract("apparent_temperature"),
        "humidity_pct":       extract("relative_humidity_2m"),
        "precipitation_mm":   extract("precipitation"),
        "rain_mm":            extract("rain"),
        "weather_code":       extract("weather_code"),
        "pressure_hpa":       extract("surface_pressure"),
        "cloud_cover_pct":    extract("cloud_cover"),
        "wind_speed_mps":     extract("wind_speed_10m"),
        "wind_direction_deg": extract("wind_direction_10m"),
        "wind_gusts_mps":     extract("wind_gusts_10m"),
        "visibility":         extract("visibility"),
    }
    df = pd.DataFrame(hourly_data_dict)

    log.info(
        "[%s]  parsed %d rows | lat=%.4f  lon=%.4f",
        city["name"], len(df), response.Latitude(), response.Longitude(),
    )
    return df

def validate_dataframe(df: pd.DataFrame, city_name: str) -> bool:
    """
    Run sanity checks on a parsed city DataFrame.
    All failures are logged; the DataFrame is accepted unless it is empty.
    """
    if df.empty:
        log.error("[%s]  FAIL — empty DataFrame, skipping city", city_name)
        return False

    # Null counts
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        log.warning(
            "[%s]  nulls detected: %s",
            city_name,
            cols_with_nulls.to_dict(),
        )

    # Temperature sanity (Kenya range)
    temp = df["temp_celsius"].dropna()
    out_of_range = (~temp.between(TEMP_MIN_C, TEMP_MAX_C)).sum()
    if out_of_range:
        log.warning(
            "[%s]  %d temperature readings outside[%.0f, %.0f] °C",
            city_name, out_of_range, TEMP_MIN_C, TEMP_MAX_C,
        )
    
    # Row count check (only meaningful for live/single-day mode)
    if len(df) < EXPECTED_ROWS_PER_DAY:
        log.warning(
            "[%s]  only %d rows (expected ≥ %d for a full day)",
            city_name, len(df), EXPECTED_ROWS_PER_DAY,
        )

    # Duplicate timestamps within this city
    dupes = df.duplicated(subset=["city", "recorded_at"]).sum()
    if dupes:
        log.warning("[%s]  %d duplicate (city, recorded_at) rows", city_name, dupes)

    log.info("[%s]  validation complete (%d rows)", city_name, len(df))
    return True

def write_to_db(df: pd.DataFrame, engine) -> None:
    """
    Write the unified DataFrame to:
        BRONZE: insert raw data to a staging table
        SILVER: upsert from staging into silver.weather_readings
            ON CONFLICT DO NOTHING = safe to re-run without creating duplicates
    """
    staging_table = "weather_readings_staging"

    # Step 1: write to a temporary staging table
    df.to_sql(
        staging_table,
        schema="silver",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",   
    )
    log.info("  Staging table written (%d rows)", len(df))

    # Step 2: upsert from staging into the real table
    upsert_sql = text("""
        INSERT INTO silver.weather_readings
        SELECT * FROM silver.weather_readings_staging
        ON CONFLICT (city, recorded_at) DO NOTHING;

        DROP TABLE IF EXISTS silver.weather_readings_staging;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql)

    log.info(
        "  Upsert complete — %d rows offered, duplicates skipped automatically",
        len(df),
    )

def write_to_csv(df: pd.DataFrame, output_dir: str = "data/raw") -> None:
    """
    writes a new csv file named with the UTC ingestion timestamp.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = out_path / f"weather_{ts}.csv"

    df.to_csv(filepath, index=False)
    log.info("CSV output written: %s (%d rows)", filepath, len(df))

def main(mode: str = "live", dry_run: bool = False) -> None:
    """
    Full Generation stage pipeline.

    Steps:
        1. Build API client
        2. Build request params based on mode
        3. Call Open-Meteo API for all cities
        4. Parse + validate each city response (per-city error isolation)
        5. Concatenate all valid DataFrames
        6. Write to PostgreSQL (or CSV fallback)
    """
    log.info(
        "=========  Weather Ingest START  [mode=%s | dry_run=%s]  =========",
        mode, dry_run,
    )

    fetched_at = datetime.now(timezone.utc)   

    # Step 1 — Client
    client = build_client()

    # Step 2 — Params
    params = build_params(mode)

    # Step 3 — Fetch
    try:
        responses = fetch_weather(client, params)
    except Exception as e:
        log.error("API call failed: %s", e)
        sys.exit(1)

    # Step 4 — Parse + Validate
    all_dfs: list[pd.DataFrame] = []
    failed_cities: list[str] = []

    for idx, response in enumerate(responses):
        city = CITIES[idx]
        city_name = city["name"]
        try:
            df = parse_response(response, city, fetched_at)
            if validate_dataframe(df, city_name):
                all_dfs.append(df)
            else:
                failed_cities.append(city_name)
        except Exception as e:
            log.error("[%s]  unhandled error during parse: %s", city_name, e)
            failed_cities.append(city_name)
            continue   

    if failed_cities:
        log.warning("Cities skipped due to errors: %s", failed_cities)

    if not all_dfs:
        log.error("No valid data collected from any city. Exiting.")
        sys.exit(1)

    # Step 5 — Combine
    unified_df = pd.concat(all_dfs, ignore_index=True)

    log.info(
        "Collection summary | rows: %d | cities: %d | "
        "date range: %s  →  %s",
        len(unified_df),
        len(all_dfs),
        unified_df["recorded_at"].min(),
        unified_df["recorded_at"].max(),
    )

    if dry_run:
        log.info("DRY RUN — skipping all writes. Sample output (10 rows):")
        print(unified_df.head(10).to_string(index=False))
        log.info("=========  Weather Ingest DRY RUN DONE  =========")
        return

    # Step 6 — Write
    db_url = os.getenv("DATABASE_URL")

    if db_url and DB_AVAILABLE:
        try:
            engine = create_engine(
                db_url,
                pool_pre_ping=True,   
            )
            write_to_db(unified_df, engine)
        except Exception as e:
            log.error("DB write failed (%s). Falling back to CSV.", e)
            write_to_csv(unified_df)
    else:
        reason = "DATABASE_URL not set" if not db_url else "sqlalchemy not installed"
        log.warning("%s — writing to CSV fallback.", reason)
        write_to_csv(unified_df)

    log.info("=========  Weather Ingest DONE  =========")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Open-Meteo Weather Data Warehouse — Generation Stage"
    )
    parser.add_argument(
        "--mode",
        choices=["live", "backfill"],
        default=os.getenv("INGEST_MODE", "live"),
        help=(
            "live     = today only (forecast_days=1)  [default]\n"
            "backfill = last 31 days (past_days=31)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate only — do not write to DB or CSV",
    )
    args = parser.parse_args()
    main(mode=args.mode, dry_run=args.dry_run)
