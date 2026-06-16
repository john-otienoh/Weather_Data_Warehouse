import logging
import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Optional


from sqlalchemy import text
from db.connection import get_engine

# Logging
log = logging.getLogger(__name__)

# Path to the SQL files
SQL_DIR = Path(__file__).parent

def run_sql(sql: str, params: dict, label: str) -> int:
    """
    Executes a SQL string inside a transaction.
    Returns the number of rows affected.
    If anything fails, the transaction is rolled back automatically.
    """
    with get_engine().begin() as conn:
        result = conn.execute(text(sql), params)
        count = result.rowcount if result.rowcount >= 0 else -1
        log.info("[%s] rows affected: %s", label, count)
        return count
    
def load(filename: str):
    """Read a SQL file from the transforms folder."""
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text()

def run_daily_summary(target_date: Optional[date] = None) -> dict:
    """
    Compute daily aggregates for target_date (default: yesterday).
    Writes 5 rows to gold.daily_summary (one per city).
    """
    target_date = target_date or date.today() - timedelta(days=1)
    log.info("Running daily summary for %s ...", target_date)
    sql  = _load("daily_summary.sql")
    rows = _run_sql(sql, {"target_date": str(target_date)}, "daily_summary")
    return {"transform": "daily_summary", "date": str(target_date), "rows": rows}

def run_monthly_summary(year: int = None, month: int = None) -> dict:
    """
    Compute monthly aggregates (default: previous calendar month).
    Only writes rows when all daily data for that month is in gold.daily_summary.
    """
    if not year or not month:
        prev = (date.today().replace(day=1) - timedelta(days=1))
        year, month = prev.year, prev.month
    log.info("Running monthly summary for %04d-%02d ...", year, month)
    sql  = _load("monthly_summary.sql")
    rows = _run_sql(sql, {"target_year": year, "target_month": month}, "monthly_summary")
    return {"transform": "monthly_summary", "period": f"{year:04d}-{month:02d}", "rows": rows}

def run_anomaly_detection() -> dict:
    """
    Flag hourly readings from the last 25 hours as anomalies if they are
    more than 2 standard deviations from the 30-day baseline.
    """
    log.info("Running anomaly detection ...")
    sql  = _load("anomaly_detection.sql")
    rows = _run_sql(sql, {}, "anomaly_detection")

    with get_engine().connect() as conn:
        flagged = conn.execute(text("""
            SELECT COUNT(*) FROM gold.temperature_anomalies
            WHERE is_anomaly = TRUE AND detected_at >= NOW() - INTERVAL '25 hours'
        """)).scalar()

    return {"transform": "anomaly_detection", "rows": rows, "anomalies": flagged}

def validate_gold() -> dict:
    """Check that gold.daily_summary has rows for yesterday."""
    yesterday = date.today() - timedelta(days=1)
    with get_engine().connect() as conn:
        result = conn.execute(text("""
            SELECT city, avg_temp, total_rain
            FROM gold.daily_summary
            WHERE summary_date = :d ORDER BY city
        """), {"d": str(yesterday)})
        rows = result.fetchall()

    found = [r[0] for r in rows]
    missing = [c for c in ["Eldoret","Kisumu","Mombasa","Nairobi","Nakuru"] if c not in found]

    if missing:
        raise ValueError(f"Gold missing cities for {yesterday}: {missing}")

    log.info("Gold validation OK for %s: %d cities", yesterday, len(rows))
    return {"date": str(yesterday), "cities": found}
