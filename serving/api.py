
import os
from datetime import date, timedelta
from flask import Flask, jsonify, request, Response
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

app   = Flask(__name__)
engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

CITIES = {"Nairobi","Mombasa","Kisumu","Nakuru","Eldoret"}

def db_query(sql: str, params: dict = {}) -> list:
    """Run a SQL query and return results as a list of dicts."""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        cols   = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]


# ── Health check ─────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            latest = conn.execute(text("SELECT MAX(updated_at)::TEXT FROM gold.daily_summary")).scalar()
        return jsonify({"status": "ok", "latest_gold_update": latest})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── List all cities ──────────────────────────────────────────────────────────
@app.get("/api/cities")
def cities():
    rows = db_query("SELECT DISTINCT city FROM gold.daily_summary ORDER BY city")
    return jsonify({"cities": [r["city"] for r in rows]})


# ── Current conditions (yesterday's summary) ─────────────────────────────────
@app.get("/api/weather/current/<city>")
def current(city: str):
    city = city.strip().title()
    if city not in CITIES:
        return jsonify({"error": f"Unknown city: {city}"}), 404
    rows = db_query("""
        SELECT * FROM gold.daily_summary
        WHERE city = :city ORDER BY summary_date DESC LIMIT 1
    """, {"city": city})
    if not rows:
        return jsonify({"error": "No data found"}), 404
    # Convert date to string for JSON
    rows[0]["summary_date"] = str(rows[0]["summary_date"])
    rows[0]["updated_at"]   = str(rows[0]["updated_at"])
    return jsonify(rows[0])


# ── Daily summary with optional date range ───────────────────────────────────
@app.get("/api/weather/daily/<city>")
def daily(city: str):
    city      = city.strip().title()
    from_date = request.args.get("from_date", str(date.today() - timedelta(days=30)))
    to_date   = request.args.get("to_date",   str(date.today() - timedelta(days=1)))
    rows      = db_query("""
        SELECT * FROM gold.daily_summary
        WHERE city = :city AND summary_date BETWEEN :f AND :t
        ORDER BY summary_date DESC
    """, {"city": city, "f": from_date, "t": to_date})
    for r in rows:
        r["summary_date"] = str(r["summary_date"])
        r["updated_at"]   = str(r["updated_at"])
    return jsonify({"city": city, "count": len(rows), "data": rows})


# ── Monthly summary ──────────────────────────────────────────────────────────
@app.get("/api/weather/monthly/<city>")
def monthly(city: str):
    city = city.strip().title()
    rows = db_query("""
        SELECT * FROM gold.monthly_summary
        WHERE city = :city ORDER BY year DESC, month DESC LIMIT 12
    """, {"city": city})
    for r in rows:
        r["updated_at"] = str(r["updated_at"])
    return jsonify({"city": city, "data": rows})


# ── Recent anomalies ─────────────────────────────────────────────────────────
@app.get("/api/weather/anomalies")
def anomalies():
    city  = request.args.get("city")
    hours = int(request.args.get("hours", 48))
    clause = "AND city = :city" if city else ""
    rows  = db_query(f"""
        SELECT city, recorded_at AT TIME ZONE 'Africa/Nairobi' AS local_time,
               temp_celsius, baseline_avg, deviation, is_anomaly
        FROM gold.temperature_anomalies
        WHERE detected_at >= NOW() - INTERVAL '{hours} hours'
          AND is_anomaly = TRUE {clause}
        ORDER BY ABS(deviation) DESC LIMIT 100
    """, {"city": city} if city else {})
    for r in rows:
        r["local_time"] = str(r["local_time"])
    return jsonify({"count": len(rows), "data": rows})


# ── All-city comparison for one date ─────────────────────────────────────────
@app.get("/api/weather/compare")
def compare():
    d = request.args.get("date", str(date.today() - timedelta(days=1)))
    rows = db_query("""
        SELECT * FROM gold.daily_summary WHERE summary_date = :d ORDER BY city
    """, {"d": d})
    for r in rows:
        r["summary_date"] = str(r["summary_date"])
        r["updated_at"]   = str(r["updated_at"])
    return jsonify({"date": d, "cities": rows})


# ── CSV export ───────────────────────────────────────────────────────────────
@app.get("/api/export/csv")
def export_csv():
    city      = request.args.get("city")
    from_date = request.args.get("from_date", str(date.today() - timedelta(days=30)))
    to_date   = request.args.get("to_date",   str(date.today() - timedelta(days=1)))
    clause    = "AND city = :city" if city else ""
    with engine.connect() as conn:
        df = pd.read_sql(text(f"""
            SELECT * FROM gold.daily_summary
            WHERE summary_date BETWEEN :f AND :t {clause}
            ORDER BY city, summary_date
        """), conn, params={"f": from_date, "t": to_date, "city": city} if city
             else {"f": from_date, "t": to_date})
    csv = df.to_csv(index=False)
    return Response(csv, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment;filename=weather_{from_date}_{to_date}.csv"})


if __name__ == "__main__":
    print("Starting API at http://localhost:5000")
    print("Try: curl http://localhost:5000/api/cities")
    app.run(debug=True, port=5000)

