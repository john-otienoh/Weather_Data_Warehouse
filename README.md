# 🌦️ Weather Data Warehouse

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly%20Dash-2.17-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0-000000?style=for-the-badge&logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)

![Open-Meteo](https://img.shields.io/badge/Open--Meteo-Free%20API-blue?style=flat-square)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-gold?style=flat-square)
![Cities](https://img.shields.io/badge/Cities-5%20Kenyan-green?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-brightgreen?style=flat-square)

<br/>

**A production-grade, end-to-end data engineering pipeline built entirely in Python.**
Collects hourly weather data for 5 major Kenyan cities, stores it in a Medallion-architecture
PostgreSQL warehouse, runs daily SQL transformation jobs, and serves insights through
an interactive Plotly Dash dashboard and a Flask REST API.

[Pipeline Overview](#-pipeline-overview) · [Quick Start](#-quick-start) · [Dashboard](#-dashboard) · [API](#-api-reference) · [Skills Learned](#-skills-learned) · [Roadmap](#-roadmap)

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Pipeline Overview](#-pipeline-overview)
- [Medallion Architecture](#-medallion-architecture)
- [Data Collected](#-data-collected)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Usage](#-usage)
- [Dashboard](#-dashboard)
- [API Reference](#-api-reference)
- [Airflow DAGs](#-airflow-dags)
- [Skills Learned](#-skills-learned)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🌍 Overview

The **Weather Data Warehouse** is a full-lifecycle data engineering project that demonstrates every stage of a production pipeline applied to a single real-world domain: East African weather.

It tracks **temperature, rainfall, wind, humidity, pressure, and visibility** for **Nairobi, Mombasa, Kisumu, Nakuru, and Eldoret** — every hour, around the clock — using the [Open-Meteo API](https://open-meteo.com/) (completely free, no API key needed).

The entire stack is **pure Python** and **open-source**. No paid services, no proprietary tools.

### What the system produces

| Output | Detail |
|---|---|
| **Raw ingestion** | 120 rows/hour · 5 cities · 12 weather variables · UPSERT-safe |
| **Daily summaries** | Avg/min/max temp, total rainfall, wind, visibility per city per day |
| **Monthly rollups** | Monthly aggregates, rainy-day counts, trend direction |
| **Anomaly detection** | Hourly readings flagged where temp deviates > 2σ from 30-day baseline |
| **Live dashboard** | 5-tab Plotly Dash app, auto-refreshes hourly |
| **REST API** | 8 Flask endpoints + CSV export |
| **Weekly email** | HTML digest with 7-day city summaries |

---

## 🔁 Pipeline Overview

```
┌──────────────┬───────────────┬───────────────┬───────────────┬─────────────────┐
│   STAGE 1    │    STAGE 2    │    STAGE 3    │    STAGE 4    │     STAGE 5     │
│  Generation  │   Storage     │  Ingestion    │Transformation │    Serving      │
├──────────────┼───────────────┼───────────────┼───────────────┼─────────────────┤
│              │               │               │               │                 │
│ Open-Meteo   │  PostgreSQL   │   Apache      │  SQL + Python │  Plotly Dash    │
│ REST API     │  Medallion    │   Airflow     │               │  (port 8050)    │
│              │  Architecture │               │  daily_summary│                 │
│ 5 cities     │               │  Hourly DAG   │  monthly_sum  │  Flask API      │
│ 12 variables │  Bronze       │  Backfill DAG │  anomaly_det  │  (port 5000)    │
│ hourly       │  Silver       │  Daily DAG    │               │                 │
│              │  Gold         │  Weekly DAG   │               │  Email Reports  │
└──────────────┴───────────────┴───────────────┴───────────────┴─────────────────┘
```

### End-to-End Data Flow

```
Open-Meteo API  (free, no key)
       │
       ▼  Stage 1 — Python, every hour via Airflow
bronze.weather_raw          ← raw JSON payload, never modified
silver.weather_readings     ← parsed columns, 120 rows/run, UPSERT-safe
       │
       ▼  Stage 4 — SQL transforms, daily at 01:00 EAT
gold.daily_summary          ──────────────────────────────┐
gold.monthly_summary         ── Plotly Dash Dashboard ◄───┤
gold.temperature_anomalies   ── Flask REST API        ◄───┤
gold.rainfall_trends (view)  ── Weekly Email          ◄───┘
```

---

## 🏅 Medallion Architecture

This project implements the industry-standard **Medallion (Bronze → Silver → Gold)** pattern.

```
BRONZE                   SILVER                  GOLD
──────────────           ──────────────          ────────────────
Raw data exactly         Validated, typed,       Pre-computed,
as it arrived            deduplicated            business-ready
from the API             hourly readings         aggregates

bronze.weather_raw  ──►  silver.weather_readings  ──►  gold.daily_summary
                                                        gold.monthly_summary
                                                        gold.temperature_anomalies
                                                        gold.rainfall_trends (view)
```

| Layer | Schema | Purpose | Who writes | Who reads |
|---|---|---|---|---|
| 🥉 Bronze | `bronze.*` | Immutable raw JSON — audit trail and replay source | Stage 1 | Nobody directly |
| 🥈 Silver | `silver.*` | Clean, typed, one column per variable | Stage 1 | Stage 4 transforms |
| 🥇 Gold | `gold.*` | Pre-computed answers — fast queries | Stage 4 | Dashboard + API |

> **Why three layers?**
> If parsing logic changes, replay from Bronze without re-hitting the API.
> Silver gives transforms a consistent input. Gold makes dashboards fast — they query aggregates, not raw hourly rows.

---

## 📡 Data Collected

### Cities monitored

| City | Latitude | Longitude | Altitude | Notes |
|---|---|---|---|---|
| **Nairobi** | -1.2833° | 36.8167° | ~1700 m | Capital · pressure baseline ~840 hPa |
| **Mombasa** | -4.0547° | 39.6636° | ~17 m | Coastal · highest temperatures |
| **Kisumu** | -0.0861° | 34.7289° | ~1131 m | Lake Victoria region |
| **Nakuru** | -0.3072° | 36.0722° | ~1850 m | Rift Valley |
| **Eldoret** | 0.5204° | 35.2699° | ~2100 m | Highest altitude · coolest |

### Weather variables (12 per hourly reading)

| Column | Unit | Description |
|---|---|---|
| `temp_celsius` | °C | 2 m air temperature |
| `apparent_temp` | °C | Feels-like temperature |
| `humidity_pct` | % | Relative humidity |
| `precipitation_mm` | mm | Total precipitation |
| `rain_mm` | mm | Rain-only precipitation |
| `weather_code` | WMO | 0=clear · 61=rain · 95=thunderstorm |
| `pressure_hpa` | hPa | Surface pressure |
| `cloud_cover_pct` | % | Cloud cover |
| `wind_speed_mps` | m/s | 10 m wind speed |
| `wind_direction_deg` | ° | Wind direction |
| `wind_gusts_mps` | m/s | Wind gusts |
| `visibility_m` | m | Visibility in metres |

---

## 🛠️ Tech Stack

| Layer | Tool | Version | Why this? |
|---|---|---|---|
| Language | **Python** | 3.11+ | Readable, huge community, runs everywhere |
| Database | **PostgreSQL** | 15 | Free, reliable, excellent SQL support |
| Orchestration | **Apache Airflow** | 2.8 | Industry-standard scheduler — free, Python-native |
| Dashboard | **Plotly Dash** | 2.17 | Interactive web charts in pure Python — no JavaScript |
| API | **Flask** | 3.0 | Simplest Python web framework |
| Data Source | **Open-Meteo** | v1 | Completely free, no API key needed |
| Data Wrangling | **pandas** | 2.2 | The standard Python data tool |
| DB Adapter | **psycopg2** | 2.9 | Native PostgreSQL connector |
| ORM | **SQLAlchemy** | 2.0 | Works seamlessly with pandas |
| Containerisation | **Docker Compose** | v2 | One command to start everything |
| Config | **python-dotenv** | 1.0 | Keeps passwords out of code |

---

## 📁 Project Structure

```
weather-data-warehouse/
│
├── .env.example                    Template for environment variables
├── requirements.txt                All Python dependencies
├── docker-compose.yml              Starts PostgreSQL + Airflow
│
├── scripts/
│   ├── init_db.sql                 Creates Bronze/Silver/Gold schemas (auto-runs on first start)
│   └── setup_airflow.sh            Configures Airflow connections (run once)
│
├── src/
│   └── db/
│       └── connection.py           Centralised SQLAlchemy engine — imported everywhere
│
├── generation/                     STAGE 1 — Generation
│   └── weather_ingest.py           Fetches Open-Meteo API → writes to Bronze + Silver
│
├── transformation/                 STAGE 4 — Transformation
│   ├── transform_runner.py         Python wrapper — called by Airflow tasks + CLI
│   ├── daily_summary.sql           Silver → Gold: daily aggregates
│   ├── monthly_summary.sql         Gold daily → Gold monthly rollup
│   └── anomaly_detection.sql       Flags readings > 2σ from 30-day baseline
│
├── dags/                           STAGE 3 — Ingestion (Airflow)
│   ├── hourly_ingest.py            Runs every hour automatically
│   ├── backfill.py                 Loads 31 days of history (manual trigger)
│   ├── daily_transforms.py         Runs transforms at 01:00 EAT daily
│   └── weekly_digest.py            Sends HTML email report every Monday
│
├── serving/                        STAGE 5 — Serving
│   ├── dashboard.py                Plotly Dash app — 5 tabs (port 8050)
│   ├── api.py                      Flask REST API — 8 endpoints (port 5000)
│   └── reports.py                  Weekly HTML email generator
│
└── logs/                           Airflow task logs (auto-created)
```

---

## 📦 Prerequisites

| Tool | Download |
|---|---|
| Python 3.11+ | [python.org](https://www.python.org/downloads/) |
| Docker Desktop | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Git | [git-scm.com](https://git-scm.com/downloads) |

> No database installation needed — PostgreSQL runs in Docker.
> No API keys needed — Open-Meteo is completely free.

---

## ⚡ Quick Start

### 1 — Clone

```bash
git clone https://github.com/yourhandle/weather-data-warehouse.git
cd weather-data-warehouse
```

### 2 — Configure

```bash
cp .env.example .env
```

Edit `.env` with a text editor:

```env
# Required — pick any strong password
POSTGRES_PASSWORD=MyWeatherPass123!
DATABASE_URL=postgresql+psycopg2://weather_admin:MyWeatherPass123!@localhost:5432/weather_db

# Airflow Fernet key (run this in Python, copy the output):
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_FERNET_KEY=paste_output_here

# Airflow secret key (run this in Python, copy the output):
# python -c "import secrets; print(secrets.token_hex(32))"
AIRFLOW_SECRET_KEY=paste_output_here

AIRFLOW_ADMIN_PASSWORD=admin123
```

### 3 — Install Python packages

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 4 — Start Docker services

```bash
docker compose up -d
```

PostgreSQL starts, and `scripts/init_db.sql` runs **automatically** — creating all Bronze, Silver, and Gold schemas, tables, and indexes. No manual SQL needed.

```bash
docker compose ps    # all containers should show "healthy" or "Up"
```

### 5 — Configure Airflow

```bash
bash scripts/setup_airflow.sh
```

### 6 — Load data

```bash
# Load 31 days of history (recommended)
python generation/weather_ingest.py --mode backfill

# Or just pull today
python generation/weather_ingest.py
```

```
2026-06-13 20:31:06  INFO  ===== Weather Ingest START [mode=backfill] =====
2026-06-13 20:31:07  INFO  [Nairobi]  744 rows parsed
2026-06-13 20:31:08  INFO  [Mombasa]  744 rows parsed
...
2026-06-13 20:31:10  INFO  Total: 3720 rows | cities: 5
2026-06-13 20:31:10  INFO  Wrote 3720 rows to silver.weather_readings
2026-06-13 20:31:10  INFO  ===== Weather Ingest DONE =====
```

### 7 — Run transforms

```bash
python -m transformation.transform_runner --transform all
```

### 8 — Open the dashboard

```bash
python serving/dashboard.py
```

**→ Open http://localhost:8050** 🎉

---

## 🚀 Usage

### Service map

| Service | URL | Credentials |
|---|---|---|
| 📊 Dashboard | http://localhost:8050 | None |
| 🔌 REST API | http://localhost:5000 | None |
| ✈️ Airflow UI | http://localhost:8080 | admin / your `AIRFLOW_ADMIN_PASSWORD` |

### Enable automatic hourly collection

1. Open Airflow at http://localhost:8080
2. Find `hourly_weather_ingest` and toggle it **ON**
3. Find `daily_weather_transforms` and toggle it **ON**
4. Data now collects automatically — no manual intervention needed

### Verify data

```bash
docker compose exec postgres psql -U weather_admin -d weather_db -c "
SELECT 'silver.weather_readings'     AS layer, COUNT(*) FROM silver.weather_readings
UNION ALL
SELECT 'gold.daily_summary',                   COUNT(*) FROM gold.daily_summary
UNION ALL
SELECT 'gold.temperature_anomalies',           COUNT(*) FROM gold.temperature_anomalies;"
```

### Run transforms for a specific date

```bash
python -m transformation.transform_runner --transform daily --date 2026-06-12
python -m transformation.transform_runner --transform anomalies
python -m transformation.transform_runner --transform all
```

---

## 📊 Dashboard

Five interactive tabs, built with **Plotly Dash** — pure Python, no JavaScript, no external services.

```
python serving/dashboard.py   →   http://localhost:8050
```

| Tab | Charts | Data Source |
|---|---|---|
| 📊 **Overview** | City stat cards: temp, rain, wind, condition | `gold.daily_summary` |
| 🌡️ **Temperature** | 30-day trend line + min/max shaded range band | `gold.daily_summary` |
| 🌧️ **Rainfall** | Daily grouped bar chart + 7-day rolling average line | `gold.rainfall_trends` |
| ⚠️ **Anomalies** | Scatter plot vs baseline + sortable anomaly table | `gold.temperature_anomalies` |
| 📅 **Monthly** | Monthly avg temp + total rainfall grouped bar charts | `gold.monthly_summary` |

**Features:** auto-refresh every hour · city filter on temperature tab · consistent city colour palette · anomaly rows highlighted red/blue · all queries hit pre-computed Gold layer

---

## 🔌 API Reference

```
python serving/api.py   →   http://localhost:5000
```

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Liveness check + last Gold layer update timestamp |
| `GET` | `/api/cities` | List all monitored cities |
| `GET` | `/api/weather/current/<city>` | Latest daily summary for one city |
| `GET` | `/api/weather/daily/<city>` | Date-range daily summaries (`?from_date=&to_date=`) |
| `GET` | `/api/weather/monthly/<city>` | Monthly aggregates (last 12 months) |
| `GET` | `/api/weather/anomalies` | Flagged anomalies (`?hours=48&city=Nairobi`) |
| `GET` | `/api/weather/compare` | All-city snapshot for one date (`?date=2026-06-12`) |
| `GET` | `/api/export/csv` | Download Gold data as CSV (`?city=Nairobi&from_date=`) |

**Example:**

```bash
curl http://localhost:5000/api/weather/current/Nairobi
```

```json
{
  "city": "Nairobi",
  "summary_date": "2026-06-12",
  "avg_temp": 18.2,
  "min_temp": 15.1,
  "max_temp": 21.4,
  "avg_humidity": 82.5,
  "avg_pressure": 840.3,
  "total_rain": 0.4,
  "avg_wind_speed": 4.8,
  "avg_wind_gusts": 9.2,
  "avg_visibility": 9400.0,
  "dominant_code": 51
}
```

---

## ✈️ Airflow DAGs

Access at **http://localhost:8080** (admin / your password)

| DAG | Schedule | Trigger | What it does |
|---|---|---|---|
| `hourly_weather_ingest` | Every hour | Automatic | Pulls API → Bronze + Silver |
| `backfill_weather` | None | Manual ▶ | Loads 31 days of history |
| `daily_weather_transforms` | Daily 01:00 EAT | Automatic | Silver → Gold aggregates |
| `weekly_weather_digest` | Monday 07:00 EAT | Automatic | Sends HTML email report |

---

## 🎓 Skills Learned

<details>
<summary><strong>Data Engineering Fundamentals</strong></summary>

- Full pipeline thinking — each stage has one job and hands a clean contract to the next
- Medallion architecture — why three layers beat one flat table in every production system
- Idempotent design — every write uses `ON CONFLICT DO NOTHING/UPDATE`, making every stage safe to retry without data corruption

</details>

<details>
<summary><strong>Batch Pipeline Design</strong></summary>

- Airflow patterns — `ShortCircuitOperator` for conditional monthly runs, `max_active_runs=1` for concurrency safety, XCom for inter-task reporting
- Per-task error isolation — wrapping each city in `try/except` so one failure never kills the others
- Retry logic — exponential backoff for transient API failures

</details>

<details>
<summary><strong>Time-Series Data & SQL</strong></summary>

- Timezone correctness — why `DATE(recorded_at)` gives wrong dates without `AT TIME ZONE 'Africa/Nairobi'` for EAT data stored as UTC
- SQL window functions — `STDDEV() OVER`, `AVG() OVER (ROWS BETWEEN N PRECEDING)` for rolling baselines
- `MODE() WITHIN GROUP` — ordered-set aggregate for dominant WMO weather code per day
- Upsert patterns — `ON CONFLICT DO UPDATE` for safe incremental loads

</details>

<details>
<summary><strong>Python Data Stack</strong></summary>

- **Plotly Dash** — multi-tab interactive dashboards with callbacks, dropdown filters, DataTable, and auto-refresh
- **Flask** — REST endpoints with query params, JSON serialisation, and streaming CSV responses
- **pandas + SQLAlchemy** — `pd.read_sql()` for dashboard queries, `df.to_sql()` for bulk inserts
- **openmeteo-requests** — batched multi-city API calls with caching and retry middleware

</details>

<details>
<summary><strong>Infrastructure & DevOps</strong></summary>

- Docker Compose — multi-service orchestration with `healthcheck`, `depends_on: condition: service_healthy`, and named volumes
- Airflow connection management — externalising database credentials out of DAG code for portability
- Environment management — `.env` files and `python-dotenv`, never committing secrets

</details>

<details>
<summary><strong>East African Context</strong></summary>

- `timezone: auto` in Open-Meteo — all 5 Kenyan cities auto-resolve to `Africa/Nairobi` (EAT = UTC+3) from coordinates
- High-altitude pressure — Nairobi's ~840 hPa is correct at 1700 m above sea level, not a data error
- Multi-city pipeline design — a single codebase handles N cities with zero code duplication

</details>

---

## 🗺️ Roadmap

### Data quality
- [ ] Great Expectations for dataset-level validation suites with DAG-level quality gates
- [ ] Alembic schema migrations for version-controlled database evolution

### Pipeline
- [ ] dbt to replace raw `.sql` files with tested, versioned, lineage-tracked models
- [ ] GitHub Actions CI/CD — `pytest` on every push, DAG linting before merge
- [ ] Apache Kafka for real-time anomaly detection (seconds, not hourly)

### Coverage
- [ ] Pan-Africa expansion — Kampala, Dar es Salaam, Kigali, Addis Ababa, Lagos
- [ ] Open-Meteo Historical API — weather data back to 1940 for long-term climate analysis

### Serving
- [ ] USSD interface via Africa's Talking for feature-phone users (`*384#`)
- [ ] WhatsApp Business API alerts for rainfall thresholds and anomalies
- [ ] Auto-generated weekly Jupyter notebook reports via Papermill
- [ ] Embeddable JavaScript weather widget for media and government portals

### Infrastructure
- [ ] Kubernetes + Helm for cloud-scale deployment
- [ ] HashiCorp Vault or GCP Secret Manager replacing `.env` in production
- [ ] Prometheus metrics + dedicated pipeline health dashboard

---

## 🚨 Common Errors & Fixes

| Error | Cause | Fix |
|---|---|---|
| `DATABASE_URL is not set` | `.env` not configured | `cp .env.example .env` and fill in values |
| `Connection refused` on port 5432 | PostgreSQL not running | `docker compose up -d postgres` |
| `relation silver.weather_readings does not exist` | `init_db.sql` didn't auto-run | Run: `docker compose exec postgres psql -U weather_admin -d weather_db -f /docker-entrypoint-initdb.d/init_db.sql` |
| DAG not appearing in Airflow | Mount not configured | Check `dags/` volume in `docker-compose.yml`; restart scheduler |
| `No module named 'openmeteo_requests'` | venv not active | `source .venv/bin/activate && pip install -r requirements.txt` |
| `ImportError` on generation module | PYTHONPATH missing | `export PYTHONPATH="$PWD"` |
| Dashboard shows "No data yet" | Transforms not run | `python -m transformation.transform_runner --transform all` |
| Airflow Fernet key error | Key not generated | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |

---

## 🤝 Contributing

1. **Fork** the repository
2. **Create a branch**: `git checkout -b feature/add-kisii`
3. **Make changes** — adding a city only requires one line in `CITIES` list
4. **Test**: `python generation/weather_ingest.py --dry-run`
5. **Pull request** with a clear description

---

## 📄 License

```
MIT License — Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies,
without restriction.
```

---

<div align="center">

**Built in Nairobi, Kenya 🇰🇪**

`Python` · `PostgreSQL` · `Apache Airflow` · `Plotly Dash` · `Flask` · `Open-Meteo`

*Implements the complete data engineering lifecycle:*
*Generation → Storage → Ingestion → Transformation → Serving*

---

⭐ **Star this repo** if it helped you learn data engineering!

</div>