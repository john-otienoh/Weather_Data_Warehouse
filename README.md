# Weather Data Warehouse
> A production-style data engineering project covering the full lifecycle — from raw API data to Grafana dashboards.

---

## What You'll Build

A historical weather warehouse that collects data from multiple cities every hour, stores raw and processed records in PostgreSQL, runs transformation pipelines via Airflow, and surfaces insights through Grafana dashboards.

---

## The Data Engineering Lifecycle

```
Generation → Storage → Ingestion → Transformation → Serving
```

This project maps cleanly to every stage of the lifecycle. Each section below describes:
- What happens at that stage
- How the weather project implements it
- The tools used
- The deliverables produced

---

## Stage 1 — Generation

> **Definition:** The origin of data. This is where raw data is created or emitted by an external source — sensors, APIs, event streams, or databases.

### In This Project

The data source is the **OpenWeatherMap API**. It exposes current and forecast weather data for any city worldwide, returning JSON payloads with temperature, humidity, wind speed, pressure, and precipitation readings.

- Cities tracked: Multiple (e.g., Nairobi, Mombasa, Kisumu, Nakuru, Eldoret — extendable globally)
- Frequency: Hourly polling
- Format: JSON via REST
- Authentication: API Key

### Sample Raw Payload (OpenMeteo)

```json
{
    "latitude": 52.52,
    "longitude": 13.419,
    "elevation": 44.812,
    "generationtime_ms": 2.2119,
    "utc_offset_seconds": 0,
    "timezone": "Europe/Berlin",
    "timezone_abbreviation": "CEST",
    "hourly": {
        "time": ["2022-07-01T00:00", "2022-07-01T01:00", "2022-07-01T02:00", ...],
        "temperature_2m": [13, 12.7, 12.7, 12.5, 12.5, 12.8, 13, 12.9, 13.3, ...]
    },
    "hourly_units": {
        "temperature_2m": "°C"
    }
}
```

### Tools

| Tool | Role |
|---|---|
| OpenMeteo API | Data source (weather readings) |
| Python `requests` | HTTP client to call the API |
| `python-dotenv` | Manage API keys securely via `.env` |

### Deliverables

- [ ] OpenWeather API account + key provisioned
- [ ] City config list (`cities.yaml`)
- [ ] Python API client module (`generation/openmeteo.ipynb`)
- [ ] Sample raw JSON payload files for testing (`data/samples/`)

---

## Stage 2 — Storage

> **Definition:** Where data is persisted — either as raw (landing zone) or as structured tables (warehouse). Good storage design separates raw from processed and supports auditability and reprocessing.

### In This Project

PostgreSQL is the backbone of the warehouse. The design follows a **two-layer architecture**:

| Layer | Schema | Purpose |
|---|---|---|
| Raw / Bronze | `raw` | Stores unmodified API responses as-is |
| Processed / Silver | `warehouse` | Clean, typed, structured data |
| Aggregated / Gold | `metrics` | Pre-computed averages, anomalies, trends |

### Schema Design

```sql
-- RAW LAYER
CREATE SCHEMA raw;

CREATE TABLE raw.weather_events (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100)    NOT NULL,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    payload         JSONB           NOT NULL
);

-- PROCESSED LAYER
CREATE SCHEMA warehouse;

CREATE TABLE warehouse.weather_readings (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(100)    NOT NULL,
    recorded_at     TIMESTAMPTZ     NOT NULL,
    temp_celsius    NUMERIC(5, 2),
    humidity_pct    INTEGER,
    pressure_hpa    INTEGER,
    wind_speed_mps  NUMERIC(5, 2),
    wind_deg        INTEGER,
    rainfall_1h_mm  NUMERIC(6, 3)   DEFAULT 0,
    condition       VARCHAR(50),
    description     TEXT
);

-- METRICS LAYER
CREATE SCHEMA metrics;

CREATE TABLE metrics.daily_summary (
    city            VARCHAR(100)    NOT NULL,
    summary_date    DATE            NOT NULL,
    avg_temp        NUMERIC(5, 2),
    min_temp        NUMERIC(5, 2),
    max_temp        NUMERIC(5, 2),
    avg_humidity    NUMERIC(5, 2),
    total_rainfall  NUMERIC(6, 3),
    PRIMARY KEY (city, summary_date)
);

CREATE TABLE metrics.monthly_summary (
    city            VARCHAR(100)    NOT NULL,
    year            INTEGER         NOT NULL,
    month           INTEGER         NOT NULL,
    avg_temp        NUMERIC(5, 2),
    total_rainfall  NUMERIC(6, 3),
    PRIMARY KEY (city, year, month)
);

CREATE TABLE metrics.temperature_anomalies (
    city            VARCHAR(100)    NOT NULL,
    recorded_at     TIMESTAMPTZ     NOT NULL,
    temp_celsius    NUMERIC(5, 2),
    baseline_avg    NUMERIC(5, 2),
    deviation       NUMERIC(5, 2),
    is_anomaly      BOOLEAN
);
```

### Tools

| Tool | Role |
|---|---|
| PostgreSQL | Primary warehouse (raw + processed + metrics) |
| SQLAlchemy | Python ORM / connection pooling |
| Alembic | Database migration management |
| psycopg2 | Native PostgreSQL adapter for Python |
| Docker Compose | Spin up local PostgreSQL instance |

### Deliverables

- [ ] `docker-compose.yml` with PostgreSQL + pgAdmin
- [ ] Schema migration scripts (`migrations/`)
- [ ] Database initialization script (`scripts/init_db.sql`)
- [ ] `src/db/models.py` — SQLAlchemy table definitions
- [ ] `src/db/connection.py` — Connection pool manager
- [ ] `.env.example` with `DATABASE_URL` template

---

## Stage 3 — Ingestion

> **Definition:** The process of moving data from the source into your storage system. Ingestion can be batch (scheduled pulls) or streaming (real-time events). This project uses batch ingestion on a fixed schedule.

### In This Project

An **Apache Airflow DAG** runs every hour, calling the OpenWeather API for each city and writing raw JSON to the `raw.weather_events` table. A second step parses and normalizes the raw payload into `warehouse.weather_readings`.

### Airflow DAG Design

```
hourly_weather_ingest (DAG)
│
├── fetch_weather_nairobi     ─── PythonOperator
├── fetch_weather_mombasa     ─── PythonOperator
├── fetch_weather_kisumu      ─── PythonOperator
│        ↓
├── validate_raw_records      ─── PythonOperator (data quality check)
│        ↓
└── parse_to_warehouse        ─── PythonOperator (raw → structured)
```

### Sample DAG Definition

```python
# dags/hourly_weather_ingest.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.clients.openweather import fetch_weather
from src.ingestion.writer import write_raw, parse_to_warehouse

CITIES = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="hourly_weather_ingest",
    default_args=default_args,
    schedule_interval="0 * * * *",   # Every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "ingestion"],
) as dag:

    fetch_tasks = [
        PythonOperator(
            task_id=f"fetch_{city.lower()}",
            python_callable=write_raw,
            op_kwargs={"city": city},
        )
        for city in CITIES
    ]

    parse_task = PythonOperator(
        task_id="parse_to_warehouse",
        python_callable=parse_to_warehouse,
    )

    fetch_tasks >> parse_task
```

### Data Quality Checks at Ingestion

```python
# src/ingestion/validator.py
def validate_record(record: dict) -> bool:
    required_fields = ["city", "dt", "main", "weather"]
    if not all(f in record for f in required_fields):
        return False
    temp = record["main"].get("temp")
    if temp is None or not (-50 < temp < 60):   # Celsius sanity range
        return False
    return True
```

### Tools

| Tool | Role |
|---|---|
| Apache Airflow | DAG scheduler + pipeline orchestration |
| Python `requests` | API polling |
| `pendulum` | Timezone-aware scheduling |
| `great_expectations` | Data quality validation (optional) |
| Docker Compose | Airflow local setup (webserver + scheduler + db) |

### Deliverables

- [ ] `dags/hourly_weather_ingest.py` — Main ingestion DAG
- [ ] `src/clients/openweather.py` — API wrapper
- [ ] `src/ingestion/writer.py` — Raw insert + parse logic
- [ ] `src/ingestion/validator.py` — Quality checks
- [ ] Airflow connection config for PostgreSQL
- [ ] Backfill script for historical data (`scripts/backfill.py`)

---

## Stage 4 — 🔄 Transformation

> **Definition:** The process of cleaning, reshaping, enriching, and aggregating raw data into analytics-ready datasets. This is where business logic lives. In modern data stacks, this stage is where tools like dbt or SQL jobs run.

### In This Project

Transformations run as downstream Airflow tasks (or a separate daily DAG) and produce three outputs: daily summaries, monthly summaries, and temperature anomaly flags.

### Transformation 1 — Daily Averages

```sql
-- Runs daily, aggregates hourly readings
INSERT INTO metrics.daily_summary (
    city, summary_date,
    avg_temp, min_temp, max_temp,
    avg_humidity, total_rainfall
)
SELECT
    city,
    DATE(recorded_at AT TIME ZONE 'Africa/Nairobi') AS summary_date,
    ROUND(AVG(temp_celsius)::NUMERIC, 2),
    ROUND(MIN(temp_celsius)::NUMERIC, 2),
    ROUND(MAX(temp_celsius)::NUMERIC, 2),
    ROUND(AVG(humidity_pct)::NUMERIC, 2),
    ROUND(SUM(rainfall_1h_mm)::NUMERIC, 3)
FROM warehouse.weather_readings
WHERE DATE(recorded_at AT TIME ZONE 'Africa/Nairobi') = CURRENT_DATE - 1
GROUP BY city, summary_date
ON CONFLICT (city, summary_date) DO UPDATE
SET avg_temp = EXCLUDED.avg_temp,
    total_rainfall = EXCLUDED.total_rainfall;
```

### Transformation 2 — Monthly Averages

```sql
-- Runs on 1st of each month for prior month
INSERT INTO metrics.monthly_summary (city, year, month, avg_temp, total_rainfall)
SELECT
    city,
    EXTRACT(YEAR FROM summary_date)::INT,
    EXTRACT(MONTH FROM summary_date)::INT,
    ROUND(AVG(avg_temp)::NUMERIC, 2),
    ROUND(SUM(total_rainfall)::NUMERIC, 3)
FROM metrics.daily_summary
WHERE DATE_TRUNC('month', summary_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
GROUP BY city, 2, 3
ON CONFLICT (city, year, month) DO UPDATE
SET avg_temp = EXCLUDED.avg_temp,
    total_rainfall = EXCLUDED.total_rainfall;
```

### Transformation 3 — Temperature Anomaly Detection

```sql
-- Flags hourly readings that deviate > 2 std deviations from the 30-day rolling baseline
WITH baseline AS (
    SELECT
        city,
        AVG(temp_celsius) AS baseline_avg,
        STDDEV(temp_celsius) AS baseline_std
    FROM warehouse.weather_readings
    WHERE recorded_at >= NOW() - INTERVAL '30 days'
    GROUP BY city
)
INSERT INTO metrics.temperature_anomalies
    (city, recorded_at, temp_celsius, baseline_avg, deviation, is_anomaly)
SELECT
    r.city,
    r.recorded_at,
    r.temp_celsius,
    b.baseline_avg,
    ROUND((r.temp_celsius - b.baseline_avg)::NUMERIC, 2) AS deviation,
    ABS(r.temp_celsius - b.baseline_avg) > (2 * b.baseline_std) AS is_anomaly
FROM warehouse.weather_readings r
JOIN baseline b ON r.city = b.city
WHERE r.recorded_at >= NOW() - INTERVAL '1 hour';
```

### Transformation 4 — Rainfall Trends (Rolling 7-Day)

```sql
SELECT
    city,
    summary_date,
    total_rainfall,
    ROUND(
        AVG(total_rainfall) OVER (
            PARTITION BY city
            ORDER BY summary_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::NUMERIC, 3
    ) AS rolling_7d_rainfall
FROM metrics.daily_summary
ORDER BY city, summary_date DESC;
```

### Tools

| Tool | Role |
|---|---|
| PostgreSQL SQL | Core transformation logic |
| Apache Airflow | Schedule and orchestrate transform jobs |
| Python / Pandas | Ad-hoc transformation and enrichment |
| dbt *(optional)* | Version-controlled, testable SQL transforms |

### Deliverables

- [ ] `sql/transforms/daily_summary.sql`
- [ ] `sql/transforms/monthly_summary.sql`
- [ ] `sql/transforms/temperature_anomalies.sql`
- [ ] `sql/transforms/rainfall_trends.sql`
- [ ] `dags/daily_transforms.py` — Transformation DAG
- [ ] dbt project (`dbt/`) with models and tests *(optional)*
- [ ] Transform test suite (`tests/test_transforms.py`)

---

## Stage 5 — 📊 Serving

> **Definition:** Making transformed data available and consumable — through dashboards, APIs, reports, or exports. This is the final mile where stakeholders interact with your data.

### In This Project

**Grafana** connects directly to PostgreSQL and renders live dashboards showing weather trends, anomalies, and city comparisons.

### Dashboard Panels

| Panel | Type | Data Source Query |
|---|---|---|
| Current Temperature by City | Stat Panel | `SELECT city, avg_temp FROM metrics.daily_summary WHERE summary_date = CURRENT_DATE` |
| Temperature Trend (7-day) | Time Series | `daily_summary` by city |
| Monthly Rainfall Comparison | Bar Chart | `monthly_summary` grouped by city |
| Rainfall Rolling Average | Line Chart | `rainfall_trends` view |
| Temperature Anomalies | Table | `temperature_anomalies WHERE is_anomaly = true` |
| City Weather Heatmap | Heatmap | Hourly `warehouse.weather_readings` |

### Grafana Setup

```yaml
# docker-compose.yml (Grafana service)
grafana:
  image: grafana/grafana:latest
  ports:
    - "3000:3000"
  environment:
    - GF_SECURITY_ADMIN_PASSWORD=admin
  volumes:
    - grafana_data:/var/lib/grafana
    - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
    - ./grafana/datasources:/etc/grafana/provisioning/datasources
```

```yaml
# grafana/datasources/postgres.yaml
apiVersion: 1
datasources:
  - name: WeatherWarehouse
    type: postgres
    url: postgres:5432
    database: weather_db
    user: grafana_reader
    secureJsonData:
      password: ${GRAFANA_DB_PASSWORD}
    jsonData:
      sslmode: disable
      timescaledb: false
```

### Tools

| Tool | Role |
|---|---|
| Grafana | Dashboard and visualization layer |
| PostgreSQL (as data source) | Direct query from Grafana |
| `grafana/grafana` Docker image | Local Grafana instance |
| Grafana provisioning YAMLs | Version-controlled dashboard configs |

### Deliverables

- [ ] Grafana Docker service in `docker-compose.yml`
- [ ] `grafana/datasources/postgres.yaml` — DB connection config
- [ ] `grafana/dashboards/weather_overview.json` — Main dashboard export
- [ ] `grafana/dashboards/anomalies.json` — Anomaly monitoring dashboard
- [ ] Read-only `grafana_reader` PostgreSQL role + SQL script
- [ ] Dashboard screenshots in `docs/screenshots/`

---

## 🗓️ Project Roadmap

### Phase 1 — Foundation (Week 1)

**Goal:** Get the environment running end-to-end with one city.

| Task | Stage | Status |
|---|---|---|
| Set up repo structure and `README.md` | All | `[ ]` |
| Create `docker-compose.yml` (Postgres + Airflow + Grafana) | Storage / Serving | `[ ]` |
| Provision OpenWeather API key | Generation | `[ ]` |
| Write `openweather.py` API client | Generation | `[ ]` |
| Create raw schema + `weather_events` table | Storage | `[ ]` |
| Write basic ingestion script (manual, single city) | Ingestion | `[ ]` |
| Verify data lands in PostgreSQL | Storage | `[ ]` |

---

### Phase 2 — Ingestion Pipeline (Week 2)

**Goal:** Automate hourly collection across all cities.

| Task | Stage | Status |
|---|---|---|
| Create `warehouse.weather_readings` schema | Storage | `[ ]` |
| Write `writer.py` (raw insert + parse logic) | Ingestion | `[ ]` |
| Write `validator.py` (data quality checks) | Ingestion | `[ ]` |
| Build Airflow DAG `hourly_weather_ingest.py` | Ingestion | `[ ]` |
| Add all 5 cities to config | Ingestion | `[ ]` |
| Run backfill for past 7 days | Ingestion | `[ ]` |
| Verify DAG runs and data appears in both schemas | Ingestion | `[ ]` |

---

### Phase 3 — Transformation Layer (Week 3)

**Goal:** Compute all derived metrics from raw readings.

| Task | Stage | Status |
|---|---|---|
| Build `daily_summary.sql` and test | Transformation | `[ ]` |
| Build `monthly_summary.sql` and test | Transformation | `[ ]` |
| Build `temperature_anomalies.sql` and test | Transformation | `[ ]` |
| Build `rainfall_trends.sql` (rolling window) | Transformation | `[ ]` |
| Create Airflow DAG `daily_transforms.py` | Transformation | `[ ]` |
| Write SQL unit tests (`tests/`) | Transformation | `[ ]` |
| Backfill all metrics tables from historical raw | Transformation | `[ ]` |

---

### Phase 4 — Serving & Dashboards (Week 4)

**Goal:** Visual, shareable dashboards in Grafana.

| Task | Stage | Status |
|---|---|---|
| Create `grafana_reader` read-only DB role | Serving | `[ ]` |
| Configure Grafana data source (PostgreSQL) | Serving | `[ ]` |
| Build weather overview dashboard | Serving | `[ ]` |
| Build anomaly monitoring dashboard | Serving | `[ ]` |
| Export dashboard JSON for version control | Serving | `[ ]` |
| Write provisioning YAMLs for reproducibility | Serving | `[ ]` |
| Document all panels with query explanations | Serving | `[ ]` |

---

### Phase 5 — Hardening & Polish (Week 5)

**Goal:** Make the project production-grade and portfolio-ready.

| Task | Stage | Status |
|---|---|---|
| Add Airflow alerting (email on DAG failure) | Ingestion | `[ ]` |
| Add data freshness checks (SLA monitoring) | Transformation | `[ ]` |
| Write full `README.md` with architecture diagram | All | `[ ]` |
| Add `.env.example` and `Makefile` | All | `[ ]` |
| Write `CONTRIBUTING.md` | All | `[ ]` |
| Performance-tune PostgreSQL indexes | Storage | `[ ]` |
| Record a demo video / add screenshots | Serving | `[ ]` |

---

## 📁 Project Structure

```
weather-data-warehouse/
│
├── dags/
│   ├── hourly_weather_ingest.py       # Ingestion DAG
│   └── daily_transforms.py            # Transformation DAG
│
├── src/
│   ├── clients/
│   │   └── openweather.py             # API wrapper
│   ├── ingestion/
│   │   ├── writer.py                  # Raw + parsed inserts
│   │   └── validator.py               # Data quality checks
│   └── db/
│       ├── connection.py              # Connection pool
│       └── models.py                  # SQLAlchemy models
│
├── sql/
│   ├── migrations/
│   │   ├── 001_init_schemas.sql
│   │   └── 002_create_metrics.sql
│   └── transforms/
│       ├── daily_summary.sql
│       ├── monthly_summary.sql
│       ├── temperature_anomalies.sql
│       └── rainfall_trends.sql
│
├── grafana/
│   ├── datasources/
│   │   └── postgres.yaml
│   └── dashboards/
│       ├── weather_overview.json
│       └── anomalies.json
│
├── data/
│   └── samples/                       # Sample API responses for dev/testing
│
├── tests/
│   ├── test_client.py
│   ├── test_transforms.py
│   └── test_validator.py
│
├── scripts/
│   ├── init_db.sql
│   └── backfill.py
│
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🛠️ Full Technology Stack

| Component | Tool | Version |
|---|---|---|
| Language | Python | 3.11+ |
| Data Source | OpenWeather API | v2.5 / v3.0 |
| Database | PostgreSQL | 15+ |
| Orchestration | Apache Airflow | 2.8+ |
| Visualization | Grafana | 10+ |
| ORM | SQLAlchemy | 2.0+ |
| DB Adapter | psycopg2 | 2.9+ |
| Migrations | Alembic | 1.13+ |
| Containerization | Docker + Docker Compose | Latest |
| Testing | pytest | 7+ |
| Config Management | python-dotenv | 1.0+ |

---

## 🎓 Skills Learned

| Skill | Where It's Applied |
|---|---|
| **Batch Pipelines** | Airflow DAG scheduling hourly + daily runs |
| **Time-Series Data** | Hourly weather readings, rolling averages, trend queries |
| **Data Warehousing** | Three-layer schema (raw / warehouse / metrics), SCD patterns |
| **Dashboarding** | Grafana panels connected to PostgreSQL metrics layer |
| **Data Quality** | Validation at ingestion, null checks, range checks |
| **SQL Window Functions** | Rolling 7-day rainfall, anomaly baselines |
| **Docker & DevOps** | Full local stack via Docker Compose |
| **Pipeline Observability** | Airflow task logs, retry logic, SLA monitoring |

---

## ⚙️ Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/yourhandle/weather-data-warehouse.git
cd weather-data-warehouse
cp .env.example .env
# Fill in your OPENWEATHER_API_KEY and DATABASE_URL

# 2. Start the full stack
docker compose up -d

# 3. Initialise the database
docker compose exec postgres psql -U postgres -f /scripts/init_db.sql

# 4. Trigger a manual ingestion run
docker compose exec airflow airflow dags trigger hourly_weather_ingest

# 5. Open the dashboards
open http://localhost:3000   # Grafana (admin / admin)
open http://localhost:8080   # Airflow UI
```

---

## 📄 License

MIT — free to use, extend, and adapt.