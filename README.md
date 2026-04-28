# Diesel Emissions Pipeline

End-to-end data engineering pipeline for diesel engine emissions analysis, built on GCP. Simulates a real-world homologation environment using the WHSC (World Harmonized Stationary Cycle) and validates exhaust emissions against EPA Tier 4 Final and Euro VI regulatory limits.

---

## Architecture

```
generate_dyno_data.py          # Simulated WHSC dynamometer data
        │
        ▼
GCP Cloud Storage              # Raw CSV — date-partitioned landing zone
        │
        ▼
BigQuery — diesel_raw          # Bronze layer — explicit schema, no transformations
        │
        ▼ dbt
BigQuery — diesel_silver       # Silver layer — cleaned, standardized, tested
        │
        ├──▶ emissions_compliance    # Gold — EPA Tier 4 / Euro VI weighted compliance
        └──▶ engine_performance      # Gold — BSFC, power curves, engine ranking
                │
                ▼
        Looker Studio Dashboard     # Connected directly to BigQuery
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data generation | Python |
| Cloud storage | GCP Cloud Storage |
| Data warehouse | BigQuery |
| Transformation | dbt-core + dbt-bigquery |
| Orchestration | Apache Airflow (Docker) — in progress |
| Visualization | Looker Studio |
| Version control | Git + GitHub |

---

## Domain Context

This pipeline models a real diesel engine test bench environment. The data follows the **WHSC (World Harmonized Stationary Cycle)** — a 13-mode steady-state test cycle used internationally for heavy-duty engine homologation.

Each test run measures:
- Engine operating conditions: RPM, torque, brake power, fuel flow, boost pressure, lambda
- Exhaust emissions: NOx, CO, CO₂, HC, PM (g/kWh)
- Temperatures: coolant, exhaust gas, oil, intake air
- Regulatory compliance flags: EPA Tier 4 Final and Euro VI per pollutant

Emissions are aggregated using the official WHSC weighting factors to produce a single weighted result per test run, which is then compared against regulatory limits.

**EPA Tier 4 Final limits (g/kWh):**

| Pollutant | Limit |
|---|---|
| NOx | 0.40 |
| CO | 3.50 |
| HC | 0.19 |
| PM | 0.02 |

**Euro VI limits (g/kWh):**

| Pollutant | Limit |
|---|---|
| NOx | 0.40 |
| CO | 4.00 |
| HC | 0.16 |
| PM | 0.01 |

---

## Data Models

### Bronze — `diesel_raw.dyno_measurements`
Raw sensor readings loaded directly from GCS. One row per measurement sample. Schema is explicitly defined — no auto-detect. 32 columns including all operating conditions, emissions and compliance flags.

### Silver — `diesel_silver.stg_dyno_measurements`
Cleaned and standardized measurements. Adds derived columns:
- `load_category` — idle / low_load / mid_load / high_load / full_load
- `bsfc_g_kwh` — brake specific fuel consumption
- `epa_tier4_overall_pass` — combined compliance flag across all four pollutants
- `euro6_overall_pass` — combined Euro VI compliance flag

### Gold — `diesel_silver.emissions_compliance`
One row per test run. WHSC weighted emissions compared against EPA and Euro VI limits. Includes NOx margin percentage and failure rate per test. Used for regulatory reporting dashboards.

### Gold — `diesel_silver.engine_performance`
Efficiency metrics aggregated by engine, WHSC mode and load category. Includes BSFC curves, power output, exhaust temperatures and engine ranking by NOx and fuel efficiency within each operating mode.

---

## dbt Tests

45 data quality tests covering all three layers:

- `not_null` on all critical columns across bronze, silver and gold
- `unique` on test_id in the compliance mart
- Source freshness checks on the raw BigQuery table

```bash
dbt test
# Done. PASS=45 WARN=0 ERROR=0 SKIP=0 TOTAL=45
```

---

## Project Structure

```
diesel-emissions-pipeline/
├── config.py                        # Central configuration (project, bucket, dataset)
├── generate_dyno_data.py            # WHSC data simulator
├── upload_to_gcs.py                 # GCS ingestion script
├── load_gcs_to_bigquery.py          # BigQuery loader with explicit schema
└── diesel_emissions/                # dbt project
    └── models/
        ├── staging/
        │   ├── stg_dyno_measurements.sql
        │   ├── sources.yml
        │   └── schema.yml
        └── marts/
            ├── emissions_compliance.sql
            └── engine_performance.sql
```

---

## How to Run

### Prerequisites
- Python 3.11+
- GCP project with BigQuery and Cloud Storage APIs enabled
- Service Account JSON key with roles: Storage Object Admin, BigQuery Data Editor, BigQuery Job User
- dbt-core and dbt-bigquery installed

### Setup

```bash
# Install dependencies
pip install google-cloud-storage google-cloud-bigquery dbt-core dbt-bigquery

# Clone the repository
git clone https://github.com/jachsondenizjr/diesel-emissions-pipeline.git
cd diesel-emissions-pipeline
```

### Configure

Edit `config.py` with your GCP project details:

```python
CREDENTIALS_PATH = r"path/to/your-service-account.json"
GCP_PROJECT_ID   = "your-gcp-project-id"
GCS_BUCKET_NAME  = "your-bucket-name"
```

### Run the pipeline

```bash
# Step 1 — Generate simulated dynamometer data
python generate_dyno_data.py

# Step 2 — Upload CSV to GCS
python upload_to_gcs.py

# Step 3 — Load raw data into BigQuery
python load_gcs_to_bigquery.py

# Step 4 — Run dbt transformations
cd diesel_emissions
dbt run

# Step 5 — Run data quality tests
dbt test
```

---

## Key Findings

The pipeline processes 20 WHSC test runs across 3 engines (ENG-001, ENG-002, ENG-003) in 2 test cells (DYNO-A, DYNO-B), generating 2,600 measurement samples across all 13 operating modes.

- NOx emissions peak at high-load modes (modes 7 and 8) as expected from combustion temperature increase
- PM emissions show higher variability at mid-load conditions, consistent with real-world diesel combustion behavior
- BSFC reaches optimal values between 60–80% load, aligning with engine efficiency curves
- Euro VI PM limit (0.01 g/kWh) is the most restrictive constraint across all test runs

---

## About

This project was built as part of a Data Engineering portfolio transition, combining 15+ years of mechanical engineering and diesel engine homologation experience with modern data stack technologies. The domain knowledge behind the data model — WHSC cycles, EPA Tier 4, Euro VI limits, BSFC curves — comes from hands-on experience with engine dynamometer testing.

---

## License

MIT