"""
GCS → BigQuery Loader (v2 — Service Account auth)
===================================================
Creates dataset + table in BigQuery and loads the CSV from GCS.
Authenticates using a Service Account JSON key file.

Requirements:
  pip install google-cloud-bigquery

Author: [Your Name]
Project: diesel-emissions-pipeline
"""

import sys
from datetime import datetime

try:
    from config import (
        GCP_PROJECT_ID, BQ_DATASET_RAW, BQ_TABLE_RAW,
        BQ_FULL_TABLE, GCS_CSV_URI, DATASET_REGION,
        CREDENTIALS_PATH, validate
    )
except ImportError:
    print("ERROR: config.py not found. Make sure it's in the same folder.")
    sys.exit(1)

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.api_core.exceptions import Conflict
except ImportError:
    print("ERROR: Run:  pip install google-cloud-bigquery")
    sys.exit(1)


# ─── Schema ───────────────────────────────────────────────────────────────────
SCHEMA = [
    bigquery.SchemaField("test_id",             "STRING",    description="Unique test run identifier"),
    bigquery.SchemaField("engine_id",           "STRING",    description="Engine under test identifier"),
    bigquery.SchemaField("test_cell",           "STRING",    description="Dynamometer cell used"),
    bigquery.SchemaField("timestamp",           "TIMESTAMP", description="Measurement timestamp (UTC)"),
    bigquery.SchemaField("whsc_mode",           "INTEGER",   description="WHSC mode number (1-13)"),
    bigquery.SchemaField("mode_speed_pct",      "FLOAT",     description="Speed as % of rated speed"),
    bigquery.SchemaField("mode_torque_pct",     "FLOAT",     description="Torque as % of max torque"),
    bigquery.SchemaField("mode_weight_factor",  "FLOAT",     description="WHSC weighting factor"),
    bigquery.SchemaField("sample_num",          "INTEGER",   description="Sample number within mode"),
    bigquery.SchemaField("rpm",                 "FLOAT",     description="Engine speed (RPM)"),
    bigquery.SchemaField("torque_Nm",           "FLOAT",     description="Engine torque (Nm)"),
    bigquery.SchemaField("power_kW",            "FLOAT",     description="Brake power output (kW)"),
    bigquery.SchemaField("fuel_flow_g_h",       "FLOAT",     description="Fuel mass flow rate (g/h)"),
    bigquery.SchemaField("boost_pressure_bar",  "FLOAT",     description="Turbocharger boost pressure (bar)"),
    bigquery.SchemaField("lambda",              "FLOAT",     description="Excess air ratio"),
    bigquery.SchemaField("NOx_g_kWh",          "FLOAT",     description="NOx emissions (g/kWh)"),
    bigquery.SchemaField("CO_g_kWh",           "FLOAT",     description="CO emissions (g/kWh)"),
    bigquery.SchemaField("CO2_g_kWh",          "FLOAT",     description="CO2 emissions (g/kWh)"),
    bigquery.SchemaField("HC_g_kWh",           "FLOAT",     description="HC emissions (g/kWh)"),
    bigquery.SchemaField("PM_g_kWh",           "FLOAT",     description="Particulate matter (g/kWh)"),
    bigquery.SchemaField("coolant_temp_C",      "FLOAT",     description="Coolant temperature (°C)"),
    bigquery.SchemaField("exhaust_temp_C",      "FLOAT",     description="Exhaust gas temperature (°C)"),
    bigquery.SchemaField("oil_temp_C",          "FLOAT",     description="Oil temperature (°C)"),
    bigquery.SchemaField("intake_air_temp_C",   "FLOAT",     description="Intake air temperature (°C)"),
    bigquery.SchemaField("epa_tier4_NOx_pass",  "BOOLEAN",   description="NOx within EPA Tier 4 limit"),
    bigquery.SchemaField("epa_tier4_CO_pass",   "BOOLEAN",   description="CO within EPA Tier 4 limit"),
    bigquery.SchemaField("epa_tier4_HC_pass",   "BOOLEAN",   description="HC within EPA Tier 4 limit"),
    bigquery.SchemaField("epa_tier4_PM_pass",   "BOOLEAN",   description="PM within EPA Tier 4 limit"),
    bigquery.SchemaField("euro6_NOx_pass",      "BOOLEAN",   description="NOx within Euro VI limit"),
    bigquery.SchemaField("euro6_CO_pass",       "BOOLEAN",   description="CO within Euro VI limit"),
    bigquery.SchemaField("euro6_HC_pass",       "BOOLEAN",   description="HC within Euro VI limit"),
    bigquery.SchemaField("euro6_PM_pass",       "BOOLEAN",   description="PM within Euro VI limit"),
]


def get_client() -> bigquery.Client:
    """Create authenticated BigQuery client using Service Account JSON."""
    creds_path = validate()
    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)


def create_dataset(client: bigquery.Client):
    dataset = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}")
    dataset.location = DATASET_REGION
    dataset.description = "Raw (bronze) layer — diesel dynamometer emissions data."
    try:
        client.create_dataset(dataset, timeout=30)
        print(f"  Dataset created : {GCP_PROJECT_ID}.{BQ_DATASET_RAW}")
    except Conflict:
        print(f"  Dataset exists  : {GCP_PROJECT_ID}.{BQ_DATASET_RAW}")


def create_table(client: bigquery.Client):
    table = bigquery.Table(BQ_FULL_TABLE, schema=SCHEMA)
    table.description = (
        "Raw WHSC dynamometer measurements. "
        "Includes engine conditions, exhaust emissions and EPA/Euro VI compliance flags."
    )
    try:
        client.create_table(table, timeout=30)
        print(f"  Table created   : {BQ_FULL_TABLE}")
    except Conflict:
        print(f"  Table exists    : {BQ_FULL_TABLE} — data will be overwritten")


def load_data(client: bigquery.Client) -> float:
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        null_marker="",
    )
    print(f"\n  Source      : {GCS_CSV_URI}")
    print(f"  Destination : {BQ_FULL_TABLE}")

    start    = datetime.utcnow()
    job      = client.load_table_from_uri(GCS_CSV_URI, BQ_FULL_TABLE, job_config=job_config)
    job.result()
    elapsed  = (datetime.utcnow() - start).total_seconds()

    if job.errors:
        for err in job.errors:
            print(f"  ERROR: {err}")
        sys.exit(1)

    return elapsed


def verify(client: bigquery.Client):
    table = client.get_table(BQ_FULL_TABLE)
    print(f"\n  Rows loaded : {table.num_rows:,}")
    print(f"  Table size  : {table.num_bytes / 1024:.1f} KB")

    query = f"""
        SELECT
            engine_id,
            COUNT(*)                        AS samples,
            ROUND(AVG(NOx_g_kWh), 4)        AS avg_NOx,
            ROUND(MAX(NOx_g_kWh), 4)        AS max_NOx,
            COUNTIF(NOT epa_tier4_NOx_pass) AS nox_failures
        FROM `{BQ_FULL_TABLE}`
        GROUP BY engine_id
        ORDER BY engine_id
    """
    print(f"\n  NOx summary by engine (EPA Tier 4):")
    print(f"  {'Engine':<12} {'Samples':>8} {'Avg NOx':>9} {'Max NOx':>9} {'Failures':>10}")
    print(f"  {'-'*52}")
    for row in client.query(query).result():
        print(
            f"  {row.engine_id:<12}"
            f"{row.samples:>8,}"
            f"{row.avg_NOx:>9.4f}"
            f"{row.max_NOx:>9.4f}"
            f"{row.nox_failures:>10,}"
        )


def main():
    print("=" * 55)
    print("  Diesel Emissions Pipeline — GCS → BigQuery")
    print("=" * 55)

    client = get_client()
    print(f"Authenticated: Service Account OK")
    print(f"Project: {GCP_PROJECT_ID}\n")

    print("Step 1/3 — Creating dataset and table...")
    create_dataset(client)
    create_table(client)

    print("\nStep 2/3 — Loading data...")
    elapsed = load_data(client)
    print(f"  Loaded in   : {elapsed:.1f}s")

    print("\nStep 3/3 — Verifying load...")
    verify(client)

    print(f"""
─────────────────────────────────────────────────────
Done! Raw table is ready in BigQuery.

Next step: dbt setup
  pip install dbt-core dbt-bigquery
  dbt init diesel_emissions_pipeline
─────────────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()
