"""
GCS → BigQuery Loader (Raw / Bronze Layer)
============================================
Creates the dataset and table in BigQuery if they don't exist,
then loads the CSV from GCS into the raw table.

Dataset structure:
  Project : whsc-homologacao
  Dataset : diesel_raw
  Table   : dyno_measurements

Requirements:
  pip install google-cloud-bigquery

Authentication (same as before):
  gcloud auth application-default login

Author: [Your Name]
Project: diesel-emissions-pipeline
"""

import sys
from datetime import datetime

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import Conflict, NotFound
except ImportError:
    print("ERROR: google-cloud-bigquery not installed.")
    print("Run: pip install google-cloud-bigquery")
    sys.exit(1)


# ─── Configuration ────────────────────────────────────────────────────────────
GCP_PROJECT_ID  = "whsc-homologacao"
GCS_URI         = "gs://whsc-homolo/dyno_emissions_data.csv"
DATASET_ID      = "diesel_raw"
TABLE_ID        = "dyno_measurements"
DATASET_REGION  = "US"
# ─────────────────────────────────────────────────────────────────────────────


# ─── Schema definition ────────────────────────────────────────────────────────
# Explicit schema is better than auto-detect:
# - Garante tipos corretos (BOOLEAN não vira STRING)
# - Documenta o dado para quem vier depois
# - O dbt vai usar esse schema como referência
SCHEMA = [
    # Test metadata
    bigquery.SchemaField("test_id",            "STRING",  description="Unique test run identifier"),
    bigquery.SchemaField("engine_id",          "STRING",  description="Engine under test identifier"),
    bigquery.SchemaField("test_cell",          "STRING",  description="Dynamometer cell used"),
    bigquery.SchemaField("timestamp",          "TIMESTAMP", description="Measurement timestamp (UTC)"),

    # WHSC cycle info
    bigquery.SchemaField("whsc_mode",          "INTEGER", description="WHSC mode number (1-13)"),
    bigquery.SchemaField("mode_speed_pct",     "FLOAT",   description="Speed as % of rated speed"),
    bigquery.SchemaField("mode_torque_pct",    "FLOAT",   description="Torque as % of max torque"),
    bigquery.SchemaField("mode_weight_factor", "FLOAT",   description="WHSC weighting factor for this mode"),
    bigquery.SchemaField("sample_num",         "INTEGER", description="Sample number within mode"),

    # Engine operating conditions
    bigquery.SchemaField("rpm",                "FLOAT",   description="Engine speed (RPM)"),
    bigquery.SchemaField("torque_Nm",          "FLOAT",   description="Engine torque (Nm)"),
    bigquery.SchemaField("power_kW",           "FLOAT",   description="Brake power output (kW)"),
    bigquery.SchemaField("fuel_flow_g_h",      "FLOAT",   description="Fuel mass flow rate (g/h)"),
    bigquery.SchemaField("boost_pressure_bar", "FLOAT",   description="Turbocharger boost pressure (bar)"),
    bigquery.SchemaField("lambda",             "FLOAT",   description="Excess air ratio (lambda)"),

    # Exhaust emissions (g/kWh)
    bigquery.SchemaField("NOx_g_kWh",         "FLOAT",   description="NOx emissions (g/kWh)"),
    bigquery.SchemaField("CO_g_kWh",          "FLOAT",   description="CO emissions (g/kWh)"),
    bigquery.SchemaField("CO2_g_kWh",         "FLOAT",   description="CO2 emissions (g/kWh)"),
    bigquery.SchemaField("HC_g_kWh",          "FLOAT",   description="HC emissions (g/kWh)"),
    bigquery.SchemaField("PM_g_kWh",          "FLOAT",   description="Particulate matter (g/kWh)"),

    # Temperatures (°C)
    bigquery.SchemaField("coolant_temp_C",    "FLOAT",   description="Engine coolant temperature (°C)"),
    bigquery.SchemaField("exhaust_temp_C",    "FLOAT",   description="Exhaust gas temperature (°C)"),
    bigquery.SchemaField("oil_temp_C",        "FLOAT",   description="Engine oil temperature (°C)"),
    bigquery.SchemaField("intake_air_temp_C", "FLOAT",   description="Intake air temperature (°C)"),

    # EPA Tier 4 compliance flags
    bigquery.SchemaField("epa_tier4_NOx_pass", "BOOLEAN", description="NOx within EPA Tier 4 Final limit"),
    bigquery.SchemaField("epa_tier4_CO_pass",  "BOOLEAN", description="CO within EPA Tier 4 Final limit"),
    bigquery.SchemaField("epa_tier4_HC_pass",  "BOOLEAN", description="HC within EPA Tier 4 Final limit"),
    bigquery.SchemaField("epa_tier4_PM_pass",  "BOOLEAN", description="PM within EPA Tier 4 Final limit"),

    # Euro VI compliance flags
    bigquery.SchemaField("euro6_NOx_pass",    "BOOLEAN", description="NOx within Euro VI limit"),
    bigquery.SchemaField("euro6_CO_pass",     "BOOLEAN", description="CO within Euro VI limit"),
    bigquery.SchemaField("euro6_HC_pass",     "BOOLEAN", description="HC within Euro VI limit"),
    bigquery.SchemaField("euro6_PM_pass",     "BOOLEAN", description="PM within Euro VI limit"),
]


def create_dataset(client: bigquery.Client, dataset_id: str, region: str):
    """Create BigQuery dataset if it doesn't exist."""
    full_id = f"{GCP_PROJECT_ID}.{dataset_id}"
    dataset  = bigquery.Dataset(full_id)
    dataset.location = region
    dataset.description = (
        "Raw (bronze) layer for diesel engine dynamometer emissions data. "
        "Data loaded directly from GCS — no transformations applied."
    )
    try:
        client.create_dataset(dataset, timeout=30)
        print(f"Dataset created: {full_id}")
    except Conflict:
        print(f"Dataset already exists: {full_id}")


def create_table(client: bigquery.Client, dataset_id: str, table_id: str):
    """Create BigQuery table with explicit schema if it doesn't exist."""
    full_id = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"
    table    = bigquery.Table(full_id, schema=SCHEMA)
    table.description = (
        "Raw dynamometer measurements following WHSC (World Harmonized Stationary Cycle). "
        "Includes engine operating conditions, exhaust emissions, temperatures, "
        "and compliance flags against EPA Tier 4 Final and Euro VI limits."
    )
    try:
        client.create_table(table, timeout=30)
        print(f"Table created: {full_id}")
    except Conflict:
        print(f"Table already exists: {full_id} — will overwrite data.")


def load_gcs_to_bigquery(client: bigquery.Client, gcs_uri: str, dataset_id: str, table_id: str):
    """Load CSV from GCS into BigQuery table."""
    full_table_id = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,          # skip header row
        write_disposition="WRITE_TRUNCATE",  # overwrite table on each run
        null_marker="",
    )

    print(f"\nStarting load job...")
    print(f"  Source      : {gcs_uri}")
    print(f"  Destination : {full_table_id}")

    start = datetime.utcnow()
    load_job = client.load_table_from_uri(gcs_uri, full_table_id, job_config=job_config)
    load_job.result()  # waits for job to complete
    elapsed = (datetime.utcnow() - start).total_seconds()

    if load_job.errors:
        print(f"\nERROR during load:")
        for err in load_job.errors:
            print(f"  {err}")
        sys.exit(1)

    return elapsed


def verify_load(client: bigquery.Client, dataset_id: str, table_id: str):
    """Verify load by checking row count and a quick sample query."""
    full_table_id = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"

    table = client.get_table(full_table_id)
    print(f"\nVerification:")
    print(f"  Rows loaded : {table.num_rows:,}")
    print(f"  Table size  : {table.num_bytes / 1024:.1f} KB")

    # Quick sanity check: NOx stats per engine
    query = f"""
        SELECT
            engine_id,
            COUNT(*)                        AS total_samples,
            ROUND(AVG(NOx_g_kWh), 4)        AS avg_NOx,
            ROUND(MAX(NOx_g_kWh), 4)        AS max_NOx,
            COUNTIF(NOT epa_tier4_NOx_pass) AS nox_failures
        FROM `{full_table_id}`
        GROUP BY engine_id
        ORDER BY engine_id
    """

    print(f"\nQuick NOx summary by engine:")
    print(f"  {'Engine':<12} {'Samples':>10} {'Avg NOx':>10} {'Max NOx':>10} {'Failures':>10}")
    print(f"  {'-'*56}")

    results = client.query(query).result()
    for row in results:
        print(
            f"  {row.engine_id:<12} "
            f"{row.total_samples:>10,} "
            f"{row.avg_NOx:>10.4f} "
            f"{row.max_NOx:>10.4f} "
            f"{row.nox_failures:>10,}"
        )


def print_next_steps(dataset_id: str, table_id: str):
    """Print next steps for dbt setup."""
    full_table_id = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"
    print(f"""
─────────────────────────────────────────────────────
NEXT STEP — dbt setup
─────────────────────────────────────────────────────
Your raw table is ready:
  {full_table_id}

Next we will:
  1. Install dbt-bigquery
  2. Create dbt project (dbt init)
  3. Configure profiles.yml to connect to this table
  4. Create staging model: stg_dyno_measurements
  5. Create mart model: emissions_compliance_summary

Run this to install dbt:
  pip install dbt-core dbt-bigquery
─────────────────────────────────────────────────────
""")


def main():
    print("=" * 55)
    print("  Diesel Emissions Pipeline — GCS → BigQuery Load")
    print("=" * 55)
    print(f"  Project : {GCP_PROJECT_ID}")
    print(f"  Source  : {GCS_URI}")
    print(f"  Dataset : {DATASET_ID}")
    print(f"  Table   : {TABLE_ID}")
    print()

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Step 1: Create dataset
    create_dataset(client, DATASET_ID, DATASET_REGION)

    # Step 2: Create table with schema
    create_table(client, DATASET_ID, TABLE_ID)

    # Step 3: Load data
    elapsed = load_gcs_to_bigquery(client, GCS_URI, DATASET_ID, TABLE_ID)
    print(f"\nLoad complete in {elapsed:.1f}s")

    # Step 4: Verify
    verify_load(client, DATASET_ID, TABLE_ID)

    # Step 5: Next steps
    print_next_steps(DATASET_ID, TABLE_ID)


if __name__ == "__main__":
    main()
    