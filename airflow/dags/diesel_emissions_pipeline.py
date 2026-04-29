"""
diesel_emissions_pipeline.py
=============================
Airflow DAG — orchestrates the full diesel emissions pipeline:

  1. generate_data   : Generate WHSC dynamometer CSV
  2. upload_gcs      : Upload CSV to GCP Cloud Storage
  3. load_bigquery   : Load raw data into BigQuery
  4. dbt_run         : Run dbt transformations (silver + gold)
  5. dbt_test        : Run dbt data quality tests

Schedule: Daily at 06:00 UTC
Owner: jachson.denizjr

Project: diesel-emissions-pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default arguments ────────────────────────────────────────────
default_args = {
    "owner":            "jachson.denizjr",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ───────────────────────────────────────────────
with DAG(
    dag_id="diesel_emissions_pipeline",
    default_args=default_args,
    description="End-to-end diesel engine emissions data pipeline",
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["diesel", "emissions", "gcp", "dbt", "bigquery"],
) as dag:

    # ── Task 1 — Generate simulated WHSC data ────────────────────
    generate_data = BashOperator(
        task_id="generate_dyno_data",
        bash_command="cd /opt/airflow/pipeline && python generate_dyno_data.py",
        doc_md="""
        ## Generate Dynamometer Data
        Runs the WHSC data simulator and produces `dyno_emissions_data.csv`.
        Simulates 20 test runs across 13 WHSC modes with realistic emissions values.
        """,
    )

    # ── Task 2 — Upload to GCS ───────────────────────────────────
    upload_gcs = BashOperator(
        task_id="upload_to_gcs",
        bash_command="cd /opt/airflow/pipeline && python upload_to_gcs.py",
        doc_md="""
        ## Upload to GCS
        Uploads the generated CSV to GCP Cloud Storage bucket.
        File is partitioned by date: raw/dyno_emissions/year=.../month=.../day=.../
        """,
    )

    # ── Task 3 — Load into BigQuery ──────────────────────────────
    load_bigquery = BashOperator(
        task_id="load_gcs_to_bigquery",
        bash_command="cd /opt/airflow/pipeline && python load_gcs_to_bigquery.py",
        doc_md="""
        ## Load into BigQuery
        Loads the CSV from GCS into BigQuery raw table (bronze layer).
        Uses explicit schema — no auto-detect. WRITE_TRUNCATE on each run.
        """,
    )

    # ── Task 4 — dbt run ─────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/pipeline/diesel_emissions && dbt run --profiles-dir /root/.dbt",
        doc_md="""
        ## dbt Run
        Executes all dbt models:
        - stg_dyno_measurements (silver)
        - emissions_compliance (gold)
        - engine_performance (gold)
        """,
    )

    # ── Task 5 — dbt test ────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/pipeline/diesel_emissions && dbt test --profiles-dir /root/.dbt",
        doc_md="""
        ## dbt Test
        Runs 45 data quality tests across bronze, silver and gold layers.
        Pipeline fails if any test fails — ensuring data integrity.
        """,
    )

    # ── Task dependencies ────────────────────────────────────────
    # Linear pipeline: each task depends on the previous
    generate_data >> upload_gcs >> load_bigquery >> dbt_run >> dbt_test
