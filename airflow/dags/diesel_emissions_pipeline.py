"""
diesel_emissions_pipeline.py
=============================
Airflow DAG — orquestra o pipeline completo de emissões diesel:

  1. generate_data  : Gera o CSV simulado WHSC
  2. upload_gcs     : Faz upload para o GCS (retorna URI particionado via XCom)
  3. load_bigquery  : Carrega o CSV do GCS para o BigQuery (usa URI do XCom)
  4. dbt_run        : Executa as transformações dbt (Silver + Gold)
  5. dbt_test       : Executa os 45 testes de qualidade dbt

Schedule: diário às 06:00 UTC
Owner: jachson.denizjr
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Callables Python ─────────────────────────────────────────────────────────
# Importados aqui para que o Airflow os serialize corretamente.
# Cada callable carrega suas próprias settings — sem estado global no módulo.

def _generate_data() -> None:
    from pipeline.generator import DynoDataGenerator
    from pipeline.settings import load_settings
    DynoDataGenerator(load_settings()).run()


def _upload_gcs() -> str:
    """Retorna o GCS URI — o Airflow salva automaticamente no XCom (return_value)."""
    from pipeline.gcs_uploader import GCSUploader
    from pipeline.settings import load_settings
    return GCSUploader(load_settings()).run()


def _load_bigquery(ti) -> None:
    """Recebe o GCS URI do XCom da task anterior."""
    from pipeline.bq_loader import BigQueryLoader
    from pipeline.settings import load_settings
    gcs_uri = ti.xcom_pull(task_ids="upload_to_gcs")
    BigQueryLoader(load_settings()).run(gcs_uri)


# ── DAG ──────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "jachson.denizjr",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="diesel_emissions_pipeline",
    default_args=default_args,
    description="End-to-end diesel engine emissions data pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["diesel", "emissions", "gcp", "dbt", "bigquery"],
) as dag:

    # ── Task 1 — Gerar dados WHSC simulados ──────────────────────────────────
    generate_data = PythonOperator(
        task_id="generate_dyno_data",
        python_callable=_generate_data,
        doc_md="Gera `dyno_emissions_data.csv` com 20 ciclos × 13 modos WHSC.",
    )

    # ── Task 2 — Upload para GCS ─────────────────────────────────────────────
    upload_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_gcs,
        doc_md="Upload do CSV para o GCS com particionamento por data. URI salvo no XCom.",
    )

    # ── Task 3 — Carregar no BigQuery ────────────────────────────────────────
    load_bigquery = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=_load_bigquery,
        doc_md="Carrega o CSV do GCS para a tabela Bronze. URI lido do XCom da task anterior.",
    )

    # ── Task 4 — dbt run ─────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/pipeline/diesel_emissions "
            "&& dbt run --profiles-dir /root/.dbt"
        ),
        doc_md="Executa stg_dyno_measurements (Silver), emissions_compliance e engine_performance (Gold).",
    )

    # ── Task 5 — dbt test ────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/pipeline/diesel_emissions "
            "&& dbt test --profiles-dir /root/.dbt"
        ),
        doc_md="Executa os testes de qualidade dbt. Pipeline falha se qualquer teste falhar.",
    )

    # ── Dependências ─────────────────────────────────────────────────────────
    generate_data >> upload_gcs >> load_bigquery >> dbt_run >> dbt_test
