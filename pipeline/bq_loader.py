from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from pipeline.auth import bq_client
from pipeline.settings import Settings

_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "dyno_measurements.json"


class BigQueryLoader:
    """Cria dataset/tabela no BigQuery e carrega o CSV vindo do GCS.

    Recebe o GCS URI retornado pelo GCSUploader. Se não for fornecido,
    reconstrói o URI a partir da data UTC atual (útil para execução standalone).
    """

    def __init__(self, settings: Settings) -> None:
        self._s = settings

    def run(self, gcs_uri: str | None = None) -> None:
        uri = gcs_uri or self._derive_gcs_uri()
        schema = self._load_schema()
        client = bq_client(self._s)

        self._ensure_dataset(client)
        self._ensure_table(client, schema)
        self._load_data(client, uri, schema)
        self._verify(client)

    # ── private ───────────────────────────────────────────────────────────────

    def _derive_gcs_uri(self) -> str:
        """Reconstrói o URI da partição do dia atual — fallback para execução standalone."""
        today = datetime.now(tz=timezone.utc)
        filename = Path(self._s.local_csv_path).name
        path = (
            f"{self._s.gcs.raw_prefix}/"
            f"year={today.year}/"
            f"month={today.month:02d}/"
            f"day={today.day:02d}/"
            f"{filename}"
        )
        return f"gs://{self._s.gcs.bucket}/{path}"

    def _load_schema(self) -> list[bigquery.SchemaField]:
        return bigquery.Client.schema_from_json(str(_SCHEMA_PATH))

    def _ensure_dataset(self, client: bigquery.Client) -> None:
        dataset_id = f"{self._s.gcp.project_id}.{self._s.bigquery.dataset_raw}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self._s.gcp.region
        dataset.description = "Camada Bronze — medições brutas do dinamômetro WHSC."
        try:
            client.create_dataset(dataset, timeout=30)
            print(f"  Dataset criado  : {dataset_id}")
        except Conflict:
            print(f"  Dataset existe  : {dataset_id}")

    def _ensure_table(self, client: bigquery.Client, schema: list[bigquery.SchemaField]) -> None:
        table = bigquery.Table(self._s.bq_full_table, schema=schema)
        table.description = (
            "Medições brutas WHSC. Uma linha por leitura de sensor. "
            "Flags de conformidade são calculados pela camada Silver (dbt)."
        )
        try:
            client.create_table(table, timeout=30)
            print(f"  Tabela criada   : {self._s.bq_full_table}")
        except Conflict:
            print(f"  Tabela existe   : {self._s.bq_full_table} — dados serão sobrescritos")

    def _load_data(
        self,
        client: bigquery.Client,
        gcs_uri: str,
        schema: list[bigquery.SchemaField],
    ) -> None:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            null_marker="",
        )
        print(f"  Fonte          : {gcs_uri}")
        print(f"  Destino        : {self._s.bq_full_table}")

        job = client.load_table_from_uri(gcs_uri, self._s.bq_full_table, job_config=job_config)
        job.result()

        if job.errors:
            errors = "\n".join(str(e) for e in job.errors)
            raise RuntimeError(f"Erros no job de carga:\n{errors}")

        print(f"  Job concluído  : {job.job_id}")

    def _verify(self, client: bigquery.Client) -> None:
        table = client.get_table(self._s.bq_full_table)
        print(f"  Linhas carregadas : {table.num_rows:,}")
        print(f"  Tamanho da tabela : {table.num_bytes / 1024:.1f} KB")
