from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pipeline.auth import gcs_client
from pipeline.settings import Settings


class GCSUploader:
    """Faz upload do CSV gerado para o GCS com particionamento por data.

    Retorna o GCS URI real (particionado), que deve ser passado ao BigQueryLoader.
    O URI retornado tem o formato:
        gs://<bucket>/raw/dyno_emissions/year=YYYY/month=MM/day=DD/<filename>
    """

    def __init__(self, settings: Settings) -> None:
        self._s = settings

    def run(self, csv_path: Path | None = None) -> str:
        """Faz upload do CSV e retorna o GCS URI da partição de destino."""
        local_file = Path(csv_path or self._s.local_csv_path)

        if not local_file.exists():
            raise FileNotFoundError(
                f"CSV não encontrado: {local_file}\n"
                "Execute generate_dyno_data.py primeiro."
            )

        client = gcs_client(self._s)
        reference_date = datetime.now(tz=timezone.utc)
        destination = self._build_destination(local_file.name, reference_date)
        gcs_uri = self._upload(client, local_file, destination)

        print(f"  Arquivo      : {local_file} ({local_file.stat().st_size / 1024:.1f} KB)")
        print(f"  Destino      : {gcs_uri}")
        return gcs_uri

    # ── private ───────────────────────────────────────────────────────────────

    def _build_destination(self, filename: str, reference_date: datetime) -> str:
        return (
            f"{self._s.gcs.raw_prefix}/"
            f"year={reference_date.year}/"
            f"month={reference_date.month:02d}/"
            f"day={reference_date.day:02d}/"
            f"{filename}"
        )

    def _upload(self, client, local_file: Path, destination: str) -> str:
        bucket = client.bucket(self._s.gcs.bucket)
        blob = bucket.blob(destination)
        blob.upload_from_filename(str(local_file), content_type="text/csv")
        return f"gs://{self._s.gcs.bucket}/{destination}"
