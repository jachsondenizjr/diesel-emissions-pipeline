from __future__ import annotations

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from pipeline.settings import Settings

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


def _credentials(settings: Settings) -> service_account.Credentials:
    settings.validate()
    return service_account.Credentials.from_service_account_file(
        settings.gcp.credentials_path,
        scopes=_SCOPES,
    )


def gcs_client(settings: Settings) -> storage.Client:
    return storage.Client(
        project=settings.gcp.project_id,
        credentials=_credentials(settings),
    )


def bq_client(settings: Settings) -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp.project_id,
        credentials=_credentials(settings),
    )
