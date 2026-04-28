"""
GCS Ingestion Script (v2 — Service Account auth)
=================================================
Uploads the generated dynamometer CSV to GCP Cloud Storage.
Authenticates using a Service Account JSON key file.

Requirements:
  pip install google-cloud-storage

Author: [Your Name]
Project: diesel-emissions-pipeline
"""

import sys
from datetime import datetime
from pathlib import Path

try:
    from config import (
        GCP_PROJECT_ID, GCS_BUCKET_NAME, GCS_RAW_PREFIX,
        LOCAL_CSV_PATH, CREDENTIALS_PATH, validate
    )
except ImportError:
    print("ERROR: config.py not found. Make sure it's in the same folder.")
    sys.exit(1)

try:
    from google.cloud import storage
    from google.oauth2 import service_account
except ImportError:
    print("ERROR: Run:  pip install google-cloud-storage")
    sys.exit(1)


def get_client() -> storage.Client:
    """Create authenticated GCS client using Service Account JSON."""
    creds_path = validate()
    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return storage.Client(project=GCP_PROJECT_ID, credentials=credentials)


def build_gcs_destination(filename: str, reference_date: datetime = None) -> str:
    """Build partitioned GCS path: raw/dyno_emissions/year=.../month=.../day=..."""
    if reference_date is None:
        reference_date = datetime.utcnow()
    return (
        f"{GCS_RAW_PREFIX}/"
        f"year={reference_date.year}/"
        f"month={str(reference_date.month).zfill(2)}/"
        f"day={str(reference_date.day).zfill(2)}/"
        f"{filename}"
    )


def upload(client: storage.Client) -> dict:
    local_file = Path(LOCAL_CSV_PATH)
    if not local_file.exists():
        raise FileNotFoundError(
            f"CSV not found: {LOCAL_CSV_PATH}\n"
            f"Run generate_dyno_data.py first."
        )

    file_size_mb = local_file.stat().st_size / (1024 * 1024)
    destination  = build_gcs_destination(local_file.name)

    bucket = client.bucket(GCS_BUCKET_NAME)
    blob   = bucket.blob(destination)

    print(f"  File        : {LOCAL_CSV_PATH} ({file_size_mb:.2f} MB)")
    print(f"  Destination : gs://{GCS_BUCKET_NAME}/{destination}")

    start = datetime.utcnow()
    blob.upload_from_filename(LOCAL_CSV_PATH, content_type="text/csv")
    elapsed = (datetime.utcnow() - start).total_seconds()

    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{destination}"
    print(f"  Uploaded in : {elapsed:.1f}s")
    print(f"  GCS URI     : {gcs_uri}")
    return {"gcs_uri": gcs_uri, "elapsed": elapsed}


def main():
    print("=" * 55)
    print("  Diesel Emissions Pipeline — Upload to GCS")
    print("=" * 55)

    client = get_client()
    print(f"Authenticated: Service Account OK")
    print(f"Project: {GCP_PROJECT_ID}\n")

    upload(client)
    print("\nDone! Next step: run load_gcs_to_bigquery.py")


if __name__ == "__main__":
    main()
