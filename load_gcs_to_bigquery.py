import sys

from pipeline.bq_loader import BigQueryLoader
from pipeline.settings import load_settings

if __name__ == "__main__":
    settings = load_settings()
    gcs_uri = sys.argv[1] if len(sys.argv) > 1 else None
    BigQueryLoader(settings).run(gcs_uri)
