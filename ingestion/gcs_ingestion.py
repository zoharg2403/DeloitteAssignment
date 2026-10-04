
import traceback
from google.cloud import storage, bigquery
from google.cloud.exceptions import BadRequest, NotFound

from common.config import Config
from common.logger import Logger
from ingestion.utils.audit_service import AuditService, AuditStatus, Metadata
from ingestion.utils.gcs_service import GCSService
from ingestion.utils.bq_service import BigQueryService


class GCSIngestion:

    def __init__(self):
        """Initialize GCS, BigQuery, and audit services from project configuration."""
        self.cfg = Config().load("config/ingestion.yaml")
        self.logger = Logger()

        self.project_id = self.cfg.env.project_id
        self.ingestion_bucket = self.cfg.env.buckets.data_storage.lstrip("gs://").strip("/")

        bq_client = bigquery.Client(project=self.project_id)
        self.bq_service = BigQueryService(
            bq_client      = bq_client,
            project_id     = self.project_id,
            target_dataset = self.cfg.ingestion.bigquery.target_dataset,
            )
        self.audit_service = AuditService(
            bq_client     = bq_client, 
            project_id    = self.project_id,
            audit_dataset = self.cfg.ingestion.bigquery.audit_dataset,
            audit_table   = self.cfg.ingestion.bigquery.audit_table,
            )
        self.gcs_service = GCSService(
            gcs_client  = storage.Client(project=self.project_id),
            bucket_name = self.ingestion_bucket, 
            **self.cfg.ingestion.gcs.blobs
            )

    def get_blob_metadata(self, blob: storage.Blob) -> Metadata:
        file_path = f"gs://{self.ingestion_bucket}/{blob.name}"
        file_name = blob.name.rsplit("/", 1)[-1]
        file_md5 = blob.md5_hash
        target_dataset = self.cfg.ingestion.bigquery.target_dataset
        target_table = file_name.split("_")[0]
        return Metadata(
            file_path      = file_path,
            file_name      = file_name,
            file_md5       = file_md5,
            target_dataset = target_dataset,
            target_table   = target_table,
            )

    def _process_file(self, blob: storage.Blob) -> None:
        blob_md = self.get_blob_metadata(blob)

        if self.audit_service.is_processed(blob_md):
            self.audit_service.insert(
                blob_metadata = blob_md,
                rows_loaded   = 0, 
                status        = AuditStatus.SKIP,
                error_message = "file_md5 match a file that was already loaded to target table" 
            )
            self.gcs_service.archive_skipped(blob)
            return

        source_uri = f"gs://{blob.bucket.name}/{blob.name}"
        target_table = f"{self.project_id}.{blob_md.target_dataset}.{blob_md.target_table}"
        try:
            rows_loaded = self.bq_service.load_file(
                source_uri = source_uri,
                target_table = target_table, 
            )
            self.audit_service.insert(
                blob_metadata = blob_md,
                rows_loaded   = rows_loaded, 
                status        = AuditStatus.SUCCESS, 
            )
            self.gcs_service.archive_processed(blob)
        except Exception as e:
            self.audit_service.insert(
                blob_metadata = blob_md,
                rows_loaded = 0, 
                status = AuditStatus.FAILED, 
                error_message=str(e)
            )
            self.gcs_service.archive_failed(blob)
            raise e

    def run(self):
        """Load incoming files and raise an error if any file fails processing."""
        self.logger.info(f"Starting data ingestion (source=gs://{self.ingestion_bucket}/{self.gcs_service.incoming_blob})")
        files = self.gcs_service.list_incoming_files()
        nfiles = len(files)
        self.logger.info(f"{nfiles} files found in source bucket")

        failed: list[tuple[str, Exception, str]] = []
        for i, blob in enumerate(files, start=1):
            try:
                self._process_file(blob)
            except (BadRequest, NotFound):
                raise
            except Exception as error:
                failed.append((blob.name, error, traceback.format_exc()))
            
            if i == 1 or i % 3 == 0 or i == nfiles:
                self.logger.info(f"Load file {i} / {nfiles}")

        if failed:
            self.logger.error(
                f"File load ended with {len(failed)} failed file(s):" + "\n" + 
                "\n".join(
                    [
                        f"FILE: {filename} failed with: {type(error).__name__}: {error}\n{tb}"
                        for filename, error, tb in failed
                    ]
                )
            )
            raise RuntimeError(f"Failed to ingest {len(failed)} of {nfiles} file(s)")
        else:
            self.logger.info("All files loaded successfully!")


if __name__ == "__main__":
    GCSIngestion().run()
    