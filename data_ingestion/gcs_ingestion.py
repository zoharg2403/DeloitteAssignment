
import traceback
from google.cloud import storage, bigquery
from google.cloud.exceptions import BadRequest, NotFound

from common.config import Config
from common.logger import Logger
from data_ingestion.utils.audit_service import AuditService, AuditStatus, Metadata
from data_ingestion.utils.gcs_service import GCSService
from data_ingestion.utils.bq_service import BigQueryService


class GCSIngestion:

    def __init__(self):
        self.cfg = Config.load("data_ingestion/gcs_ingestion.yaml")
        self.logger = Logger()

        bq_client = bigquery.Client(project=self.cfg.project_id)
        self.bq_service = BigQueryService(
            bq_client      = bq_client,
            project_id     = self.cfg.project_id,
            target_dataset = self.cfg.bigquery.target_dataset,
            )
        self.audit_service = AuditService(
            bq_client     = bq_client, 
            project_id    = self.cfg.project_id,
            audit_dataset = self.cfg.bigquery.audit_dataset,
            audit_table   = self.cfg.bigquery.audit_table,
            )
        self.gcs_service = GCSService(
            gcs_client  = storage.Client(project=self.cfg.project_id),
            bucket_name = self.cfg.gcs.bucket_name.lstrip("gs://").strip("/"), 
            **self.cfg.gcs.blobs  
            )

    def get_blob_metadata(self, blob: storage.Blob) -> Metadata:
        file_path = f"gs://{self.cfg.gcs.bucket_name.lstrip("gs://").strip("/")}/{blob.name}"
        file_name = blob.name.rsplit("/", 1)[-1]
        file_md5 = blob.md5_hash
        target_dataset = self.cfg.bigquery.target_dataset
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
        target_table = f"{self.cfg.project_id}.{blob_md.target_dataset}.{blob_md.target_table}"
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
        self.logger.info(f"Starting data ingestion (source=gs://{self.cfg.gcs.bucket_name.lstrip("gs://").strip("/")}/{self.cfg.gcs.blobs.incoming_blob})")
        files = self.gcs_service.list_incoming_files()
        nfiles = len(files)
        self.logger.info(f"{nfiles} files found in source bucket")

        failed = []
        for i, blob in enumerate(files, start=1):
            try:
                self._process_file(blob)
            except (BadRequest, NotFound) as e:
                raise 
            except Exception as e:
                failed.append((blob.name, e.__class__.__name__, str(e), traceback.format_exc()))
            
            if i == 1 or i % 3 == 0 or i == nfiles:
                self.logger.info(f"Load file {i} / {nfiles}")

        if failed:
            self.logger.error(
                f"File load ended with {len(failed)} failed file(s):" + "\n" + 
                "\n".join(
                    [
                        f"FILE: {filename} failed with: {exc_type}: {exc_msg}\n{tb}"
                        for filename, exc_type, exc_msg, tb in failed
                    ]
                )
            )
        else:
            self.logger.info("All files loaded successfully!")


if __name__ == "__main__":
    GCSIngestion().run()
    