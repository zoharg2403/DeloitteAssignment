from google.cloud import storage, bigquery

from common.config import Config
from common.logger import Logger
from data_ingestion.utils.audit_service import AuditService, AuditStatus
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
            gcs_client    = storage.Client(project=self.cfg.project_id),
            bucket_name   = self.cfg.gcs.bucket_name.lstrip("gs://").strip("/"), 
            incoming_blob = self.cfg.gcs.incoming_blob.strip("/"), 
            archive_blob  = self.cfg.gcs.archive_blob.strip("/"), 
            )

    def _process_file(self, blob: storage.Blob) -> None:
        filepath = f"gs://{self.cfg.gcs.bucket_name.lstrip("gs://").strip("/")}/{blob.name}"
        filename = blob.name.rsplit("/", 1)[-1]
        file_md5 = blob.md5_hash
        table_name = filename.split("_")[0]
        target_table = self.bq_service.get_target_table(table_name)

        if self.audit_service.is_processed(file_path=filepath, file_md5=file_md5):
            self.audit_service.insert(
                file_path    = filepath, 
                file_name    = filename, 
                target_table = target_table, 
                rows_loaded  = 0, 
                status       = AuditStatus.SKIP, 
                file_md5     = file_md5
            )
            self.gcs_service.archive_file(blob)
            return

        source_uri = self.gcs_service.get_gcs_uri(blob)
        try:
            rows_loaded = self.bq_service.load_file(
                source_uri = source_uri,
                target_table = target_table, 
            )
            self.audit_service.insert(
                file_path = filepath, 
                file_name = filename, 
                target_table = target_table, 
                rows_loaded = rows_loaded, 
                status = AuditStatus.SUCCESS, 
                file_md5 = file_md5
            )
            self.gcs_service.archive_file(blob)
        except Exception as e:
            self.audit_service.insert(
                file_path = filepath, 
                file_name = filename, 
                target_table = target_table, 
                rows_loaded = 0, 
                status = AuditStatus.FAILED, 
                file_md5 = file_md5, 
                error_message=str(e)
            )
            raise e

    def run(self):
        self.logger.info(f"Starting data ingestion (source=gs://{self.cfg.gcs.bucket_name.lstrip("gs://").strip("/")}/{self.cfg.gcs.incoming_blob})")
        files = self.gcs_service.list_incoming_files()
        nfiles = len(files)
        self.logger.info(f"{nfiles} files found in source bucket")

        failed = []
        for i, blob in enumerate(files, start=1):
            try:
                self._process_file(blob)
            except Exception as e:
                failed.append((blob.name, e))

            if i == 1 or i % 3 == 0 or i == nfiles:
                self.logger.info(f"Load file {i} / {nfiles}")

        if failed:
            self.logger.info(f"File load ended with {len(failed)} failed file(s)")
            for f, e in failed:
                self.logger.warning(f"  - {f}: {e}")
        else:
            self.logger.info("All files loaded successfully!")


if __name__ == "__main__":
    GCSIngestion().run()
    