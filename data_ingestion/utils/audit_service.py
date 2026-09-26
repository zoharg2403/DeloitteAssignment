from enum import Enum
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class AuditStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIP = "SKIP"


class AuditService:
    
    def __init__(self, bq_client: bigquery.Client, project_id: str, audit_dataset:str, audit_table: str):
        self.bq_client     = bq_client
        self.project_id    = project_id
        self.audit_dataset = audit_dataset
        self.audit_table   = audit_table

        self.dataset = f"{self.project_id}.{self.audit_dataset}"
        self.table   = f"{self.dataset}.{self.audit_table}"

        self._ensure_dataset_exists()
        self._ensure_table_exists()

    def _ensure_dataset_exists(self):
        try:
            self.bq_client.get_dataset(self.dataset)
            return
        except NotFound:
            pass
        dataset = bigquery.Dataset(self.dataset)
        self.bq_client.create_dataset(dataset, exists_ok=True)

    def _ensure_table_exists(self) -> None:
        try:
            self.bq_client.get_table(self.table)
            return
        except NotFound:
            pass

        schema = [
            bigquery.SchemaField("file_path", "STRING"),
            bigquery.SchemaField("file_name", "STRING"),
            bigquery.SchemaField("file_md5", "STRING"),
            bigquery.SchemaField("target_table", "STRING"),
            bigquery.SchemaField("rows_loaded", "INT64"),
            bigquery.SchemaField("processed_ts", "TIMESTAMP"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("error_message", "STRING"),
            ]
        
        table = bigquery.Table(self.table, schema=schema)
        self.bq_client.create_table(table)      

    def is_processed(self, file_path: str, file_md5: str | None = None) -> bool:
        if file_md5:
            query = f"""
            SELECT COUNT(*)
            FROM `{self.table}`
            WHERE file_md5 = @file_md5
            AND status = 'SUCCESS'
            """
            params = [bigquery.ScalarQueryParameter("file_md5", "STRING", file_md5,)]
        else:
            query = f"""
            SELECT COUNT(*)
            FROM `{self.table}`
            WHERE file_path = @file_path
            AND status = 'SUCCESS'
            """
            params = [bigquery.ScalarQueryParameter("file_path", "STRING", file_path)]

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        result = self.bq_client.query(query, job_config=job_config).result()
        return list(result)[0][0] > 0 

    def insert(self, file_path: str, file_name: str, target_table: str, rows_loaded: int, status: AuditStatus, file_md5: str | None = None, error_message: str | None = None):
        query = f"""
        INSERT INTO `{self.table}`
        (file_path, file_name, file_md5, target_table, rows_loaded, processed_ts, status, error_message)
        VALUES
        (@file_path, @file_name, @file_md5, @target_table, @rows_loaded, CURRENT_TIMESTAMP(), @status, @error_message)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_path", "STRING", file_path),
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                bigquery.ScalarQueryParameter("file_md5", "STRING", file_md5),
                bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
                bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
                bigquery.ScalarQueryParameter("status", "STRING", status.value),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            ]
        )
        self.bq_client.query(query, job_config=job_config).result()
        