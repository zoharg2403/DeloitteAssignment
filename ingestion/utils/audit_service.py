from enum import Enum
from dataclasses import dataclass, fields
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


@dataclass
class Metadata:
    file_path:      str
    file_name:      str
    file_md5:       str
    target_dataset: str
    target_table:   str


class BQTypeMap(str, Enum):
    str   = "STRING"
    int   = "INTEGER"
    int64 = "INT64"
    float = "FLOAT"
    bool  = "BOOLEAN"


class AuditStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIP = "SKIP"


class AuditService:
    
    def __init__(self, bq_client: bigquery.Client, project_id: str, audit_dataset:str, audit_table: str):
        self.bq_client      = bq_client
        self.project_id     = project_id

        self._audit_dataset = audit_dataset
        self.audit_dataset  = f"{self.project_id}.{self._audit_dataset}"
        self._audit_table   = audit_table
        self.audit_table    = f"{self.audit_dataset}.{self._audit_table}"

        self._ensure_dataset_exists()
        self._ensure_table_exists()

    def _ensure_dataset_exists(self):
        try:
            self.bq_client.get_dataset(self.audit_dataset)
            return
        except NotFound:
            pass
        dataset = bigquery.Dataset(self.audit_dataset)
        self.bq_client.create_dataset(dataset, exists_ok=True)

    def _ensure_table_exists(self) -> None:
        try:
            self.bq_client.get_table(self.audit_table)
            return
        except NotFound:
            pass

        schema = [
            *[
                bigquery.SchemaField(f.name, BQTypeMap[f.type.__name__].value) 
                for f in fields(Metadata)
            ],
            bigquery.SchemaField("rows_loaded", "INT64"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("error_message", "STRING"),
            ]
        
        table = bigquery.Table(self.audit_table, schema=schema)
        table.clustering_fields = ["target_dataset", "target_table", "file_md5"]
        # table.require_partition_filter = True
        table.time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY, 
            field = "ingested_at", 
            # expiration_ms = 365 * 24 * 60 * 60 * 1000 # 365 days [ms]
            )

        self.bq_client.create_table(table)  

    def is_processed(self, blob_metadata: Metadata) -> bool:
        query = f"""
        SELECT 1
        FROM `{self.audit_table}`
        WHERE file_md5 = @file_md5 
            AND target_dataset = @target_dataset
            AND target_table = @target_table
            AND status = @success
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_md5", "STRING", blob_metadata.file_md5),
                bigquery.ScalarQueryParameter("target_dataset", "STRING", blob_metadata.target_dataset),
                bigquery.ScalarQueryParameter("target_table", "STRING", blob_metadata.target_table),
                bigquery.ScalarQueryParameter("success", "STRING", AuditStatus.SUCCESS),
                ]
            )
        result = self.bq_client.query(query, job_config=job_config).result()
        return next(result, None) is not None

    def insert(self, blob_metadata: Metadata, rows_loaded: int, status: AuditStatus, error_message: str | None = None):
        query = f"""
        INSERT INTO `{self.audit_table}`
        (file_path, file_name, file_md5, target_dataset, target_table, rows_loaded, status, error_message)
        VALUES
        (@file_path, @file_name, @file_md5, @target_dataset, @target_table, @rows_loaded, @status, @error_message)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                *[
                    bigquery.ScalarQueryParameter(f.name, BQTypeMap[f.type.__name__].value, getattr(blob_metadata, f.name)) 
                    for f in fields(blob_metadata)
                    ],
                bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
                bigquery.ScalarQueryParameter("status", "STRING", status.value),
                bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            ]
        )
        self.bq_client.query(query, job_config=job_config).result()
        