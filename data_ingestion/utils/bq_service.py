from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class BigQueryService:

    def __init__(self, bq_client: bigquery.Client, project_id: str, target_dataset: str):
        self.bq_client      = bq_client
        self.project_id     = project_id
        self.target_dataset = target_dataset

        self.dataset = f"{self.project_id}.{self.target_dataset}"

        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self):
        try:
            self.bq_client.get_dataset(self.dataset)
            return
        except NotFound:
            pass
        dataset = bigquery.Dataset(self.dataset)
        self.bq_client.create_dataset(dataset, exists_ok=True)

    def is_table_exists(self, table: str) -> bool:
        try:
            self.bq_client.get_table(table)
            return True
        except NotFound:
            return False


    def load_file(self, source_uri: str, target_table: str) -> int:
        new_table = self.is_table_exists(target_table)
        job_config = bigquery.LoadJobConfig(
            source_format         = bigquery.SourceFormat.CSV,
            skip_leading_rows     = 1,
            autodetect            = True,
            write_disposition     = bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition    = bigquery.CreateDisposition.CREATE_IF_NEEDED,
            schema_update_options = [
                 bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        job = self.bq_client.load_table_from_uri(source_uri, target_table, job_config=job_config,)
        job.result()

        if new_table:
            # add __ingested_at__ column with default value
            query = f"""
            ALTER TABLE `{target_table}`
            ADD COLUMN IF NOT EXISTS __ingested_at__ TIMESTAMP;
            """
            self.bq_client.query(query).result()
            query = f"""
            ALTER TABLE `{target_table}`
            ALTER COLUMN __ingested_at__ SET DEFAULT CURRENT_TIMESTAMP();
            """
            self.bq_client.query(query).result()
            query = f"""
            UPDATE `{target_table}`
            SET __ingested_at__ = CURRENT_TIMESTAMP()
            WHERE __ingested_at__ IS NULL;
            """
            self.bq_client.query(query).result()

        return job.output_rows
