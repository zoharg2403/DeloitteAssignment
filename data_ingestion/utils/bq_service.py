from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class BigQueryService:

    def __init__(self, bq_client: bigquery.Client, project_id: str, target_dataset: str):
        self.bq_client      = bq_client
        self.project_id     = project_id
        self.target_dataset = target_dataset

        self.dataset = f"{self.project_id}.{self.target_dataset}"

        self.source_format      = bigquery.SourceFormat.CSV
        self.skip_leading_rows  = 1
        self.autodetect         = True
        self.write_disposition  = bigquery.WriteDisposition.WRITE_APPEND
        self.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self):
        try:
            self.bq_client.get_dataset(self.dataset)
            return
        except NotFound:
            pass
        dataset = bigquery.Dataset(self.dataset)
        self.bq_client.create_dataset(dataset, exists_ok=True)

    def get_target_table(self, table_name: str) -> str:
        return f"{self.dataset}.{table_name}"

    def load_file(self, source_uri: str, target_table: str) -> int:
        job_config = bigquery.LoadJobConfig(
            source_format      = self.source_format,
            skip_leading_rows  = self.skip_leading_rows,
            autodetect         = self.autodetect,
            write_disposition  = self.write_disposition,
            create_disposition = self.create_disposition,
        )
        job = self.bq_client.load_table_from_uri(source_uri, target_table, job_config=job_config,)
        job.result()

        return job.output_rows
