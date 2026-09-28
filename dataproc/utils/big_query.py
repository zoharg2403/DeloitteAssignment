
from pyspark.sql import DataFrame, SparkSession

from common.logger import Logger


class BigQueryIO:
    
    def __init__(self, project_id: str, temp_bucket: str, spark_session: SparkSession | None = None):
        self.project_id    = project_id
        self.temp_bucket   = temp_bucket
        self.spark_session = spark_session

        self.logger = Logger()
        self.logger.debug(f"Initialized BigQuery client for project '{self.project_id}'")

    def _table_path(self, dataset, table):
        return f"{self.project_id}.{dataset}.{table}"

    def read(self, dataset, table):
        table_path = self._table_path(dataset, table)
        self.logger.info(f"Reading BigQuery table '{table_path}'")
        try:
            dataframe = (
                self.spark_session.read.format("bigquery")
                .option("table", table_path)
                .load()
            )
        except Exception:
            self.logger.exception(f"Failed to read BigQuery table '{table_path}'")
            raise
        self.logger.info(f"Finished reading BigQuery table '{table_path}'")
        return dataframe

    def write(self, df: DataFrame, dataset: str, table: str, mode="overwrite"):
        table_path = self._table_path(dataset, table)
        self.logger.info(f"Writing BigQuery table '{table_path}' with mode '{mode}'")
        try:
            (
                df.write.format("bigquery")
                .option("table", table_path)
                .option("temporaryGcsBucket", self.cfg.buckets.dataproc_temp)
                .mode(mode)
                .save()
            )
        except Exception:
            self.logger.exception(f"Failed to write BigQuery table '{table_path}'")
            raise
        self.logger.info(f"Finished writing BigQuery table '{table_path}'")

    def write_partitioned(self, df: DataFrame, dataset: str, table: str, partition_field: str, mode="overwrite"):
        table_path = self._table_path(dataset, table)
        self.logger.info(f"Writing partitioned BigQuery table '{table_path}' by '{partition_field}' with mode '{mode}'")

        try:
            (
                df.write.format("bigquery")
                .option("table", table_path)
                .option("temporaryGcsBucket", self.cfg.buckets.dataproc_temp)
                .option("partitionField", partition_field)
                .mode(mode)
                .save()
            )
        except Exception:
            self.logger.exception(f"Failed to write partitioned BigQuery table '{table_path}'")
            raise
        self.logger.info(f"Finished writing partitioned BigQuery table '{table_path}'")


if __name__ == "__main__":
    BigQueryIO()
