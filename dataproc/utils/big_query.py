
from pyspark.sql import DataFrame, SparkSession

from common.config import Config
from common.logger import Logger
from dataproc.utils.spark_session import SparkSessionBuilder


class BigQueryIO:
    
    cfg = Config().env

    def __init__(self, spark: SparkSession | None = None):
        self.spark = spark or SparkSessionBuilder.build()
        self.logger = Logger()
        self.logger.debug(f"Initialized BigQuery client for project '{self.cfg.project_id}'")

    def _table_path(self, dataset, table):
        return f"{self.cfg.project_id}.{dataset}.{table}"

    def read(self, dataset, table):
        table_path = self._table_path(dataset, table)
        self.logger.info(f"Reading BigQuery table '{table_path}'")
        try:
            dataframe = (
                self.spark.read.format("bigquery")
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
                .option("temporaryGcsBucket", self.cfg.bigquery.temp_bucket)
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
                .option("temporaryGcsBucket", self.cfg.bigquery.temp_bucket)
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
