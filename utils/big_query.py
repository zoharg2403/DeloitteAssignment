from pyspark.sql import DataFrame, SparkSession
from pydantic import BaseModel, Field

from utils.config import Config
from utils.logger import Logger


logger = Logger()


class ConfigBigQueryIO(BaseModel):
    project_id:      str = Field(..., description="BigQuery Project ID")
    temp_GCS_bucket: str = Field(..., description="Temporary GCS Bucket for BigQuery write path")


class BigQueryIO:

    def __init__(self, spark: SparkSession):
        self.cfg = ConfigBigQueryIO(**Config().big_query)
        self.spark = spark
        logger.debug(f"Initialized BigQuery client for project '{self.cfg.project_id}'")

    def _table_path(self, dataset, table):
        return f"{self.cfg.project_id}.{dataset}.{table}"

    def read(self, dataset, table):
        table_path = self._table_path(dataset, table)
        logger.info(f"Reading BigQuery table '{table_path}'")
        try:
            dataframe = (
                self.spark.read.format("bigquery")
                .option("table", table_path)
                .load()
            )
        except Exception:
            logger.exception(f"Failed to read BigQuery table '{table_path}'")
            raise
        logger.info(f"Finished reading BigQuery table '{table_path}'")
        return dataframe

    def write(self, df: DataFrame, dataset: str, table: str, mode="overwrite"):
        table_path = self._table_path(dataset, table)
        logger.info(f"Writing BigQuery table '{table_path}' with mode '{mode}'")
        try:
            (
                df.write.format("bigquery")
                .option("table", table_path)
                .option("temporaryGcsBucket", self.cfg.temp_GCS_bucket)
                .mode(mode)
                .save()
            )
        except Exception:
            logger.exception(f"Failed to write BigQuery table '{table_path}'")
            raise
        logger.info(f"Finished writing BigQuery table '{table_path}'")

    def write_partitioned(self, df: DataFrame, dataset: str, table: str, partition_field: str, mode="overwrite"):
        table_path = self._table_path(dataset, table)
        logger.info(f"Writing partitioned BigQuery table '{table_path}' by '{partition_field}' with mode '{mode}'")

        try:
            (
                df.write.format("bigquery")
                .option("table", table_path)
                .option("temporaryGcsBucket", self.cfg.temp_GCS_bucket)
                .option("partitionField", partition_field)
                .mode(mode)
                .save()
            )
        except Exception:
            logger.exception(f"Failed to write partitioned BigQuery table '{table_path}'")
            raise
        logger.info(f"Finished writing partitioned BigQuery table '{table_path}'")


if __name__ == "__main__":
    BigQueryIO()
