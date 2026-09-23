
from pyspark.sql import SparkSession

from common.config import Config
from common.logger import Logger


class SparkSessionBuilder:

    cfg = Config().env
    logger = Logger()

    @classmethod
    def build(cls, job_name: str):
        """Creates or retrieves a Spark session with BigQuery pre-configured."""
        cls.logger.info(f"Build Spark session '{cls.cfg.spark_session.app_name}'")
        try:
            spark = (
                SparkSession.builder
                .appName(cls.cfg.app_name + f"-{job_name}")
                .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0")
                .config("viewsEnabled", cls.cfg.spark_session.views_enabled)
                .config("materializationDataset", f"{cls.cfg.project_id}.{cls.cfg.spark_session.materialization_dataset}")
                .getOrCreate()
            )
        except Exception as e:
            cls.logger.error(f"Failed to create Spark session '{cls.cfg.spark_session.app_name}' with error: {e}")
            raise Exception(f"Failed to create Spark session '{cls.cfg.spark_session.app_name}'") from e
        cls.logger.info(f"Spark session '{cls.cfg.spark_session.app_name}' is ready")
        return spark


if __name__ == "__main__":
    SparkSessionBuilder()
