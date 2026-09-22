from pyspark.sql import SparkSession
from pydantic import BaseModel, Field

from dataproc.utils.config import Config
from dataproc.utils.logger import Logger


logger = Logger()

class ConfigSparkSessionBuilder(BaseModel):
    project_id:   str = Field(..., description="Project ID")
    app_name:     str = Field(..., description="Spark Application Name")
    temp_dataset: str = Field(..., description="Temporary Dataset for BigQuery Materialization")


class SparkSessionBuilder:

    cfg = ConfigSparkSessionBuilder(**Config().spark_session)

    @classmethod
    def build(cls):
        """Creates or retrieves a Spark session with BigQuery pre-configured."""
        logger.info(f"Starting Spark session '{cls.cfg.app_name}'")
        try:
            spark = (
                SparkSession.builder
                .appName(cls.cfg.app_name)
                .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0")
                .config("viewsEnabled", "true")
                .config("materializationDataset", f"{cls.cfg.project_id}.{cls.cfg.temp_dataset}")
                .getOrCreate()
            )
        except Exception:
            logger.exception(f"Failed to create Spark session '{cls.cfg.app_name}'")
            raise
        logger.info(f"Spark session '{cls.cfg.app_name}' is ready")
        return spark

