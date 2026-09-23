
from pyspark.sql import SparkSession

from common.config import Config
from common.logger import Logger


class SparkSessionBuilder:

    cfg = Config()
    cfg_ss = cfg.env.spark_session
    logger = Logger()

    @classmethod
    def build(cls, job_name: str):
        """Creates or retrieves a Spark session with BigQuery pre-configured."""
        app_name = cls.cfg_ss.app_name
        cls.logger.info(f"Build Spark session '{app_name}'")
        try:
            spark = (
                SparkSession.builder
                .appName(cls.cfg_ss.app_name + f"-{job_name}")
                .config("viewsEnabled", str(cls.cfg_ss.views_enabled).lower())
                .config("materializationDataset", f"{cls.cfg.env.project_id}.{cls.cfg_ss.materialization_dataset}")
                .getOrCreate()
            )
        except Exception as e:
            cls.logger.error(f"Failed to create Spark session '{app_name}' with error: {e}")
            raise Exception(f"Failed to create Spark session '{app_name}'") from e
        cls.logger.info(f"Spark session '{app_name}' is ready")
        return spark


if __name__ == "__main__":
    SparkSessionBuilder()
