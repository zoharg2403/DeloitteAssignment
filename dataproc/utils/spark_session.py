
from pyspark.sql import SparkSession

from common.logger import Logger


class SparkSessionBuilder:

    logger = Logger()

    @classmethod
    def build(
        cls, 
        app_name: str, 
        views_enabled: bool | str, 
        materialization_dataset : str
        ):

        """Creates or retrieves a Spark session with BigQuery pre-configured."""
        cls.logger.info(f"Build Spark session '{app_name}'")
        try:
            spark = (
                SparkSession.builder
                .appName(app_name)
                .config("viewsEnabled", str(views_enabled).lower())
                .config("materializationDataset", materialization_dataset)
                .getOrCreate()
            )
        except Exception as e:
            cls.logger.error(f"Failed to create Spark session '{app_name}' with error: {e}")
            raise Exception(f"Failed to create Spark session '{app_name}'") from e
        cls.logger.info(f"Spark session '{app_name}' is ready")
        return spark


if __name__ == "__main__":
    SparkSessionBuilder()
