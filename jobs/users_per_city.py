
from pyspark.sql import functions as F

from utils.config import Config
from utils.config_models import ConfigJob
from utils.big_query import BigQueryIO
from utils.logger import Logger
from utils.spark_session import SparkSessionBuilder


logger = Logger()


def main():
    logger.info("Starting users_per_city job")
    spark = None
    try:
        cfg = ConfigJob(**Config().jobs.users_per_city)
        logger.debug(f"Job configured to read '{cfg.source.dataset}.{cfg.source.table}' and write '{cfg.target.dataset}.{cfg.target.table}'")
        spark = SparkSessionBuilder.build()
        bq = BigQueryIO(spark)

        loc_cols = ["country", "region", "city"]
        users_per_city = (
            bq.read(cfg.source.dataset, cfg.source.table)
            .dropna(subset=loc_cols)
            .groupBy(*loc_cols)
            .agg(F.countDistinct("user_id").alias("user_count"))
        )

        bq.write(
            users_per_city,
            cfg.target.dataset,
            cfg.target.table,
            mode=cfg.write_mode,
        )
        logger.info("users_per_city job completed successfully")
    except Exception:
        logger.exception("users_per_city job failed")
        raise
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()


if __name__ == "__main__":
    main()
