
from dataclasses import dataclass
from pyspark.sql import SparkSession

from common.config import Config
from common.logger import Logger
from dataproc.utils.spark_session import SparkSessionBuilder
from dataproc.utils.big_query import BigQueryIO


@dataclass
class JobContext:
    """Shared runtime resources and configuration for Dataproc job."""

    job_name: str
    cfg:      Config
    logger:   Logger
    spark:    SparkSession
    bigquery: BigQueryIO  

    @classmethod
    def create(cls, job_name: str) -> JobContext:
        cfg      = Config()
        logger   = Logger()
        spark    = SparkSessionBuilder.build(job_name)
        bigquery = BigQueryIO(spark)
        return cls(
            cfg      = cfg, 
            job_name = job_name,
            logger   = logger,
            spark    = spark,
            bigquery = bigquery
            )

    def __enter__(self) -> JobContext:
        return self

    def __exit__(self):
        self.close()

    def close(self) -> None:
        self.logger.info("Stopping Spark session")
        self.spark.stop()


if __name__ == "__main__":
    JobContext().create('users_per_city')
