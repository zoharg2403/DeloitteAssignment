from __future__ import annotations

from dataclasses import dataclass
from pyspark.sql import SparkSession

from common.config import Config, _Root
from common.logger import Logger
from dataproc.utils.spark_session import SparkSessionBuilder
from dataproc.utils.big_query import BigQueryIO


@dataclass
class JobContext:
    """
    Shared runtime resources and configuration for Dataproc job.
    Create instances via JobContext.create(...).
    """

    job_name:      str
    cfg:           Config
    logger:        Logger
    spark_session: SparkSession
    bigquery:      BigQueryIO

    @property
    def cfg_job(self) -> _Root:
        return getattr(self.cfg.jobs, self.job_name)

    @property
    def source_table(self) -> str:
        return f"{self.cfg.env.project_id}.{self.cfg_job.source.dataset}.{self.cfg_job.source.table}"

    @property
    def target_table(self) -> str:
        return f"{self.cfg.env.project_id}.{self.cfg_job.target.dataset}.{self.cfg_job.target.table}"

    @classmethod
    def create(cls, job_name: str) -> JobContext:
        cfg    = Config().load("config/dataproc/runtime.yaml", "config/dataproc/jobs.yaml")
        logger = Logger()
        spark  = SparkSessionBuilder.build(
            app_name = f"{cfg.dataproc.spark_session.app_name} - {job_name}",
            views_enabled = cfg.dataproc.spark_session.views_enabled,
            materialization_dataset = f"{cfg.env.project_id}.{cfg.dataproc.spark_session.materialization_dataset}"
        )
        bigquery = BigQueryIO(
            project_id    = cfg.env.project_id, 
            temp_bucket   = cfg.env.buckets.dataproc_temp,
            spark_session = spark
        )
        return cls(
            job_name      = job_name,
            cfg           = cfg, 
            logger        = logger,
            spark_session = spark,
            bigquery      = bigquery
            )

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self) -> None:
        self.logger.info("Stopping Spark session")
        self.spark_session.stop()


# if __name__ == "__main__":
#     with JobContext.create("users_per_city") as ctx:
#         print(ctx.source_table)
