
import os
from datetime import datetime as dt, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

from common.config import Config
from airflow.dag_config import ConfigDag


class PipelineRunner:
    def __init__(self):
        self.cfg = Config().load("config/airflow/pipelines.yaml")

    def run(self, pipeline_name: str):
        cfg_pipeline = getattr(self.cfg.pipelines, pipeline_name)
        cfg_dag = ConfigDag(**cfg_pipeline.dag)
        cfg_tasks = cfg_pipeline.tasks

        with DAG(**cfg_dag) as dag:
            pass



if __name__ == "__main__":
    pipeline_name = "users_per_city" # must match the "dataproc/config.yaml/jobs:{job_name}.py"
    runner = PipelineRunner()
    runner.run(pipeline_name)