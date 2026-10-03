
from airflow import DAG

from airflow_pipelines.utils.pipeline_context import PipelineContext

ctx = PipelineContext.create("users_per_city")

with DAG(**ctx.cfg_dag) as dag:
    ctx.create_tasks()


