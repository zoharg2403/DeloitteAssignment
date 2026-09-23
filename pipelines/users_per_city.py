from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


from common.config import Config


cfg = Config()


with DAG(
    dag_id=cfg.dag.dag_id,
    default_args={**cfg.dag_args},
    schedule_interval=cfg.dag.schedule_interval,
    catchup=cfg.dag.catchup,
    ) as dag:

    # Compile Dataform repository code
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=cfg.env.project_id,
        region=cfg.env.region,
        repository_id=cfg.dataform.repository_id,
        compilation_result={
            "git_commitish": cfg.dataform.git_commitish,
        },
    )

    # Run Dataform workflow
    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=cfg.env.project_id,
        region=cfg.env.region,
        repository_id=cfg.dataform.repository_id,
        workflow_invocation={
            # Link compilation result from the compile step dynamically
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}"
        },
    )

    # Dataproc Serverless PySpark batch configuration
    pyspark_batch_config = {
        "pyspark_batch": {
            "main_python_file_uri": cfg.job.main_script,
            **deploy_cfg.batch_kwargs.pyspark
        },
        "runtime_config": {
            "properties": {
                **deploy_cfg.batch_kwargs.runtime_config_properties
            }
        }
    }

    # Run PySpark Batch on Dataproc Serverless
    run_pyspark_aggregation = DataprocCreateBatchOperator(
        task_id="run_pyspark_aggregation",
        project_id=cfg.env.project_id,
        region=cfg.env.region,
        batch_id="pyspark-agg-{{ ds_nodash }}-{{ mcols_id }}".lower()[:63], # Generate a unique ID < 64 chars
        batch=pyspark_batch_config,
    )

    # Define DAG execution dependencies
    compile_dataform >> run_dataform >> run_pyspark_aggregation
