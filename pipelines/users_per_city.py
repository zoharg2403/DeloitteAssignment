
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


cfg = Config()
env_cfg = cfg.env
job_cfg = cfg.jobs.users_per_city
pipeline_cfg = cfg.pipelines
deploy_cfg = cfg.dataproc_deploy
release_uri = f"{env_cfg.scripts_bucket}/{os.getenv('RELEASE_VERSION', 'latest')}"

default_args = {
    "owner": pipeline_cfg.dag_args.owner,
    "start_date": dt.strptime(str(pipeline_cfg.dag_args.start_date), "%d-%m-%Y"),
    "retries": pipeline_cfg.dag_args.retries,
    "retry_delay": timedelta(seconds=pipeline_cfg.dag_args.retry_delay_sec),
}


with DAG(
    dag_id=pipeline_cfg.dag.dag_id,
    default_args=default_args,
    schedule_interval=pipeline_cfg.dag.schedule_interval,
    catchup=pipeline_cfg.dag.catchup,
    ) as dag:

    # Compile Dataform repository code
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=env_cfg.project_id,
        region=env_cfg.region,
        repository_id=pipeline_cfg.dataform.repository_id,
        compilation_result={
            "git_commitish": pipeline_cfg.dataform.git_commitish,
        },
    )

    # Run Dataform workflow
    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=env_cfg.project_id,
        region=env_cfg.region,
        repository_id=pipeline_cfg.dataform.repository_id,
        workflow_invocation={
            # Link compilation result from the compile step dynamically
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {
                "transitive_dependencies_included": True,
                "included_targets": [
                    {
                        "database": env_cfg.project_id,
                        "schema": "gold",
                        "name": "gold_users_address",
                    },
                ],
            },
        },
    )

    # Dataproc Serverless PySpark batch configuration
    pyspark_batch_config = {
        "pyspark_batch": {
            "main_python_file_uri": f"{release_uri}/{job_cfg.main_script}",
            **{
                key: [f"{release_uri}/{path}" for path in paths]
                for key, paths in deploy_cfg.batch_kwargs.pyspark.items()
            },
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
        project_id=env_cfg.project_id,
        region=env_cfg.region,
        batch_id="pyspark-agg-{{ ds_nodash }}".lower()[:63],
        batch=pyspark_batch_config,
    )

    # Define DAG execution dependencies
    compile_dataform >> run_dataform >> run_pyspark_aggregation
