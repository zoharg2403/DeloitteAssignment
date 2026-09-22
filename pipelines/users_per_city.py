from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

# Define configurations (Use placeholders for your real environment variables)
PROJECT_ID = "<YOUR_PROJECT_ID>"
REGION = "<YOUR_GCP_REGION>"  # e.g., "us-central1"
DATAFORM_REPOSITORY = "<YOUR_DATAFORM_REPO_ID>"
GIT_COMMITISH = "main"       # Branch, tag, or SHA to compile

BUCKET_NAME = "<YOUR_BUCKET_NAME>"
PYSPARK_SCRIPT_URI = f"gs://{BUCKET_NAME}/scripts/aggregate_job.py"
PHS_CLUSTER = "<YOUR_PERSISTENT_HISTORY_SERVER_CLUSTER>" # Optional, for Spark UI history
SUBNET_URI = "projects/<YOUR_PROJECT_ID>/regions/<YOUR_GCP_REGION>/subnetworks/<YOUR_SUBNET_NAME>"

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dataform_to_dataproc_serverless_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Set your cron schedule here, e.g., '0 6 * * *' (daily at 6 AM)
    catchup=False,
    tags=["dataform", "dataproc", "serverless"],
) as dag:

    # 1. Compile Dataform repository code
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
        },
    )

    # 2. Invoke (run) Dataform workflow
    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPOSITORY,
        workflow_invocation={
            # Link compilation result from the compile step dynamically
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}"
        },
    )

    # 3. Dataproc Serverless PySpark batch configuration
    pyspark_batch_config = {
        "pyspark_batch": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                # Pass any runtime parameters to your python script here
                "--input_table", f"{PROJECT_ID}.<DATASET>.<DATAFORM_OUTPUT_TABLE>",
                "--output_table", f"{PROJECT_ID}.<DATASET>.<DATAPROC_AGGREGATED_TABLE>"
            ]
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": SUBNET_URI,
            }
        },
        # Premium feature: Optional Lightning Engine configuration
        "runtime_config": {
            "properties": {
                "spark.dataproc.lightning.enabled": "true",
                "spark.dataproc.lightning.native.enabled": "true",
                "spark.dataproc.autotuning.enabled": "true",
            }
        }
    }

    # 4. Run PySpark Batch on Dataproc Serverless
    run_pyspark_aggregation = DataprocCreateBatchOperator(
        task_id="run_pyspark_aggregation",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="pyspark-agg-{{ ds_nodash }}-{{ mcols_id }}".lower()[:63], # Generate a unique ID < 64 chars
        batch=pyspark_batch_config,
    )

    # Define DAG execution dependencies
    compile_dataform >> run_dataform >> run_pyspark_aggregation
