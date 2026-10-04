# Deloitte data engineering assignment

This repository provides a foundation for building and operating multiple
batch data pipelines on Google Cloud.  

Its components cover the data lifecycle:
 - Uploading source files to Cloud Storage
 - Ingesting raw data files into BigQuery with
auditing 
 - Transforming data with Dataform
 - Running scalable processing jobs on
Dataproc Serverless
 - Orchestrating workflows with Airflow  

## Table of contents

- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
  - [Clone the repository](#clone-the-repository)
  - [Install Python dependencies](#install-python-dependencies)
  - [Configure Google Cloud authentication](#configure-google-cloud-authentication)
  - [Set environment](#set-environment)
- [Modules](#modules)
  - [Common Utilities](#common-utilities)
  - [Data Upload and Ingestion](#data-upload-and-ingestion)
  - [Dataform](#dataform)
  - [Dataproc](#dataproc)
  - [Airflow pipelines](#airflow-pipelines)
  - [Deployment](#deployment)
- [Configuration reference](#configuration-reference)

## Repository layout

```text
airflow_pipelines/         Airflow DAG and task-building utilities
common/                    Shared utilities
config/                    Environment, pipeline, ingestion, and deployment settings
dataform/                  Dataform SQLX project (separate repository)
dataproc/                  PySpark jobs and BigQuery/Spark helpers
deploy/                    Dataproc release and Airflow DAG upload commands
ingestion/                 Local upload and GCS-to-BigQuery ingestion
requirements.txt           Python dependencies
```

## Prerequisites

 - Git, Python 3.10 or newer, and pip.
 - Google Cloud CLI (`gcloud`) installed and available on `PATH`.
 - Access to the configured Google Cloud project (`dataengproj-500110`), its Dataform repository, and the Cloud Storage buckets and BigQuery datasets used by this project.
 - The required Google Cloud APIs enabled: BigQuery, Cloud Storage, Dataproc, and
  Dataform. A service account with access to the resources.
 - Node.js and npm only if compiling or running the local Dataform project.
 - For Airflow development or running the complete Python dependency set, use
  Linux or WSL2. Apache Airflow does not support native Windows installations.

### Clone the repository

```powershell
git clone https://github.com/zoharg2403/DeloitteAssignment.git
Set-Location <your-location>
```

The Dataform project is maintained in a separate repository.  
Clone it into the expected directory if it is not already present:

```powershell
git clone https://github.com/zoharg2403/DeloitteAssignment-dataform.git dataform
```

OR clone the repository with its submodules:

```powershell
git clone --recurse-submodules https://github.com/zoharg2403/DeloitteAssignment.git
Set-Location <your-location>
```

If you already cloned the repository and the submodule folders are empty, run this command from inside your project folder:

```powershell
git submodule update --init --recursive
```

### Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Configure Google Cloud authentication

Authenticate your user account and Application Default Credentials (ADC), which are used by the Google Cloud Python clients:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project dataengproj-500110
```

If you have permission to enable APIs, run:

```bash
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dataproc.googleapis.com \
  dataform.googleapis.com \
  --project dataengproj-500110
```

### Set environment

The project defaults to the `dev` environment.  
Set `APP_ENV` to select another environment, or set it in the [`.env`](.env#L2) file.  
That file is passed to Dataproc and included in the configured Airflow upload assets, so keep it limited to non-secret settings.  
Use ADC for credentials; do not put service account keys or access tokens in `.env`.  
GCP project environment is defined at [`environments.yaml`](config/environments.yaml)

## Modules

### Common utilities

[`common`](common/)

Shared helpers: 

**Configuration** ([`config.py`](common/config.py)):  
`Config` reads the root `.env`, loads and merges YAML files, and exposes nested settings as attributes.  
Use `cfg.env` to load [`environments.yaml`](config/environments.yaml) and get the current environment configuration.
```python
from common.config import Config

cfg = Config().load("config/common.yaml")
project_id = cfg.logger.name
project_id = cfg.env.project_id
```

**Logging** ([`common/logger.py`](common/logger.py)):  
`Logger` provides a shared, configured Python logger with stream and file handlers controlled by [`config/common.yaml`](config/common.yaml#L2).  
It forwards standard logging methods such as `info`, `warning`, and `exception`.

```python
from common.logger import Logger

logger = Logger()
logger.info("Starting ingestion for project %s", project_id)
```

### Data Upload and Ingestion

[`ingestion`](ingestion/)  

 - Main scripts:
   - Data Upload: [`data_upload.py`](ingestion/data_upload.py)
   - Ingestion: [`gcs_ingestion.py`](ingestion/gcs_ingestion.py)
   - Utilities: [`utils`](ingestion/utils/)
 - Configuration:
   - Upload and ingestion configuration - [ingestion.yaml](config\ingestion.yaml)  
   - GCS data (target) bucket - [environments.yaml/buckets.data_storage](config/environments.yaml#L9)

**File upload:**  
```python
GCSUpload.upload_files()
``` 
Local dir and file patterns are defined under [local](config\ingestion.yaml#L3). All files in `root_dir` (recursivly) that match a pattern from `patterns` will be uploaded to GCS.  
Files will be uploaded to [data storage bucket](config/environments.yaml#L9) \\ [incoming blob](config\ingestion.yaml#L10).  
Currently, this projects supports CSV files only.

**Data ingestion:** 
```python
GCSIngestion().run()
``` 
Files will be ingested from [data storage bucket](config/environments.yaml#L9) \\ [incoming blob](config\ingestion.yaml#L10), to defined [BigQuery dataset](config\ingestion.yaml#L16).  
Processed files will be moved from [incoming blob](config\ingestion.yaml#L10) to target blob based on ingestion statuse: [success](config\ingestion.yaml#L11), [skipped\duplicated](config\ingestion.yaml#L12), [failed](config\ingestion.yaml#L13).   
Source filename template\prefix defines the [target table](ingestion/gcs_ingestion.py#L46) and [path in target blob](ingestion/utils/gcs_service.py#L25).  
File processing metadata \ audit is kept in [BigQuery audit dataset](config\ingestion.yaml#L17).[audit table](config\ingestion.yaml#L18).
The audit table is used to prevent ingesting duplicated files into the same target table using file MD5 hash key, keep track of file ingestion status, and for later debugging of failed ingestions.

### Dataform

Separated repository cloned to [`dataform`](dataform/) submodule.  
SQLX files are located at [`definitions`](dataform/definitions/), separated into schemas: `bronze`, `silver`, `gold`.

Install the Dataform CLI if you want to compile or run the local SQLX project:

```bash
npm install --global @dataform/cli
cd dataform
dataform compile
dataform run
cd ..
```

### Dataproc

[`dataproc`](dataproc/)

 - Job scripts: [`dataproc/jobs`](dataproc/jobs/)
 - Shared Spark and BigQuery helpers: [`dataproc/utils`](dataproc/utils/)
 - Per-job input, output, and runtime settings: [`jobs.yaml`](config/dataproc/jobs.yaml)

**Creating a job:**  

Add a script under [`dataproc/jobs/`](dataproc/jobs/) and a matching job entry in [`jobs.yaml`](config/dataproc/jobs.yaml).  
Create a [`JobContext`](dataproc/utils/job_context.py#L14) with that entry's key (for example, `JobContext.create("my_job")`) to access:

 - `JobContext.cfg_job` — the job-specific configuration, including source, target, and write mode.
 - `JobContext.logger` — the shared application logger.
 - `JobContext.spark` — the configured Spark session.
 - `JobContext.bigquery` — the helper for reading from and writing to BigQuery.
 - `JobContext.source_table` and `JobContext.target_table` — fully qualified table names.

Use `JobContext` as a context manager so it stops the Spark session when the job finishes.

Example job: 

```python
from dataproc.utils.job_context import JobContext

def main() -> None:
    with JobContext.create("my_job") as ctx:
        ctx.logger.info("Starting job: reading %s", ctx.source_table)
        df = ctx.bigquery.read(
            ctx.cfg_job.source.dataset,
            ctx.cfg_job.source.table,
        )
        # Transform df here.
        ctx.bigquery.write(
            df,
            ctx.cfg_job.target.dataset,
            ctx.cfg_job.target.table,
            mode=ctx.cfg_job.write_mode,
        )
        ctx.logger.info("Job completed: wrote %s", ctx.target_table)
```

### Airflow pipelines

[`airflow_pipelines`](airflow_pipelines/) 

 - DAGs definitions: [`airflow_pipelines/dags`](airflow_pipelines/dags)
 - DAGs utilities: [`airflow_pipelines/utils`](airflow_pipelines/utils/)
 - DAGs and pipeline tasks definitions: [`pipelines.yaml`](config/airflow_pipelines/pipelines.yaml)

**Creating a pipeline:**  

Create a script under [`airflow_pipelines/dags`](airflow_pipelines/dags) and a matching pipeline entry under [`pipelines.yaml`](config/airflow_pipelines/pipelines.yaml).  
Create a [`PipelineContext`](airflow_pipelines/utils/pipeline_context.py#L16) with that entry's key (for example, `PipelineContext.create("my_pipeline")`) to access:

 - `PipelineContext.cfg_pipeline` — the pipeline-specific configuration (dag and tasks)
 - `PipelineContext.cfg_dag` — get DAG settings `ConfigDag`
 - `PipelineContext.task_factory` — `TaskFactory` utility used for creating the tasks 
 - `PipelineContext.create_tasks` — exposing `TaskFactory.create_tasks(...)`

Use `PipelineContext` as a context manager.

**Creating a task:**  

**Prerequisites:** An Airflow task only orchestrates an operation; it does not create the underlying job or its code. Before adding a task, ensure its target is implemented and configured. For example, a Dataproc task's `job_name` must match an existing job in [`jobs.yaml`](config/dataproc/jobs.yaml), and that job's `main_script` and supporting files must already exist and be included in the Dataproc release. Likewise, Dataform tasks require the repository, definitions, and targets they reference to be available.

Tasks are defined in the pipeline's `tasks` list in [`pipelines.yaml`](config/airflow_pipelines/pipelines.yaml).  

Each task requires:

 - `task_id` — unique ID for the task within the DAG.
 - `is_enabled` — whether to create the task (use true / false to toggle the task on / off).
 - `operator` — operator type, matched to a registered provider creator.

Optional:
 - `params` - params to pass into the provider creator methode.
 - `depends_on` - lists upstream task IDs to define execution order.

[`TaskFactory.create_tasks()`](airflow_pipelines/utils/task_factory.py#L49) reads these definitions as `ConfigTask` objects and creates an Airflow operator for each enabled task using the registered provider creator.  

Providers define creator methods that build operators, currently existing [`DataformTasks`](airflow_pipelines/utils/task_factory_providers.py#L38) and [`DataprocTasks`](airflow_pipelines/utils/task_factory_providers.py#L81).  
The decorator `@operator("OperatorName")`, links the YAML `operator` in the task configuration to that method.  
To support another operator type, add a creator method decorated with `@operator("OperatorName")` to an existing provider, or create a new provider
and register it in `TaskFactory._init_creators()`.

Example DAG:

```python
from airflow import DAG
from airflow_pipelines.utils.pipeline_context import PipelineContext

ctx = PipelineContext.create("my_pipeline")

with DAG(**ctx.cfg_dag) as dag:
    ctx.create_tasks()
```

### Deployment

**Dataproc**:

Main script: [`deploy/dataproc/main.py`](deploy/dataproc/main.py)  
Configuration: [`config/deploy/dataproc.yaml`](config/deploy/dataproc.yaml)

`JobSubmitter` resolves the release version:
 - if `dataproc_deploy.release.create_new: true` - create a new release version `release-<UTC timestamp>-<unique suffix>` and upload it to [`dataproc_scripts`](config/environments.yaml#L10) bucket. The release content is defined using [`assets`](config/deploy/dataproc.yaml#L7) and [`ignore_patterns`](config/deploy/dataproc.yaml#L19).
 - if `dataproc_deploy.release.create_new: false` and `dataproc_deploy.release.requested_version: latest` - Get the latest release in the [`dataproc_scripts`](config/environments.yaml#L10) bucket.
 - if `dataproc_deploy.release.create_new: false` and `dataproc_deploy.release.requested_version: release-<UTC timestamp>-<unique suffix>` - Use the specified release.

All jobs submitted will use the scripts from the resolved release version.  
The job submission settings are loaded from [`jobs.yaml`](config/dataproc/jobs.yaml). 

Example:
```python
submitter = JobSubmitter()
submitter.run("job1")  # Job name must correspond to an entry in jobs.yaml and have an existing main script 
submitter.run("job2")
```
**Note:** No dependency is defined between jobs

**Airflow pipelines:**  

Main script: [`deploy/airflow_pipelines/main.py`](deploy/airflow_pipelines/main.py)  
Configuration: [`config/deploy/airflow_pipelines.yaml`](config/deploy/airflow_pipelines.yaml)

`DagSubmitter` uploads the DAG files and support files/directories listed in [`assets`](config/deploy/airflow_pipelines.yaml#L3), while ignoring [`ignore_patterns`](config/deploy/airflow_pipelines.yaml#L18), to the configured [`airflow_scripts`](config/environments.yaml#L12) bucket under its `dags/` prefix.  
Airflow read the DAGs automatically from `dags/` blob.

## Configuration reference

- [`config/environments.yaml`](config/environments.yaml): Project enviroment configuration (e.g. GCS buckets, Dataform repo, dataproc release and spark session).
- [`config/ingestion.yaml`](config/ingestion.yaml): Settings for data upload and ingestion into BigQuery.
- [`config/airflow_pipelines/pipelines.yaml`](config/airflow_pipelines/pipelines.yaml): Per-pipeline configuration (e.g. DAG schedule, task operators, and dependencies).
- [`config/dataproc/jobs.yaml`](config/dataproc/jobs.yaml): Per-Dataproc batch job configuration (e.g. main script, inputs, outputs, write mode, batch kwargs).
- [`config/deploy/dataproc.yaml`] and [`config/deploy/airflow_pipelines.yaml`]: Dataproc release and DAGs upload (and submission) settings.
- [`config/common.yaml`](config/common.yaml): Common utils configurations.
