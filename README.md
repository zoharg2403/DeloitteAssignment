# Deloitte data engineering assignment

This project implements a small batch pipeline on Google Cloud:

1. Dataform transforms raw users and address data through bronze, silver, and
   gold datasets.
2. Dataproc Serverless runs a PySpark job that aggregates the gold user/address
   table by city.
3. The result is written to BigQuery as `gold.gold_users_per_city`.

## Repository layout

```text
dataform/                 Dataform project and SQLX definitions
dataproc/jobs/            Dataproc batch jobs
dataproc/utils/           Spark and BigQuery helpers
common/                   Shared Python utilities
config/                   Environment and owner-scoped application settings
   environments.yaml       Shared project, region, and bucket identifiers
   logging.yaml            Logging settings
   ingestion/              Upload and ingestion settings
   dataproc/               Runtime, jobs, and deployment settings
   airflow/                Airflow pipeline and shared DAG settings
      pipelines.yaml       DAG schedule and Dataform repository settings
      dag.yaml             Shared DAG default arguments
dataproc_deploy/           Release and Dataproc submission code
```

## Prerequisites

- Python 3.10+ with the packages in `requirements.txt`
- Node.js and npm for Dataform
- Google Cloud CLI (`gcloud`)
- A Google Cloud project with BigQuery, Cloud Storage, and Dataproc Serverless
  enabled

Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

Install Dataform dependencies:

```powershell
Set-Location dataform
npm install
Set-Location ..
```

## submodules

add dataform submodule: 
git submodule add https://github.com/zoharg2403/DeloitteAssignment-dataform.git dataform

## Authentication

```powershell
gcloud.cmd auth login
gcloud.cmd auth application-default login
gcloud.cmd config set project dataengproj-500110
gcloud.cmd auth application-default print-access-token
```

Set `APP_ENV` to select an environment; it defaults to `dev`. Shared project,
region, and bucket identifiers are defined in `config/environments.yaml`.
Component files contain only settings owned by that component. When code loads
multiple component files, duplicate leaf keys are rejected by the config
loader.

## Run Dataform

From the `dataform` directory, compile or run the definitions using the
Dataform CLI configured for the target BigQuery project:

```powershell
npx dataform compile
npx dataform run
```

The Dataproc job expects the Dataform gold table
`gold.gold_users_address` to exist first.

## Create a release and submit Dataproc

`config/deploy/dataproc.yaml` controls release creation and the Dataproc batch
options. With `dataproc_deploy.release.create_new: true`, running the submitter uploads the
configured source files and packages before submitting the batch:

```powershell
python -m dataproc_deploy.main
```

The submitter runs the `users_per_city` job from
`config/dataproc/jobs.yaml`. Each release is stored under a timestamped prefix
in the configured scripts bucket. The job uses `dataproc.zip`, `common.zip`,
and `config.zip` as Python file URIs; `.env` is passed as a file URI.

To submit an existing release, set
`dataproc_deploy.release.create_new: false` and replace
`dataproc_deploy.release.requested_version: latest` in
`config/deploy/dataproc.yaml` with the desired release version.

## Configuration

Configuration ownership is split by responsibility:

- `config/environments.yaml`: shared project, region, and bucket identifiers
- `config/ingestion/upload.yaml` and `config/ingestion/gcs_ingestion.yaml`:
   ingestion behavior
- `config/dataproc/jobs.yaml`: Dataproc job inputs, outputs, and write mode
- `config/dataproc/runtime.yaml` and `config/deploy/dataproc.yaml`:
   Spark runtime and release/submission options
- `config/airflow/pipelines.yaml`: DAG schedule and Dataform repository settings
- `config/airflow/dag.yaml`: shared Airflow DAG default arguments
- `config/logging.yaml`: application logging
- `dataform/workflow_settings.yaml`: Dataform-native project and dataset settings

The `users_per_city` job reads `gold.users_address` and writes
`gold.users_per_city` with `overwrite` mode. Its DAG runs daily at 06:00.
Logs are written to `outputs/logs` when file logging is enabled.
