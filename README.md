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
common/                   Shared configuration and logging
deploy/dataproc/          Release and Dataproc submission code
config.yaml               Environment and job configuration
workflow_settings.yaml    Workflow/DAG settings
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

## Authentication

```powershell
gcloud.cmd auth login
gcloud.cmd auth application-default login
gcloud.cmd config set project dataengproj-500110
gcloud.cmd auth application-default print-access-token
```

Update the project, region, and bucket values in `config.yaml` for another
environment. The current development environment uses project
`dataengproj-500110`, region `us-central1`, and the configured Dataproc scripts
bucket.

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

`deploy/dataproc/deploy.yaml` controls release creation and the Dataproc batch
options. With `release.create_new: True`, running the submitter uploads the
configured source files and packages before submitting the batch:

```powershell
python -m deploy.dataproc.main
```

The submitter runs the `users_per_city` job from `config.yaml`. Each release is
stored under a timestamped prefix in the configured scripts bucket. The job
uses `dataproc.zip` and `common.zip` as Python file URIs and reads `config.yaml`
and `.env` from the same release.

To submit an existing release, set `release.create_new: False` and replace
`release.requested_version: latest` in `deploy/dataproc/deploy.yaml` with the
desired release version.

## Configuration

The `users_per_city` job is configured in `config.yaml`:

- source: `gold.gold_users_address`
- target: `gold.gold_users_per_city`
- write mode: `overwrite`
- schedule settings: `workflow_settings.yaml` (daily at 06:00)

Logs are written to `outputs/logs` when file logging is enabled in
`config.yaml`.
