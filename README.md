
# Authenticate the gcloud CLI and create Application Default Credentials for
# the Google Cloud Python client libraries used by the submitter.
gcloud.cmd auth login
gcloud.cmd auth application-default login

# Select the project used by the active environment configuration.
gcloud.cmd config set project dataengproj-500110

# Verify
gcloud.cmd auth application-default print-access-token

## Releases

Uploaded job assets are stored under an immutable release / debug prefix:

```text
gs://<scripts_bucket>/releases/<release_version>/
gs://<scripts_bucket>/debug/<release_version>/
```

Set `RELEASE_VERSION` in CI to a git tag or commit SHA. It overrides the
release settings are defined in `configs/dataproc/deploy/release.yaml`:

```powershell
$env:RELEASE_VERSION = "2026.09.17-a1b2c3d"
python -m deploy.dataproc.main
```

The main script is uploaded directly, while `dataproc/utils` is uploaded as
`dataproc/utils.zip` and listed in `python_file_uris`. Configuration files are
uploaded from `configs/` so the worker can load the selected environment and job.

Set `release_version` to `latest` to submit an existing release without
uploading files. Set `update_scripts: true` and provide a concrete
`RELEASE_VERSION` when publishing a new release.

'''
dataform/
│
├── includes/
│   └── schema.sqlx
│
├── definitions/
│   ├── bronze/
│   │   ├── users.sqlx
│   │   └── address.sqlx
│   ├── silver/
│   │   ├── users_silver.sqlx
│   │   └── address_silver.sqlx
│   └── gold/
│   │   ├── 
│   │   └── 
│
├── dataform.json
└── package.json

'''