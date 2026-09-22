# Configuration

Configuration is split by ownership:

- `project.yaml`: environment-independent project conventions.
- `environments/<environment>.yaml`: GCP projects, regions, buckets, datasets, and Dataform repository settings.
- `dataproc/jobs/<job>.yaml`: Dataproc job input/output contracts and Spark options.
- `pipelines/<pipeline>.yaml`: Airflow scheduling, retries, and component dependencies.

Use this precedence when values overlap:

```text
project defaults < environment < component/job < runtime arguments
```

Keep secrets out of these files. Use Airflow Connections, Google Secret Manager, or environment variables for credentials and tokens.

The current root `config.yaml` and `submit_job/config.yaml` remain as compatibility files while the submitter and worker package are migrated to this layout.
