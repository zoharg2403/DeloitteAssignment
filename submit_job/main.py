import json
import uuid
from pydantic import BaseModel, Field
from google.cloud import dataproc_v1

from dataproc.utils.config import Config
from dataproc.utils.logger import Logger

from gcs_bucket import GCSBucketSync
from release_manager import ReleaseManager


class ConfigJobRuntime(BaseModel):
    project_id:       str  = Field(..., description="Project ID")
    bucket_uri:       str  = Field(..., description="GCS bucket for job scripts and config")
    debug_bucket_uri: str  = Field(..., description="GCS bucket for job scripts and config")
    region:           str  = Field(..., description="Region for Dataproc cluster")
    debug:            bool = Field(False, description="create debug release")    
    release:          dict = Field({'create_new': False, "requested_version": 'latest'}, description="Release varsion kwargs")
    assets:           dict = Field({'include': [], 'include_zipped': [], 'ignore_patterns': {}}, description="Release assets to include")
    kwargs:           dict = Field({}, description="Additional kwargs for pyspark batch submit")


class JobRuntime:

    def __init__(self):
        self.cfg = ConfigJobRuntime(**Config("submit_job/config.yaml").root)
        self.logger = Logger()

        self.release_version = None
        self.bucket_uri_     = None

    @property
    def bucket_uri(self):
        if self.bucket_uri_ is None:
            self.bucket_uri_ = (self.cfg.debug_bucket_uri if self.cfg.debug else self.cfg.bucket_uri).rstrip('/')
        return self.bucket_uri_

    def init_release_package(self):
        try:
            release_mgr = ReleaseManager(
                bucket_uri = self.bucket_uri,
                **self.cfg.release
                )
            self.release_version = release_mgr.resolve_release()

            if release_mgr.create_new:
                bucket_sync = GCSBucketSync(
                    bucket_uri = self.bucket_uri,
                    **self.cfg.assets
                    )
                bucket_sync.create(self.release_version)

        except Exception as e:
            self.logger.error("Failed to init release package for job submittion")
            raise

    def bucket_fullpath(self, rel_path):
        return f"{self.bucket_uri}/{self.release_version.strip("/")}/{rel_path}"

    def submit_pyspark_batch(self, job_name, aqe_enabled: bool | str = True, *args, **kwargs):
        batch_id = f"{job_name.replace("_", "-")}-{uuid.uuid4().hex[:8]}"

        client = dataproc_v1.BatchControllerClient(
              client_options={"api_endpoint": f"{self.cfg.region}-dataproc.googleapis.com:443"}
        )

        main_script = getattr(Config().jobs, job_name).main_script
        main_script_uri = self.bucket_fullpath(main_script)
        # Update filepath to full bucket path
        for kw in ['python_file_uris', 'file_uris']:
            if kw in kwargs:
                val = kwargs[kw]
                if isinstance(val, str):
                    val = [val]
                kwargs[kw] = [self.bucket_fullpath(v) for v in val]

        # Create the request body
        batch = {
            "pyspark_batch": {  # Define the PySpark job
                "main_python_file_uri": main_script_uri,
                "args": args or [],
                **kwargs
            },
            "runtime_config": {  # Define Spark runtime settings applied by Dataproc when creating the Spark session 
                "properties": {
                    "spark.sql.adaptive.enabled": str(aqe_enabled).lower()
                    }
                    },
                    }
        parent = f"projects/{self.cfg.project_id}/locations/{self.cfg.region}"
        self.logger.info(
            "Submitting Dataproc job: job=%s; batch_id=%s; release_version=%s; region=%s; args=%s; kwargs=%s; config=%s;, properties=%s;",
            job_name, batch_id, self.release_version, self.cfg.region,
            json.dumps(args or [], default=str),
            json.dumps(kwargs, default=str, sort_keys=True),
            json.dumps(Config().as_dict(), default=str, sort_keys=True),
            json.dumps(batch['runtime_config']["properties"]),
        )
        response = client.create_batch(
            request={
                "parent": parent, 
                "batch": batch, 
                "batch_id": batch_id
                }
                )
        self.logger.info(f"Dataproc job submitted: job={job_name}; batch_id={batch_id}; operation={response.operation.name};")
        job_url = f"https://console.cloud.google.com/dataproc/batches/{self.cfg.region}/{batch_id}?project={self.cfg.project_id}"
        self.logger.info(f"job url: {job_url}")

    def run(self, job_name: str, aqe_enabled: bool = True):
        self.logger.info(f"Starting job '{job_name}'" + (f" AQE_enabled={aqe_enabled}" if aqe_enabled else ''))
        self.init_release_package()
        self.submit_pyspark_batch(job_name, aqe_enabled, **self.cfg.kwargs)


if __name__ == "__main__":
    aqe_enabled = False
    job_name = "users_per_city"
    JobRuntime().run(job_name, aqe_enabled)

