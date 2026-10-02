
import json
import uuid
from pathlib import Path
from google.cloud import dataproc_v1

from common.config import Config
from deploy.utils.release_manager import ReleaseManager


class JobSubmitter:

    def __init__(self):
        self.cfg = Config().load(
            "config/dataproc/runtime.yaml",
            "config/dataproc/jobs.yaml",
            "config/dataproc/deploy.yaml",
        )

        try:
            self.release_mgr = ReleaseManager(
                bucket_uri        = self.cfg.env.buckets.dataproc_scripts,
                local_dir         = self.cfg.dataproc_deploy.release.local_dir,
                create_new        = self.cfg.dataproc_deploy.release.create_new,
                requested_version = self.cfg.dataproc_deploy.release.requested_version,
                assets            = self.cfg.dataproc_deploy.assets,
                ignore_patterns   = self.cfg.dataproc_deploy.ignore_patterns,
                )
        except Exception as e:
            raise Exception(f"Failed to initialize ReleaseManager with error: {e}") from e

    @property
    def release_version(self):
        return self.release_mgr.release_version

    @property
    def release_uri(self) -> str:
        return self.release_mgr.release_uri

    def gcs_path_join(self, path: Path | str) -> str:
        return f"{self.release_uri}/{Path(path).as_posix().strip('/')}"

    def run(self, job_name: str):
        cfg_job = getattr(self.cfg.jobs, job_name)
        batch_id = f"{cfg_job.batch_name}-{uuid.uuid4().hex[:8]}"
        main_script_uri = self.gcs_path_join(cfg_job.main_script)

        parent = f"projects/{self.cfg.env.project_id}/locations/{self.cfg.env.dataproc.region}"

        batch = {
            "pyspark_batch": {
                "main_python_file_uri": main_script_uri,
                **{
                    k: [self.gcs_path_join(vi) for vi in v] 
                    for k, v in self.cfg.dataproc_deploy.batch_kwargs.pyspark.items() 
                   }
                },
                "runtime_config": {
                    "properties": {
                        k: str(v).lower() 
                        for k, v in self.cfg.dataproc_deploy.batch_kwargs.runtime_config_properties.items()
                        }
                    }
                }

        with dataproc_v1.BatchControllerClient(
                client_options={"api_endpoint": f"{self.cfg.env.dataproc.region}-dataproc.googleapis.com:443"}
                ) as client:
            
            print("Submitting Dataproc job: job=%s; batch_id=%s; release_version=%s; parent=%s; batch=%s;" % (
                job_name, batch_id, self.release_version, parent, json.dumps(batch, default=str, sort_keys=True)))
            response = client.create_batch(
                request={
                    "parent": parent,
                    "batch": batch,
                    "batch_id": batch_id
                }
            )

        print(f"Dataproc job Compleated: operation={response.operation.name};")
        job_url = f"https://console.cloud.google.com/dataproc/batches/{self.cfg.env.dataproc.region}/{batch_id}?project={self.cfg.env.project_id}"
        print(f"job url: {job_url}")


if __name__ == "__main__":
    job_name="users_per_city" # must match the "config/dataproc/jobs.yaml/jobs:{job_name}.py"
    submitter = JobSubmitter()
    submitter.run(job_name)
