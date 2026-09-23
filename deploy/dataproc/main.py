
import json
import uuid
from pathlib import Path
from google.cloud import dataproc_v1

from common.config import Config
from common.config_models import ConfigJob, ConfigJobSubmitter
from deploy.dataproc.release_manager import ReleaseManager


class JobSubmitter:

    def __init__(self):
        cfg = Config()
        self.cfg = ConfigJobSubmitter(env=cfg.env, **cfg.dataproc_deploy)

        try:
            self.release_mgr = ReleaseManager(bucket_uri=self.cfg.env.scripts_bucket, cfg=self.cfg.release)
        except Exception as e:
            raise Exception(f"Failed to initialize ReleaseManager with error: {e}") from e

    @property
    def release_version(self):
        return self.release_mgr.release_version

    def release_path_join(self, path: Path | str) -> str:
        return self.release_mgr.gcs_path_join(path)

    def run(self, job_name: str):
        cfg_job = ConfigJob(**Config().job(job_name))
        batch_id = f"{cfg_job.batch_name}-{uuid.uuid4().hex[:8]}"
        main_script_uri = self.release_path_join(cfg_job.main_script)

        parent = f"projects/{self.cfg.env.project_id}/locations/{self.cfg.env.region}"

        self.cfg.batch_kwargs.map_bucket_fullpath(map_func=self.release_path_join)
        batch = {
            "pyspark_batch": {
                "main_python_file_uri": main_script_uri,
                **self.cfg.batch_kwargs.pyspark
                },
                "runtime_config": {
                    "properties": {
                        **self.cfg.batch_kwargs.runtime_config_properties
                    }
                }
            }

        with dataproc_v1.BatchControllerClient(
                client_options={"api_endpoint": f"{self.cfg.env.region}-dataproc.googleapis.com:443"}
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
        job_url = f"https://console.cloud.google.com/dataproc/batches/{self.cfg.env.region}/{batch_id}?project={self.cfg.env.project_id}"
        print(f"job url: {job_url}")


if __name__ == "__main__":
    job_name="users_per_city" # must match the "configs/dataproc/jobs/{job_name}.yaml"
    submitter = JobSubmitter()
    submitter.run(job_name)
