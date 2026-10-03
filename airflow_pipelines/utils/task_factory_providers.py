
import uuid
import shutil
import subprocess
from pathlib import Path
from copy import deepcopy
from dataclasses import dataclass, field

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

from common.config import _Root

@dataclass
class ConfigTask:
    task_id:    str
    is_enabled: bool
    operator:   str
    params:     dict = field(default_factory=dict)
    depends_on: list = field(default_factory=list)

    def copy_params(self):
        return deepcopy(self.params)


def operator(name):
    def decorator(func):
        func._operator = name
        return func
    return decorator


class DataformTasks:

    def __init__(self, project_id: str, region: str, repository_id: str, git_commitish: str, service_account: str):
        self.project_id    = project_id
        self.region        = region
        self.repository_id = repository_id
        self.git_commitish = git_commitish
        self.service_account = service_account

    @operator("DataformCreateCompilationResultOperator")
    def create_compilation_result(self, cfg_task: ConfigTask):
        params = cfg_task.copy_params()
        compilation_result = params.pop("compilation_result", {})
        compilation_result.setdefault("git_commitish", self.git_commitish)
        return DataformCreateCompilationResultOperator(
            task_id            = cfg_task.task_id,
            project_id         = self.project_id,
            region             = self.region,
            repository_id      = self.repository_id,
            compilation_result = compilation_result,
            **params,
        )

    @operator("DataformCreateWorkflowInvocationOperator")
    def create_workflow_invocation(self, cfg_task: ConfigTask):
        params = cfg_task.copy_params()
        
        workflow_invocation = params.pop("workflow_invocation", {})
        workflow_invocation.setdefault("compilation_result", "{{ ti.xcom_pull(task_ids='compile_dataform')['name'] }}")
        workflow_invocation.setdefault("invocation_config", {}).setdefault("service_account", self.service_account)
        
        for target in workflow_invocation.get("invocation_config", {}).get("included_targets", []):
            target.setdefault("database", self.project_id)
        return DataformCreateWorkflowInvocationOperator(
            task_id             = cfg_task.task_id,
            project_id          = self.project_id,
            region              = self.region,
            repository_id       = self.repository_id,
            workflow_invocation = workflow_invocation,
            **params,
        )


class DataprocTasks:

    def __init__(self, project_id: str, bucket_uri: str, release_version: str, cfg_jobs: _Root):
        self.project_id = project_id
        self.bucket_uri = bucket_uri.strip('/')

        # resolve release version and uri
        if release_version == "latest":
            self.release_version = self._get_latest_release_version()
        else:
            self.release_version = release_version.strip('/')
            if not self._is_release_version_exists(self.release_uri):
                raise RuntimeError(f"Release varsion '{self.release_version}' was not found in {self.bucket_uri}")

        self.cfg_jobs = cfg_jobs

        self.gcloud_      = None
        self.release_uri_ = None

    @property
    def gcloud(self):
        if self.gcloud_ is None:
            self.gcloud_ = shutil.which("gcloud.cmd") or shutil.which("gcloud")
            if not self.gcloud_:
                raise RuntimeError("gcloud CLI is required")
        return self.gcloud_

    @property
    def release_uri(self) -> str:
        if self.release_uri_ is None:
            self.release_uri_ = f"{self.bucket_uri}/{self.release_version.strip('/')}"
        return self.release_uri_

    def gcs_path_join(self, path: Path | str) -> str:
        return f"{self.release_uri}/{Path(path).as_posix().strip('/')}"
    
    def _get_latest_release_version(self) -> str:
        """Get the latest release in the bucket"""
        res = subprocess.run(
            [self.gcloud, "storage", "ls", f"{self.bucket_uri}/"],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            details = res.stderr.strip() or res.stdout.strip()
            raise RuntimeError(f"Could not list releases in '{self.bucket_uri}'.\n{details}")

        versions = [
            line.strip().rstrip("/").rsplit("/", 1)[-1]
            for line in res.stdout.splitlines()
            ]
        if not versions:
            raise RuntimeError(f"No releases found in {self.bucket_uri}")

        return max(versions, key=lambda v: v.split("-")[1])

    def _is_release_version_exists(self, uri: str) -> bool:
        res = subprocess.run(
            [self.gcloud, "storage", "ls", uri.strip("/")],
            capture_output=True,
            text=True,
        )
        return res.returncode == 0 and bool(res.stdout.strip())

    @operator("DataprocCreateBatchOperator")
    def create_batch(self, cfg_task: ConfigTask):
        params = cfg_task.copy_params()
        job_name = params.pop("job_name")
        cfg_job = self.cfg_jobs[job_name]
        batch_id = f"{cfg_job.batch_name}-{uuid.uuid4().hex[:8]}"
        
        batch = {
            "pyspark_batch": {
                "main_python_file_uri": self.gcs_path_join(cfg_job.main_script),
                **{
                    k: [self.gcs_path_join(vi) for vi in v] 
                    for k, v in cfg_job.batch_kwargs.pyspark.items() 
                   }
                },
                "runtime_config": {
                    "properties": {
                        k: str(v).lower() 
                        for k, v in cfg_job.batch_kwargs.runtime_config_properties.items()
                        }
                    }
                }
        return DataprocCreateBatchOperator(
            task_id    = cfg_task.task_id,
            project_id = self.project_id,
            region     = self.region,
            batch_id   = batch_id,
            batch      = batch,
            **params,
        )
