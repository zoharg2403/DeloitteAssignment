
from airflow_pipelines.utils.task_factory_providers import (
    ConfigTask,
    DataformTasks,
    DataprocTasks,
    )

from common.config import Config

class TaskFactory:
    
    def __init__(self, cfg: Config):
        self.cfg   = cfg
        self._creators = {}
        self._init_creators()

    def _init_creators(self):
        providers = [
            DataformTasks(
                project_id      = self.cfg.env.project_id,
                region          = self.cfg.env.region,
                repository_id   = self.cfg.env.dataform.repository_id,
                git_commitish   = self.cfg.env.dataform.git_commitish,
                service_account = self.cfg.env.dataform.service_account,
            ),
            DataprocTasks(
                project_id      = self.cfg.env.project_id,
                bucket_uri      = self.cfg.env.buckets.dataproc_scripts,
                release_version = self.cfg.env.dataproc.release_version,
                cfg_jobs        = self.cfg.jobs
            ),
        ]
        for prvdr in providers:
            for method_name in dir(prvdr):
                method = getattr(prvdr, method_name)
                operator_name = getattr(method, "_operator", None)
                if operator_name:
                    if operator_name in self._creators:
                        raise ValueError(f"Duplicate task operator registration: {operator_name}")
                    self._creators[operator_name] = method

    def create_tasks(self, tasks: list):
        cfg_tasks, created_tasks = {}, {}
        for task in tasks:
            cfg_task = ConfigTask(**task)
            if cfg_task.is_enabled is True:
                try:
                    creator = self._creators[cfg_task.operator]
                except KeyError as e:
                    raise ValueError(f"Unsupported operator: {cfg_task.operator}") from e
            cfg_tasks[cfg_task.task_id]     = cfg_task
            created_tasks[cfg_task.task_id] = creator(cfg_task)

        if not cfg_tasks or not created_tasks:
            raise ValueError("No enabled tasks are found")

        for task_id, cfg_task in cfg_tasks.items():
            for dependency in cfg_task.depends_on or []:
                try:
                    created_tasks[dependency] >> created_tasks[task_id]
                except KeyError as e:
                    raise KeyError(f"Task '{task_id}' is unknown or depends on unknown task '{dependency}'")

        return created_tasks.values()


if __name__ == "__main__":
    from common.config import _Root, Config
    cfg = Config().load("config/airflow_pipelines/pipelines.yaml")
    pipeline_name = "users_per_city"
    cfg_pipeline = getattr(cfg.pipelines, pipeline_name)
    task_factory = TaskFactory(cfg = cfg)
    task_factory.create_tasks(cfg_pipeline.tasks)
