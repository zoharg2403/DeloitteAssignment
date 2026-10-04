
from __future__ import annotations

from collections.abc import Iterable
from airflow.models import BaseOperator

from airflow_pipelines.utils.task_factory_providers import (
    ConfigTask,
    DataformTasks,
    DataprocTasks,
    )

from common.config import Config

class TaskFactory:

    def __init__(self, cfg: Config):
        self.cfg = cfg

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
                region          = self.cfg.env.region,
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
                        raise ValueError(f"Duplicate operator registration: {operator_name}")
                    self._creators[operator_name] = method

    def create_tasks(self, tasks: list[dict]) -> Iterable[BaseOperator]:
        """Create enabled Airflow operators and connect their configured dependencies."""
        cfg_tasks:     dict[str, ConfigTask]   = {}
        created_tasks: dict[str, BaseOperator] = {}

        for task in tasks:
            cfg_task = ConfigTask(**task)
            if not cfg_task.is_enabled:
                continue
            
            if cfg_task.task_id in cfg_tasks:
                raise ValueError(f"Duplicate task id: {cfg_task.task_id}")
            cfg_tasks[cfg_task.task_id] = cfg_task

            try:
                creator = self._creators[cfg_task.operator]
            except KeyError as e:
                raise ValueError(f"Unsupported operator: {cfg_task.operator}") from e
            created_tasks[cfg_task.task_id] = creator(cfg_task)

        if not created_tasks:
            raise ValueError("No enabled tasks are found")

        for task_id, cfg_task in cfg_tasks.items():
            for dependency in cfg_task.depends_on or []:
                if dependency not in created_tasks:
                    raise ValueError(f"Task '{task_id}' depends on unknown or disabled task '{dependency}'")
                created_tasks[dependency] >> created_tasks[task_id]

        return created_tasks.values()


# if __name__ == "__main__":
#     from common.config import Config
#     cfg = Config().load("config/airflow_pipelines/pipelines.yaml", "config/dataproc/jobs.yaml")
#     pipeline_name = "users_per_city"
#     cfg_pipeline = getattr(cfg.pipelines, pipeline_name)
#     task_factory = TaskFactory(cfg = cfg)
#     task_factory.create_tasks(cfg_pipeline.tasks)
