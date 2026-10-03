
from airflow_pipelines.utils.task_factory_providers import (
    ConfigTask,
    DataformTasks,
    )

from common.config import _Root

class TaskFactory:
    
    _creators = None

    def __init__(self, cfg_env: _Root):
        self.cfg_env   = cfg_env
        self._init_creators()

    def _init_creators(self):
        self._creators = self._creators or {}
        providers = [
            DataformTasks(
                project_id = self.cfg_env.project_id,
                **self.cfg_env.dataform
            ),
            # DataprocTasks(
            #     project_id = self.project_id,
            #     **self.cfg.env.dataproc
            # ),
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
    task_factory = TaskFactory(cfg_env = cfg.env)
    task_factory.create_tasks(cfg_pipeline.tasks)
