
from dataclasses import dataclass, field, asdict
from datetime import datetime as dt

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


@dataclass
class ConfigTask:
    task_id:    str
    operator:   str
    params:     dict = field(default_factory=dict)
    depends_on: list[str] | None = None


class TaskFactory:

    _creators = {}
    dataform_compilation_task_id = "compile_dataform"

    def __init__(self, project_id: str, dataform_repository_id: str, dataform_region: str, dataform_git_commitish: str):
        self.project_id = project_id
        
        self.dataform_repository_id = dataform_repository_id
        self.dataform_region        = dataform_region
        self.dataform_git_commitish = dataform_git_commitish

    @classmethod
    def register_creator(cls, operator_name: str):
        def decorator(func):
            cls._creators[operator_name] = func
            return func
        return decorator

    @register_creator("DataformCreateCompilationResultOperator")
    def _create_dataform_compilation_result_task(self, cfg_task: ConfigTask):
        params = cfg_task.params
        compilation_result = params.pop("compilation_result", {})
        compilation_result.setdefault("git_commitish", self.dataform_git_commitish)
        return DataformCreateCompilationResultOperator(
            task_id            = cfg_task.task_id,
            project_id         = self.project_id,
            region             = self.dataform_region,
            repository_id      = self.dataform_repository_id,
            compilation_result = compilation_result,
            **params,
        )

    @register_creator("DataformCreateWorkflowInvocationOperator")
    def _create_dataform_workflow_invocation_task(self, cfg_task: ConfigTask):
        params = cfg_task.params
        workflow_invocation = params.pop("workflow_invocation", {})
        workflow_invocation.setdefault(
            "compilation_result",
            "{{ ti.xcom_pull(task_ids='compile_dataform')['name'] }}",
        )
        invocation_config = dict(workflow_invocation.get("invocation_config", {}))
        for target in invocation_config.get("included_targets", []):
            target.setdefault("database", self.project_id)
        workflow_invocation["invocation_config"] = invocation_config

        return DataformCreateWorkflowInvocationOperator(
            task_id             = cfg_task.task_id,
            project_id          = self.project_id,
            region              = self.dataform_region,
            repository_id       = self.dataform_repository_id,
            workflow_invocation = workflow_invocation,
            **params,
        )

    # @register_creator("DataprocCreateBatchOperator")
    # def _create_dataproc_batch_task(self, cfg_task: ConfigTask):
    #     return DataprocCreateBatchOperator(
    #         task_id=cfg_task.task_id,
    #         project_id=self.project_id,
    #         region=self.dataform_region,
    #         **cfg_task.params,
    #     )

    def create_tasks(self, task_configs: list[dict]):
        cfg_tasks, tasks = {}, {}
        # creating tasks map: task_id -> Airflow operator instance 
        for task_config in task_configs:
            cfg_task = ConfigTask(**task_config)
            try:
                creator = self._creators[cfg_task.operator]
            except KeyError as error:
                raise ValueError(f"Unsupported task operator: {cfg_task.operator}") from error
            cfg_tasks[cfg_task.task_id] = cfg_task
            tasks[cfg_task.task_id]     = creator(cfg_task)

        # wiring up the task dependencies
        for task_id, cfg_task in cfg_tasks.items():
            task = tasks[task_id]
            for dependency in cfg_task.depends_on or []:
                tasks[dependency] >> task  # define upstream/downstream relationship

        return tasks
