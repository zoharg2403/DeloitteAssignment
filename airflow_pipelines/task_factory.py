
from dataclasses import dataclass, asdict, field
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
    dataform_compilation_task_id = "compile_dataform"

    def __init__(self, project_id: str, repository_id: str, location: str, git_commitish: str):
        self.project_id = project_id
        self.repository_id = repository_id
        self.location = location
        self.git_commitish = git_commitish

    def _create_dataform_compilation_result_task(self, cfg_task: ConfigTask):
        params = dict(cfg_task.params)
        compilation_result = params.pop("compilation_result", {})
        compilation_result.setdefault("git_commitish", self.git_commitish)

        return DataformCreateCompilationResultOperator(
            task_id=cfg_task.task_id,
            project_id=self.project_id,
            region=self.location,
            repository_id=self.repository_id,
            compilation_result=compilation_result,
            **params,
        )

    def _create_dataform_workflow_invocation_task(self, cfg_task: ConfigTask):
        params = dict(cfg_task.params)
        workflow_invocation = dict(params.pop("workflow_invocation", {}))
        workflow_invocation.setdefault(
            "compilation_result",
            "{{ ti.xcom_pull(task_ids='compile_dataform')['name'] }}",
        )
        invocation_config = dict(workflow_invocation.get("invocation_config", {}))
        for target in invocation_config.get("included_targets", []):
            target.setdefault("database", self.project_id)
        workflow_invocation["invocation_config"] = invocation_config

        return DataformCreateWorkflowInvocationOperator(
            task_id=cfg_task.task_id,
            project_id=self.project_id,
            region=self.location,
            repository_id=self.repository_id,
            workflow_invocation=workflow_invocation,
            **params,
        )

    def _create_dataproc_batch_task(self, cfg_task: ConfigTask):
        return DataprocCreateBatchOperator(
            task_id=cfg_task.task_id,
            project_id=self.project_id,
            region=self.location,
            **cfg_task.params,
        )

    def create_tasks(self, task_configs: list[dict]):
        creators = {
            "DataformCreateCompilationResultOperator": self._create_dataform_compilation_result_task,
            "DataformCreateWorkflowInvocationOperator": self._create_dataform_workflow_invocation_task,
            "DataprocCreateBatchOperator": self._create_dataproc_batch_task,
        }

        tasks = {}
        for cfg_task in task_configs:
            task_config = ConfigTask(**cfg_task)
            try:
                create_task = creators[task_config.operator]
            except KeyError as error:
                raise ValueError(f"Unsupported task operator: {task_config.operator}") from error
            tasks[task_config.task_id] = create_task(task_config)

        for cfg_task in task_configs:
            task_config = ConfigTask(**cfg_task)
            task = tasks[task_config.task_id]
            for dependency in task_config.depends_on or []:
                tasks[dependency] >> task

        return tasks