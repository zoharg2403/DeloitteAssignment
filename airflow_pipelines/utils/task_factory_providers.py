
from copy import deepcopy
from dataclasses import dataclass, field, InitVar

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
    params:     InitVar[dict | None] = None
    depends_on: list[str] | None = None

    _params: dict = field(default_factory=dict, repr=False)

    def __post_init__(self, params):
        self._params = params

    @property
    def params(self) -> dict:
        return deepcopy(self._params)


def operator(name):
    def decorator(func):
        func._operator = name
        return func
    return decorator


class DataformTasks:

    def __init__(self, project_id: str, repository_id: str, region: str, git_commitish: str):
        self.project_id    = project_id
        self.repository_id = repository_id
        self.region        = region
        self.git_commitish = git_commitish

    @operator("DataformCreateCompilationResultOperator")
    def create_compilation_result(self, cfg_task: ConfigTask):
        params = cfg_task.params
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
        params = cfg_task.params
        workflow_invocation = params.pop("workflow_invocation", {})
        workflow_invocation.setdefault("compilation_result", "{{ ti.xcom_pull(task_ids='compile_dataform')['name'] }}")
        invocation_config = dict(workflow_invocation.get("invocation_config", {}))
        for target in invocation_config.get("included_targets", []):
            target.setdefault("database", self.project_id)
        workflow_invocation["invocation_config"] = invocation_config
        return DataformCreateWorkflowInvocationOperator(
            task_id             = cfg_task.task_id,
            project_id          = self.project_id,
            region              = self.region,
            repository_id       = self.repository_id,
            workflow_invocation = workflow_invocation,
            **params,
        )


# class DataprocTasks:

#     def __init__(self, project_id: str, region: str):
#         self.project_id = project_id
#         self.region = region

#     @operator("DataprocCreateBatchOperator")
#     def create_batch(self, cfg_task: ConfigTask):
#         return DataprocCreateBatchOperator(
#             task_id=cfg_task.task_id,
#             project_id=self.project_id,
#             region=self.region,
#             **cfg_task.params,
#         )
