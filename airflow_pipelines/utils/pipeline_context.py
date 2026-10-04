from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from dataclasses import dataclass
from types import TracebackType
from airflow.models import BaseOperator

from common.config import Config, _Root
from common.logger import Logger
from airflow_pipelines.utils.task_factory import TaskFactory
from airflow_pipelines.utils.config_dag import ConfigDag


@dataclass
class PipelineContext:
    """
    Shared runtime resources and configuration for Airflow pipeline.
    Create instances via PipelineContext.create(...).
    """

    pipeline_name: str
    cfg:           Config
    logger:        Logger
    task_factory: TaskFactory | None = None


    @property
    def cfg_pipeline(self) -> _Root:
        return getattr(self.cfg.pipelines, self.pipeline_name)

    @property
    def cfg_dag(self) -> ConfigDag:
        return ConfigDag(**self.cfg_pipeline.dag)

    def create_tasks(self) -> Iterable[BaseOperator]:
        """Create the Airflow operators configured for this pipeline."""
        if self.task_factory is None:
            raise RuntimeError("Pipeline task factory has not been initialized")
        return self.task_factory.create_tasks(self.cfg_pipeline.tasks)

    def _init_task_factory(self) -> TaskFactory:
        return TaskFactory(cfg = self.cfg)

    @classmethod
    def create(cls, pipeline_name: str) -> PipelineContext:
        base_dir = Path(__file__).parent.parent.parent
        inst = cls(
            pipeline_name = pipeline_name,
            logger = Logger(base_dir / "config" / "common.yaml"),
            cfg = Config().load(
                base_dir / "config" / "airflow_pipelines" / "pipelines.yaml", 
                base_dir / "config" / "dataproc" / "jobs.yaml", 
                ), 
            )
        inst.task_factory = inst._init_task_factory()
        return inst

    def __enter__(self) -> PipelineContext:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        del _exc_type, _exc_value, _traceback
        self.close()

    def close(self) -> None:
        """Close the context; no external runtime resources are owned here."""
        self.logger.info("Pipeline context closed")

# if __name__ == "__main__":
#     ctx = PipelineContext.create("users_per_city")
#     print({**ctx.cfg_dag})
