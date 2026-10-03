from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

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

    def create_tasks(self):
        return self.task_factory.create_tasks(self.cfg_pipeline.tasks)

    def _init_task_factory(self):
        return TaskFactory(cfg_env = self.cfg.env)

    @classmethod
    def create(cls, pipeline_name: str) -> PipelineContext:
        base_dir = Path(__file__).parent.parent.parent
        inst = cls(
            pipeline_name = pipeline_name,
            cfg           = Config().load(base_dir / "config" / "airflow_pipelines" / "pipelines.yaml"), 
            logger        = Logger(base_dir / "config" / "common.yaml"),
            )
        inst.task_factory = inst._init_task_factory()
        return inst

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self) -> None:
        self.logger.info("Stopping Spark session")
        self.spark_session.stop()

# if __name__ == "__main__":
#     ctx = PipelineContext.create("users_per_city")
#     print({**ctx.cfg_dag})

