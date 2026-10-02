
from airflow import DAG
from dataclasses import dataclass, asdict
from datetime import datetime as dt

from common.config import Config
from airflow_pipelines.dags.task_factory import TaskFactory


@dataclass
class ConfigDag:
    dag_id:          str
    schedule:        str
    start_date:      str | dt  # str will be converted to datetime object
    catchup:         bool
    max_active_runs: int

    def __post_init__(self):
        # convert start_date str -> datetime object
        if isinstance(self.start_date, str):
            try:
                self.start_date = dt.strptime(self.start_date, "%d-%m-%Y")
            except ValueError:
                raise ValueError(f"Invalid start_date format: {self.start_date}. Expected format: dd-mm-yyyy")

    def __getitem__(self, key):
        return getattr(self, key)
    
    def keys(self):
        return asdict(self).keys()

    
class PipelineRunner:
    def __init__(self):
        self.cfg = Config().load(
            "config/airflow_pipelines/pipelines.yaml",
            # "config/dataproc/jobs.yaml",
            # "config/dataproc/deploy.yaml",
        )
        self.task_factory = TaskFactory(
            project_id             = self.cfg.env.project_id,
            dataform_repository_id = self.cfg.env.dataform.repository_id,
            dataform_region        = self.cfg.env.dataform.region,
            dataform_git_commitish = self.cfg.env.dataform.git_commitish,
        )

    def run(self, pipeline_name: str):
        cfg_pipeline = getattr(self.cfg.pipelines, pipeline_name)
        cfg_dag = ConfigDag(**cfg_pipeline.dag)

        with DAG(**cfg_dag) as dag:
            self.task_factory.create_tasks(cfg_pipeline.tasks)



if __name__ == "__main__":
    pipeline_name = "users_per_city"
    dag = PipelineRunner().run(pipeline_name)
