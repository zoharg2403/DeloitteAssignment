
from dataclasses import dataclass, asdict
from datetime import datetime as dt

@dataclass
class ConfigDag:
    dag_id:          str
    schedule:        str
    start_date:      str
    catchup:         bool
    max_active_runs: int

    def __post_init__(self):
        # convert start_date str -> datetime object
        try:
            self.start_date = dt.strptime(self.start_date, "%d-%m-%Y")
        except ValueError:
            raise ValueError(f"Invalid start_date format: {self.start_date}. Expected format: dd-mm-yyyy")

    def __getitem__(self, key):
        return getattr(self, key)
    
    def keys(self):
        return asdict(self).keys()

if __name__ == "__main__":
    from common.config import Config

    pipeline_name = "users_per_city"
    
    cfg = Config().load("config/airflow/pipelines.yaml")
    cfg_pipeline = getattr(cfg.pipelines, pipeline_name)
    cfg_dag = ConfigDag(**cfg_pipeline.dag)
    pass
