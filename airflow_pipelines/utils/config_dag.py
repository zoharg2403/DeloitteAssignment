
from dataclasses import dataclass, asdict
from datetime import datetime as dt, timedelta


@dataclass
class ConfigDag:
    dag_id:          str
    schedule:        str
    start_date:      str | dt  # str will be converted to datetime object
    catchup:         bool
    max_active_runs: int
    retries:         int
    retry_delay:     str | int | timedelta  # str/int will be converted to timedelta object

    def __post_init__(self):
        # convert start_date str -> datetime object
        if not isinstance(self.start_date, dt):
            try:
                self.start_date = dt.strptime(self.start_date, "%d-%m-%Y")
            except ValueError:
                raise ValueError(f"Invalid start_date format: {self.start_date}. Expected format: dd-mm-yyyy")

        # convert retry_delay to timedelta
        if not isinstance(self.retry_delay, timedelta):
            try:
                self.retry_delay = timedelta(seconds=int(self.retry_delay))
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid retry_delay: {self.retry_delay}") from e

    def __getitem__(self, key):
        return getattr(self, key)
    
    def keys(self):
        return asdict(self).keys()


# if __name__ == "__main__":
#     from common.config import Config
#     ConfigDag(**Config().load("config/airflow_pipelines/pipelines.yaml").pipelines.users_per_city.dag)

