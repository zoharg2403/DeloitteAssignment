
from dataclasses import dataclass, asdict
from datetime import datetime as dt, timedelta
from typing import Any


@dataclass
class ConfigDag:
    dag_id:          str
    schedule:        str
    start_date:      str | dt  # str will be converted to datetime object
    catchup:         bool
    max_active_runs: int
    default_args:    dict[str, Any] | None = None

    def __post_init__(self):
        # convert start_date str -> datetime object
        if not isinstance(self.start_date, dt):
            try:
                self.start_date = dt.strptime(self.start_date, "%d-%m-%Y")
            except ValueError:
                raise ValueError(f"Invalid start_date format: {self.start_date}. Expected format: dd-mm-yyyy")

        # Convert retry_delay from seconds to timedelta for Airflow.
        self.default_args = self.default_args or {}
        self.default_args.setdefault("retries", 0)
        self.default_args.setdefault("retry_delay", timedelta(seconds=300))
        if not isinstance(self.default_args["retry_delay"], timedelta):
            try:
                self.default_args["retry_delay"] = timedelta(seconds=int(self.default_args["retry_delay"]))
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid retry_delay: {self.default_args["retry_delay"]}") from e

    def __getitem__(self, key):
        return getattr(self, key)
    
    def keys(self):
        return asdict(self).keys()


# if __name__ == "__main__":
#     from common.config import Config
#     tst = ConfigDag(**Config().load("config/airflow_pipelines/pipelines.yaml").pipelines.users_per_city.dag)
#     print(tst.default_args)
