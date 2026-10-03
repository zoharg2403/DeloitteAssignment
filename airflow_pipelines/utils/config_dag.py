
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, fields
from datetime import datetime as dt, timedelta
from typing import Any

@dataclass
class DefaultArgs(Mapping[str, Any]):
    retries:                   int                          = 0
    retry_delay:               str | int | timedelta        = timedelta(seconds=5*60)  # 5 minutes [sec]
    retry_exponential_backoff: bool                         = False
    max_retry_delay:           str | int | timedelta | None = None                     # [sec]

    def __post_init__(self):
        # Convert retry_delay from seconds (str/int) to timedelta for Airflow
        if not isinstance(self.retry_delay, timedelta):
            try:
                self.retry_delay = timedelta(seconds = int(self.retry_delay))
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid retry_delay: {self.retry_delay}") from e
        
        # Convert max_retry_delay (if exists) from seconds (str/int) to timedelta for Airflow
        # When backoff is disabled, this parameter has no effect
        # There is an underlying global environment cap configured via AIRFLOW__CORE__MAX_TASK_RETRY_DELAY, which defaults to 24 hours
        if self.retry_exponential_backoff and self.max_retry_delay and not isinstance(self.max_retry_delay, timedelta):
            try:
                self.max_retry_delay = timedelta(seconds = int(self.max_retry_delay))
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid max_retry_delay: {self.max_retry_delay}") from e

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None

    def __iter__(self) -> Iterator[str]:
        return iter(field.name for field in fields(self))

    def __len__(self) -> int:
        return len(fields(self))


@dataclass
class ConfigDag(Mapping[str, Any]):
    dag_id:          str
    schedule:        str
    start_date:      str | dt  # str will be converted to datetime object
    catchup:         bool
    max_active_runs: int
    default_args:    dict[str, Any] | DefaultArgs | None = None

    def __post_init__(self):
        # convert start_date str -> datetime object
        if not isinstance(self.start_date, dt):
            try:
                self.start_date = dt.strptime(self.start_date, "%d-%m-%Y")
            except ValueError:
                raise ValueError(f"Invalid start_date format: {self.start_date}. Expected format: dd-mm-yyyy")

        self.default_args = DefaultArgs(**self.default_args)

    def __getitem__(self, key: str) -> Any:
        try:
            value = getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None
        if key == "default_args" and isinstance(value, DefaultArgs):
            return dict(value)
        return value

    def __iter__(self) -> Iterator[str]:
        return iter(field.name for field in fields(self))

    def __len__(self) -> int:
        return len(fields(self))


# if __name__ == "__main__":
#     from common.config import Config
#     tst = ConfigDag(**Config().load("config/airflow_pipelines/pipelines.yaml").pipelines.users_per_city.dag)
#     print(tst.default_args)
#     print({**tst})
