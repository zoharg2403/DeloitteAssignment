
from dataclasses import dataclass, field, asdict
from datetime import datetime as dt


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
