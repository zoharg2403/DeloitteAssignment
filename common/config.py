
import json
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


class _Root(dict[str, Any]):
    def __init__(self, data: dict[str, Any]):
        super().__init__(
            {
                k: _Root(v) if isinstance(v, dict) else v
                for k, v in data.items()
            }
        )

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError as e:
            raise KeyError(f"'{self.__class__.__name__}' object has no key '{attr}'") from e

    def __str__(self) -> str:
        return json.dumps(self, indent=2, default=str)


class Config:
    """Load the selected environment and component YAML files."""

    default_dotenv = ".env"
    default_env = 'dev'
    default_config_dir = 'configs'

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(
            self, 
            dotenv_filepath: str | Path | None = None,
            environment: str | None = None, 
            config_dir: str | Path | None = None
            ):
        load_dotenv(dotenv_filepath or self.default_dotenv, override=True)
        self.environment = environment or os.getenv("APP_ENV", self.default_env)
        self.config_dir = self._config_dir(config_dir or self.default_config_dir)
        self._root = self._load_base_config()

    def _config_dir(self, config_dir: str | Path | None) -> Path:
        path = Path(config_dir)
        if not path.is_dir():
            raise FileNotFoundError(f"Configuration directory does not exist: {path}")
        return path

    @staticmethod
    def _read_yaml(path: Path):
        if not path.is_file():
            raise FileNotFoundError(f"Configuration file does not exist: {path}")
        with path.open(encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}
        if not isinstance(values, dict):
            raise TypeError(f"Configuration file must contain a mapping: {path}")
        return values

    def _load_base_config(self) -> _Root:
        return _Root({
            **self._read_yaml(self.config_dir / "project.yaml"),
            **self._read_yaml(self.config_dir / "environments" / f"{self.environment}.yaml")
            })

    def _merge(self, yaml_filepath: Path) -> None:
        self._root = _Root({
            **self._root, 
            **self._read_yaml(yaml_filepath)
            })

    @property
    def dataproc_deploy(self):
        if "dataproc_deploy" not in self._root:
            filepath = self.config_dir / "dataproc" / "deploy.yaml"
            self._merge(filepath)
        return self._root.dataproc_deploy

    def job(self, job_name: str):
        return _Root(self._read_yaml(self.config_dir / "dataproc" / "jobs" / f"{job_name}.yaml"))

    def pipeline(self, pipe_name: str):
        return _Root(self._read_yaml(self.config_dir / "pipelines" / f"{pipe_name}.yaml"))

    def __getattr__(self, attr: str) -> Any:
        try:
            return getattr(self._root, attr)
        except AttributeError as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'") from e

    def __str__(self) -> str:
        return "Config:\n" + json.dumps(self._root, indent=2, default=str)


if __name__ == "__main__":
    cfg = Config()
    cfg.logger
    cfg.dataproc_deploy

