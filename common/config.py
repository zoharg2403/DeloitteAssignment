
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
    default_config = 'config.yaml'

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        load_dotenv(self.default_dotenv, override=True)
        self.environment = os.getenv("APP_ENV", self.default_env)
        self._root = self._load_base_config()

    @staticmethod
    def _read_yaml(path: Path | str):
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(f"Configuration file does not exist: {path}")
        with path.open(encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}
        if not isinstance(values, dict):
            raise TypeError(f"Configuration file must contain a mapping: {path}")
        return values

    def _load_base_config(self) -> _Root:
        return _Root(self._read_yaml(self.default_config))

    @property
    def env(self):
        return getattr(self._root.env, self.environment)

    @property
    def dataproc_deploy(self):
        cfg_yaml = Path("deploy") / "dataproc" / "deploy.yaml"
        return _Root(self._read_yaml(cfg_yaml))



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

