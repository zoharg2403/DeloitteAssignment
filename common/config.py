from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
import yaml
from dotenv import load_dotenv


class _Root(dict[str, Any]):
    def __init__(self, data: dict[str, Any] | _Root):
        super().__init__(
            {
                k: _Root(v) if isinstance(v, dict) else v
                for k, v in data.items()
            }
        )

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except (KeyError, AttributeError) as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'") from e

    def __str__(self) -> str:
        return json.dumps(self, indent=2, default=str)


class Config:
    """Load YAML files."""
    
    default_dotenv   = ".env"
    default_env      = "dev"
    default_env_cfg  = "config/environments.yaml"
    _root: _Root     = None

    def __init__(self):
        load_dotenv(self.default_dotenv, override=True)
        self.environment = os.getenv("APP_ENV", self.default_env)

    @staticmethod
    def _read_yaml(path: Path | str) -> dict:
        path = Path(path)
        with path.open(encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}
        return values

    def _merge(self, values: _Root | dict):
        if self._root is None:
            return _Root(values)
        return _Root(self._root | values)

    def load(self, *filepaths: str | Path | tuple[str | Path]) -> Config:

        values = {}
        for filepath in filepaths:
            path = Path(filepath)
            if path.is_file():
                try:
                    values |= self._read_yaml(path)
                except FileNotFoundError as e:
                    raise FileNotFoundError(f"Configuration file does not exist locally or in an importable package: {path}") from e
                except TypeError as e:
                    raise TypeError(f"Configuration file must contain a mapping: {path}")

        if not values or not isinstance(values, dict):
            raise TypeError(f"Configuration file(s) must contain a mapping: {filepaths}")

        self._root = self._merge(values)
        return self

    @property
    def env(self):
        if "environments" not in self._root:
            if Path(self.default_env_cfg).is_file():
                self.load(self.default_env_cfg)
            else:
                base_dir = Path(__file__).parent.parent
                filepath = base_dir / Path(self.default_env_cfg)
                if filepath.is_file():
                    self.load(filepath)
                else:
                    raise FileNotFoundError(f"Couldn't find env file in '{self.default_env_cfg}' or '{filepath}'")

        return getattr(self._root.environments, self.environment)


    def __getattr__(self, attr: str) -> Any:
        try:
            return getattr(self._root, attr)
        except AttributeError as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'") from e

    def __str__(self) -> str:
        return "Config:\n" + json.dumps(self._root, indent=2, default=str)


if __name__ == "__main__":
    cfg = Config()
    cfg.load("config/deploy/dataproc.yaml")
    # cfg.load("config/environments.yaml")
    cfg.env


