from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar
import yaml
from dotenv import load_dotenv


class _Root(dict[str, Any]):
    """Dictionary whose keys can also be accessed as attributes."""

    def __init__(self, data: Mapping[str, Any]):
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
    """Load and merge YAML configuration for the selected application environment."""

    default_dotenv:  ClassVar[str] = ".env"
    default_env:     ClassVar[str] = "dev"
    default_env_cfg: ClassVar[str] = "config/environments.yaml"

    def __init__(self):
        load_dotenv(self.default_dotenv, override=True)
        self.environment = os.getenv("APP_ENV", self.default_env)
        
        self._root = None

    @staticmethod
    def _read_yaml(path: Path | str) -> dict[str, Any]:
        path = Path(path)
        with path.open(encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}
        if not isinstance(values, dict):
            raise TypeError("YAML document root must be a mapping")
        return values

    def _merge(self, values: Mapping[str, Any] | _Root) -> _Root:
        if self._root is None:
            return _Root(values)
        return _Root(self._root | values)

    def load(self, *filepaths: str | Path) -> Config:
        """Load and merge the given YAML files, failing if any file is invalid or missing."""
        values = {}
        for filepath in filepaths:
            path = Path(filepath)
            try:
                values |= self._read_yaml(path)
            except FileNotFoundError as e:
                raise FileNotFoundError(f"Configuration file does not exist: {path}") from e
            except TypeError as e:
                raise TypeError(f"Configuration file must contain a mapping: {path}") from e

        if not values or not isinstance(values, dict):
            raise TypeError(f"Configuration file(s) must contain a mapping: {filepaths}")

        self._root = self._merge(values)
        return self

    @property
    def env(self) -> _Root:
        if self._root is None or "environments" not in self._root:
            if Path(self.default_env_cfg).is_file():
                self.load(self.default_env_cfg)
            else:
                base_dir = Path(__file__).parent.parent
                filepath = base_dir / Path(self.default_env_cfg)
                if filepath.is_file():
                    self.load(filepath)
                else:
                    raise FileNotFoundError(f"Couldn't find env file in '{self.default_env_cfg}' or '{filepath}'")

        if self._root is None:
            raise RuntimeError("Environment configuration was not loaded")
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
    cfg.load("config/deploy/dataproc.yaml", "config/environments.yaml")
    cfg.env
