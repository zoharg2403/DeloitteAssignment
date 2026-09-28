from __future__ import annotations

import json
import os
import importlib.util
import pkgutil
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
        except KeyError as e:
            raise KeyError(f"'{self.__class__.__name__}' object has no key '{attr}'") from e

    def __str__(self) -> str:
        return json.dumps(self, indent=2, default=str)


class Config:
    """Load YAML files."""

    default_dotenv  = ".env"
    default_env     = "dev"
    default_env_cfg = "config/environments.yaml"
    _root: _Root   = None

    def __init__(self):
        load_dotenv(self.default_dotenv, override=True)
        self.environment = os.getenv("APP_ENV", self.default_env)

    @staticmethod
    def _read_yaml(path: Path | str) -> dict:
        path = Path(path)
        with path.open(encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}
        return values

    @staticmethod
    def _read_yaml_from_zip(path: Path | str) -> dict:
        path = Path(path)
        package_name = path.parts[0]
        resource_path = "/".join(path.parts[1:])
        try:
            binary_data = pkgutil.get_data(package_name, resource_path)
            assert binary_data is not None, FileNotFoundError
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Configuration file does not exist inside package '{package_name}': {resource_path}") from e
        raw_text = binary_data.decode("utf-8")
        values = yaml.safe_load(raw_text) or {}
        return values

    def _merge(self, values: _Root | dict):
        if self._root is None:
            return _Root(values)
        return _Root(self._root | values)

    def load(self, *filepaths: str | Path | tuple[str | Path]) -> Config:

        print("\n\n\n ################### load config file")  # TODO

        values = {}
        for filepath in filepaths:
            path = Path(filepath)
            try:
                if path.is_file():
                    print("path.is_file() == True") # TODO
                    values |= self._read_yaml(path)
                elif path.parts and path.parts[0].isidentifier() and importlib.util.find_spec(path.parts[0]) is not None:
                    print("_read_yaml_from_zip == True") # TODO
                    values |= self._read_yaml_from_zip(path)
                else:
                    raise FileNotFoundError
            except FileNotFoundError as e:
                raise FileNotFoundError(f"Configuration file does not exist locally or in an importable package: {path}") from e
            except TypeError as e:
                raise TypeError(f"Configuration file must contain a mapping: {path}")

        if not values or not isinstance(values, dict):
            raise TypeError(f"Configuration file(s) must contain a mapping: {filepaths}")

        self._root = self._merge(values)
        print("#########################\n\n\n") # TODO
        return self

    @property
    def env(self):
        if "environments" not in self._root:
            self.load(self.default_env_cfg)
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
    cfg.load("config/dataproc/deploy.yaml")
    # cfg.load("config/environments.yaml")
    cfg.env


