
import json
import os
import importlib.util
import pkgutil
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
    default_env = "dev"
    _root: _Root
    
    def __init__(self):
        load_dotenv(self.default_dotenv, override=True)
        self.environment = os.getenv("APP_ENV", self.default_env)

    @staticmethod
    def _read_yaml(path: Path | str) -> dict:
        path = Path(path)
        with path.open(encoding="utf-8") as file:
            values = yaml.safe_load(file) or {}
        if not isinstance(values, dict):
            raise TypeError(f"Configuration file must contain a mapping: {path}")
        return values

    @staticmethod
    def _read_yaml_from_zip(path: Path | str) -> dict:
        path = Path(path)
        package_name = path.parts[0]
        resource_path = "/".join(path.parts[1:])
        try:
            binary_data = pkgutil.get_data(package_name, resource_path)
        except FileNotFoundError as error:
            raise FileNotFoundError(f"Configuration file does not exist inside package '{package_name}': {resource_path}") from error
        if binary_data is None:
            raise FileNotFoundError(f"Configuration file does not exist inside package '{package_name}': {resource_path}")
        raw_text = binary_data.decode("utf-8")
        values = yaml.safe_load(raw_text) or {}
        return values

    @classmethod
    def load(cls, filepath: str | Path) -> "Config":
        path = Path(filepath)
        if path.is_file():
            values = cls._read_yaml(path)
        elif path.parts and path.parts[0].isidentifier() and importlib.util.find_spec(path.parts[0]) is not None:
            values = cls._read_yaml_from_zip(path)
        else:
            raise FileNotFoundError(f"Configuration file does not exist locally or in an importable package: {path}")

        config = cls()
        config._root = _Root(values)
        return config

    @property
    def env(self):
        return getattr(self._root.env, self.environment)

    def __getattr__(self, attr: str) -> Any:
        try:
            return getattr(self._root, attr)
        except AttributeError as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'") from e

    def __str__(self) -> str:
        return "Config:\n" + json.dumps(self._root, indent=2, default=str)


if __name__ == "__main__":
    cfg = Config.load("dataproc/dataproc.yaml")


