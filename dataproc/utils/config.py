import yaml
import json
from typing import Any
from dotenv import load_dotenv


class _Root(dict[str, Any]):

    def __init__(self, data: dict[str, Any]):
        super().__init__(
            {
                key: _Root(value) if isinstance(value, dict) else value
                for key, value in data.items()
            }
        )

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __repr__(self) -> str:
        return dict.__repr__(self)

    def __str__(self) -> str:
        return json.dumps(self, indent=2, default=str)

    def as_dict(self) -> dict[str, Any]:
        return {
            key: value.as_dict() if isinstance(value, _Root) else value
            for key, value in self.items()
        }


class Config:

    _default_config_filepath = "config.yaml"
    _default_dotenv_filepath = ".env"

    def __init__(self, cfg_filepath: str | None = None, dotenv_filepath: str | None = None):
        self.config_filepath = cfg_filepath or self._default_config_filepath
        self.dotenv_filepath = dotenv_filepath or self._default_dotenv_filepath

        self._root = None
        self._load_dotenv()
        self._load_config()

    @property
    def root(self):
        return self._root

    def _load_dotenv(self):
        load_dotenv(self.dotenv_filepath, override=True)

    def _load_config(self):
        with open(self.config_filepath, 'r') as f:
            yaml_data = yaml.safe_load(f)
        self._root = _Root(yaml_data or {})

    def __getattr__(self, name):
        try:
            value = getattr(self._root, name)
        except (KeyError, TypeError) as exc:
            raise AttributeError(name) from exc
        return value

    def __repr__(self) -> str:
        return f"Config(root={self._root!r})"
        
    def __str__(self) -> str:
        return json.dumps(self._root.as_dict(), indent=2, default=str)

    def as_dict(self) -> dict[str, Any]:
        return self._root.as_dict()


if __name__ == "__main__":
    # Usage example:
    cfg = Config()
    cfg.spark           # >> {'app_name': 'DeloitteAssignment'}
    cfg.spark.app_name  # >> 'DeloitteAssignment'
