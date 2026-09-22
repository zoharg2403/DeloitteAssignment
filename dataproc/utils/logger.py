import logging
from datetime import datetime as dt
from pathlib import Path
from pydantic import BaseModel, Field, model_validator

from dataproc.utils.config import Config


class ConfigLogger(BaseModel):
    name:      str        = Field("applog", min_length=1, description="Logger name")
    level:     str        = Field("INFO", min_length=1, description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    format:    str        = Field("%(asctime)s - %(name)s - %(levelname)s - %(message)s", min_length=1, description="Logging format")
    logs_dir:  str | None = Field(None, description="dir path to save logs")
    to_stream: bool       = Field(True, description="Toggle on (True) / off (False) logging.StreamHandler()")
    to_file:   bool       = Field(True, description="Toggle on (True) / off (False) logging.FileHandler()")

    @model_validator(mode="after")
    def validate(self):
        if self.to_file and not self.logs_dir:
            raise ValueError("logs_dir must be set when to_file is True")
        return self


class Logger:

    def __init__(self):
        self.cfg = ConfigLogger(**Config().logger)
        existing_logger = logging.getLogger(self.cfg.name)
        self.filepath = (
            self._get_filepath()
            if self.cfg.to_file and not existing_logger.handlers
            else None
        )
        self.logger = self._init_logger()

    def _get_filepath(self):
        dir_ = Path(self.cfg.logs_dir)
        dir_.mkdir(parents=True, exist_ok=True)
        filename = f"{__name__}__{dt.now().isoformat().replace(':', '-')}.log"
        return dir_ / filename

    def _init_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.cfg.name)
        logger.setLevel(self.cfg.level.upper())
        logger.propagate = False

        if logger.handlers:
            return logger

        formatter = logging.Formatter(self.cfg.format)

        if self.cfg.to_stream:
            h = logging.StreamHandler()
            h.setLevel(self.cfg.level.upper())
            h.setFormatter(formatter)
            logger.addHandler(h)

        if self.cfg.to_file:
            h = logging.FileHandler(filename=self.filepath)
            h.setLevel(self.cfg.level.upper())
            h.setFormatter(formatter)
            logger.addHandler(h)

        return logger

    def __getattr__(self, attr: str):
        try:
            return getattr(self.logger, attr)
        except AttributeError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")


if __name__ == "__main__":
    Logger().info("Logger is configured")