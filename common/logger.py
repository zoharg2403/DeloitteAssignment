import logging
from datetime import datetime as dt
from pathlib import Path

from common.config import Config

class Logger:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.cfg      = Config().logger
        self.filepath = self._get_filepath() if self.cfg.to_file else None
        self.logger   = self._init_logger()

    def _get_filepath(self) -> Path:
        dir_ = Path(self.cfg.logs_dir)
        dir_.mkdir(parents=True, exist_ok=True)
        ts = dt.now().isoformat(timespec="seconds").replace(':', '-')
        filename = f"{self.cfg.name}__{ts}.log"
        return dir_ / filename

    def _init_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.cfg.name)
        logger.propagate = False
        logger.setLevel(self.cfg.level)

        formatter = logging.Formatter(self.cfg.format)

        if self.cfg.to_stream:
            h = logging.StreamHandler()
            h.setLevel(self.cfg.level)
            h.setFormatter(formatter)
            logger.addHandler(h)

        if self.cfg.to_file:
            h = logging.FileHandler(filename=self.filepath)
            h.setLevel(self.cfg.level)
            h.setFormatter(formatter)
            logger.addHandler(h)

        return logger

    def __getattr__(self, attr: str):
        return getattr(self.logger, attr)


if __name__ == "__main__":
    logger = Logger()
    logger.info("Example")
