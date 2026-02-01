"""Application logging helpers."""

import logging
import sys
from logging import Logger

from app.core.config import settings


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging() -> None:
    """Configure root logging handlers."""
    formatter = logging.Formatter(LOG_FORMAT)

    root_logger = logging.getLogger()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    root_logger.setLevel(level)

    if not any(isinstance(handler, logging.StreamHandler) for handler in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    if settings.environment == "production" and not any(
        isinstance(handler, logging.FileHandler) for handler in root_logger.handlers
    ):
        file_handler = logging.FileHandler("app.log")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> Logger:
    """Return a logger instance."""
    return logging.getLogger(name)