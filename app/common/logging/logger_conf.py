import sys
from pathlib import Path

from loguru import logger


def configure_logger(log_format: str, log_level: str, log_path: Path) -> None:

    logger.remove()
    logger.add(sys.stderr, format=log_format, level=log_level, colorize=True)
    logger.add(
        log_path,
        format=log_format,
        level="INFO",
    )
