"""Lightweight logging utilities for the CLI entrypoints."""
import logging
from typing import Optional


def setup_logging(level: int = logging.INFO) -> None:
    """Configure basic logging if not already configured."""

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
    else:
        logging.getLogger().setLevel(level)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger with optional name after ensuring baseline config."""

    setup_logging()
    return logging.getLogger(name)
