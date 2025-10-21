import logging
import os
from datetime import datetime

LOGS_DIR = os.environ.get("LOGS_DIR", os.path.join(os.getcwd(), "logs"))
os.makedirs(LOGS_DIR, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch.setFormatter(ch_formatter)

    # File handler per day per script
    ts = datetime.now().strftime("%Y%m%d")
    fh_path = os.path.join(LOGS_DIR, f"{name}_{ts}.log")
    fh = logging.FileHandler(fh_path)
    fh.setLevel(logging.INFO)
    fh_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(fh_formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False
    return logger
