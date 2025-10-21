import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.environ.get(key, default)


@dataclass
class Paths:
    allocation_file: str = _env(
        "ALLOCATION_DIR",
        "/Users/rishabhmadaan/Documents/Elev8/Collections_allocation/Allocation_Oct.xlsx",
    )
    payments_dir: str = _env(
        "PAYMENTS_DIR", "/Users/rishabhmadaan/Documents/Collections/Oct Payment"
    )
    output_dir: str = _env("OUTPUT_DIR", os.path.join(os.getcwd(), "output"))


@dataclass
class RedshiftConfig:
    host: str = _env("REDSHIFT_HOST", "")
    port: int = int(_env("REDSHIFT_PORT", "5439"))
    database: str = _env("REDSHIFT_DB", "")
    user: str = _env("REDSHIFT_USER", "")
    password: str = _env("REDSHIFT_PASSWORD", "")
    sslmode: str = _env("REDSHIFT_SSLMODE", "require")


@dataclass
class MySQLConfig:
    host: str = _env("MYSQL_HOST", "")
    port: int = int(_env("MYSQL_PORT", "3306"))
    database: str = _env("MYSQL_DB", "")
    user: str = _env("MYSQL_USER", "")
    password: str = _env("MYSQL_PASSWORD", "")


@dataclass
class RunWindow:
    # Default: current month
    start_date: date = date(date.today().year, date.today().month, 1)
    end_date: date = date.today()


paths = Paths()
redshift = RedshiftConfig()
mysql = MySQLConfig()
run_window = RunWindow()

os.makedirs(paths.output_dir, exist_ok=True)
