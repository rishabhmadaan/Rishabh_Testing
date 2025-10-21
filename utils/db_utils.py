from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager

from utils.config import redshift, mysql
from utils.logging_utils import get_logger

logger = get_logger("db_utils")


def _make_redshift_engine() -> Engine:
    if not redshift.host:
        raise RuntimeError("Redshift credentials missing. Set REDSHIFT_* environment variables.")
    url = (
        f"postgresql+psycopg2://{redshift.user}:{redshift.password}"
        f"@{redshift.host}:{redshift.port}/{redshift.database}?sslmode={redshift.sslmode}"
    )
    logger.info("Creating Redshift engine: %s", redshift.host)
    return create_engine(url, pool_pre_ping=True, pool_recycle=300, connect_args={"sslmode": redshift.sslmode})


def _make_mysql_engine() -> Engine:
    if not mysql.host:
        raise RuntimeError("MySQL credentials missing. Set MYSQL_* environment variables.")
    url = f"mysql+pymysql://{mysql.user}:{mysql.password}@{mysql.host}:{mysql.port}/{mysql.database}"
    logger.info("Creating MySQL engine: %s", mysql.host)
    return create_engine(url, pool_pre_ping=True, pool_recycle=300)


@contextmanager
def redshift_conn() -> Engine:
    engine = _make_redshift_engine()
    try:
        with engine.connect() as conn:
            yield conn
    finally:
        engine.dispose()


@contextmanager
def mysql_conn() -> Engine:
    engine = _make_mysql_engine()
    try:
        with engine.connect() as conn:
            yield conn
    finally:
        engine.dispose()


def run_query(conn, sql: str, params: Optional[dict] = None):
    logger.info("Running query...")
    return conn.execute(text(sql), params or {})
