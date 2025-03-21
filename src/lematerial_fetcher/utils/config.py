# Copyright 2025 Entalpic
import functools
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dotenv import load_dotenv


@dataclass
class BaseConfig:
    log_dir: str
    max_retries: int
    num_workers: int
    retry_delay: int
    log_every: int


@dataclass
class FetcherConfig(BaseConfig):
    base_url: str
    db_conn_str: str
    table_name: str
    page_limit: int
    page_offset: int
    mp_bucket_name: str
    mp_bucket_prefix: str
    mysql_config: Optional[dict] = None
    oqmd_download_dir: Optional[str] = None


@dataclass
class TransformerConfig(BaseConfig):
    source_db_conn_str: str
    dest_db_conn_str: str
    source_table_name: str
    dest_table_name: str
    page_offset: int
    batch_size: int
    mp_task_table_name: Optional[str] = None
    mysql_config: Optional[dict] = None


@dataclass
class PushConfig(BaseConfig):
    source_db_conn_str: str
    source_table_name: str
    hf_repo_id: str
    data_dir: str | None = None
    chunk_size: int = 1000


def _load_base_config() -> Dict[str, Any]:
    defaults = {
        "LEMATERIALFETCHER_LOG_DIR": "./logs",
        "LEMATERIALFETCHER_MAX_RETRIES": 3,
        "LEMATERIALFETCHER_NUM_WORKERS": 2,
        "LEMATERIALFETCHER_RETRY_DELAY": 2,
        "LEMATERIALFETCHER_LOG_EVERY": 1000,
        "LEMATERIALFETCHER_PAGE_OFFSET": 0,
    }

    # apply defaults
    for key, value in defaults.items():
        if key not in os.environ:
            os.environ[key] = str(value)

    return {
        "log_dir": os.getenv("LEMATERIALFETCHER_LOG_DIR"),
        "max_retries": int(os.getenv("LEMATERIALFETCHER_MAX_RETRIES")),
        "num_workers": int(os.getenv("LEMATERIALFETCHER_NUM_WORKERS")),
        "retry_delay": int(os.getenv("LEMATERIALFETCHER_RETRY_DELAY")),
        "log_every": int(os.getenv("LEMATERIALFETCHER_LOG_EVERY")),
    }


def _create_db_conn_str(user_env: str, password_env: str, dbname_env: str) -> str:
    """Create a database connection string from environment variables."""
    return (
        f"user={os.getenv(user_env)} "
        f"password={os.getenv(password_env)} "
        f"dbname={os.getenv(dbname_env)} "
        f"sslmode=disable"
    )


def _load_mysql_config() -> dict:
    return {
        "host": os.getenv("LEMATERIALFETCHER_MYSQL_HOST"),
        "user": os.getenv("LEMATERIALFETCHER_MYSQL_USER"),
        "password": os.getenv("LEMATERIALFETCHER_MYSQL_PASSWORD"),
        "database": os.getenv("LEMATERIALFETCHER_MYSQL_DATABASE"),
        "cert_path": os.getenv("LEMATERIALFETCHER_MYSQL_CERT_PATH"),
    }


def load_fetcher_config() -> FetcherConfig:
    load_dotenv(override=True)

    # check required variables
    required_vars = [
        "LEMATERIALFETCHER_API_BASE_URL",
        "LEMATERIALFETCHER_TABLE_NAME",
        "LEMATERIALFETCHER_MP_BUCKET_NAME",
        "LEMATERIALFETCHER_MP_BUCKET_PREFIX",
    ]

    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} is not set")

    db_conn_str = _create_db_conn_str(
        "LEMATERIALFETCHER_DB_USER",
        "LEMATERIALFETCHER_DB_PASSWORD",
        "LEMATERIALFETCHER_DB_NAME",
    )

    base_config = _load_base_config()

    mysql_config = _load_mysql_config()

    return FetcherConfig(
        **base_config,
        base_url=os.getenv("LEMATERIALFETCHER_API_BASE_URL"),
        db_conn_str=db_conn_str,
        table_name=os.getenv("LEMATERIALFETCHER_TABLE_NAME"),
        page_limit=int(os.getenv("LEMATERIALFETCHER_PAGE_LIMIT", "10")),
        page_offset=int(os.getenv("LEMATERIALFETCHER_PAGE_OFFSET")),
        mp_bucket_name=os.getenv("LEMATERIALFETCHER_MP_BUCKET_NAME"),
        mp_bucket_prefix=os.getenv("LEMATERIALFETCHER_MP_BUCKET_PREFIX"),
        oqmd_download_dir=os.getenv("LEMATERIALFETCHER_OQMD_DOWNLOAD_DIR"),
        mysql_config=mysql_config,
    )


def load_transformer_config() -> TransformerConfig:
    load_dotenv(override=True)

    required_vars = [
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME",
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER",
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD",
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_TABLE_NAME",
        "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_NAME",
        "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_USER",
        "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD",
        "LEMATERIALFETCHER_TRANSFORMER_DEST_TABLE_NAME",
        "LEMATERIALFETCHER_TRANSFORMER_BATCH_SIZE",
        "LEMATERIALFETCHER_TRANSFORMER_LOG_EVERY",
    ]

    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} is not set")

    source_db_conn_str = _create_db_conn_str(
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER",
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD",
        "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME",
    )

    dest_db_conn_str = _create_db_conn_str(
        "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_USER",
        "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD",
        "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_NAME",
    )

    base_config = _load_base_config()

    mysql_config = _load_mysql_config()

    return TransformerConfig(
        **base_config,
        source_db_conn_str=source_db_conn_str,
        dest_db_conn_str=dest_db_conn_str,
        source_table_name=os.getenv("LEMATERIALFETCHER_TRANSFORMER_SOURCE_TABLE_NAME"),
        dest_table_name=os.getenv("LEMATERIALFETCHER_TRANSFORMER_DEST_TABLE_NAME"),
        batch_size=int(os.getenv("LEMATERIALFETCHER_TRANSFORMER_BATCH_SIZE", "1000")),
        page_offset=int(os.getenv("LEMATERIALFETCHER_TRANSFORMER_PAGE_OFFSET", "0")),
        mp_task_table_name=os.getenv(
            "LEMATERIALFETCHER_TRANSFORMER_TASK_TABLE_NAME", None
        ),
        mysql_config=mysql_config,
    )


def load_push_config() -> PushConfig:
    load_dotenv(override=True)

    base_config = _load_base_config()

    return functools.partial(PushConfig, **base_config)
