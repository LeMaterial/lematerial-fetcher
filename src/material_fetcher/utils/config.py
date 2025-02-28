# Copyright 2025 Entalpic
import os
from dataclasses import dataclass
from typing import Any, Dict

from dotenv import load_dotenv


@dataclass
class BaseConfig:
    log_dir: str
    max_retries: int
    num_workers: int
    retry_delay: int


@dataclass
class FetcherConfig(BaseConfig):
    base_url: str
    db_conn_str: str
    table_name: str
    page_limit: int
    mp_bucket_name: str
    mp_bucket_prefix: str


@dataclass
class TransformerConfig(BaseConfig):
    source_db_conn_str: str
    dest_db_conn_str: str
    source_table_name: str
    dest_table_name: str
    batch_size: int


def _load_base_config() -> Dict[str, Any]:
    defaults = {
        "MATERIALFETCHER_LOG_DIR": "./logs",
        "MATERIALFETCHER_MAX_RETRIES": 3,
        "MATERIALFETCHER_NUM_WORKERS": 2,
        "MATERIALFETCHER_PAGE_LIMIT": 10,
        "MATERIALFETCHER_RETRY_DELAY": 2,
    }

    # apply defaults
    for key, value in defaults.items():
        if key not in os.environ:
            os.environ[key] = str(value)

    return {
        "log_dir": os.getenv("MATERIALFETCHER_LOG_DIR"),
        "max_retries": int(os.getenv("MAX_RETRIES")),
        "num_workers": int(os.getenv("NUM_WORKERS")),
        "retry_delay": int(os.getenv("RETRY_DELAY")),
    }


def _create_db_conn_str(user_env: str, password_env: str, dbname_env: str) -> str:
    """Create a database connection string from environment variables."""
    return (
        f"user={os.getenv(user_env)} "
        f"password={os.getenv(password_env)} "
        f"dbname={os.getenv(dbname_env)} "
        f"sslmode=disable"
    )


def load_fetcher_config() -> FetcherConfig:
    load_dotenv()

    # check required variables
    required_vars = [
        "MATERIALFETCHER_API_BASE_URL",
        "MATERIALFETCHER_TABLE_NAME",
        "MATERIALFETCHER_MP_BUCKET_NAME",
        "MATERIALFETCHER_MP_BUCKET_PREFIX",
    ]

    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} is not set")

    db_conn_str = _create_db_conn_str(
        "MATERIALFETCHER_DB_USER",
        "MATERIALFETCHER_DB_PASSWORD",
        "MATERIALFETCHER_DB_NAME",
    )

    base_config = _load_base_config()

    return FetcherConfig(
        **base_config,
        base_url=os.getenv("MATERIALFETCHER_API_BASE_URL"),
        db_conn_str=db_conn_str,
        table_name=os.getenv("MATERIALFETCHER_TABLE_NAME"),
        page_limit=int(os.getenv("MATERIALFETCHER_PAGE_LIMIT", "10")),
        mp_bucket_name=os.getenv("MATERIALFETCHER_MP_BUCKET_NAME"),
        mp_bucket_prefix=os.getenv("MATERIALFETCHER_MP_BUCKET_PREFIX"),
    )


def load_transformer_config() -> TransformerConfig:
    load_dotenv()

    required_vars = [
        "MATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME",
        "MATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER",
        "MATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD",
        "MATERIALFETCHER_TRANSFORMER_SOURCE_TABLE_NAME",
        "MATERIALFETCHER_TRANSFORMER_DEST_DB_NAME",
        "MATERIALFETCHER_TRANSFORMER_DEST_DB_USER",
        "MATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD",
        "MATERIALFETCHER_TRANSFORMER_DEST_TABLE_NAME",
        "MATERIALFETCHER_TRANSFORMER_BATCH_SIZE",
    ]

    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"{var} is not set")

    source_db_conn_str = _create_db_conn_str(
        "MATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER",
        "MATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD",
        "MATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME",
    )

    dest_db_conn_str = _create_db_conn_str(
        "MATERIALFETCHER_TRANSFORMER_DEST_DB_USER",
        "MATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD",
        "MATERIALFETCHER_TRANSFORMER_DEST_DB_NAME",
    )

    base_config = _load_base_config()

    return TransformerConfig(
        **base_config,
        source_db_conn_str=source_db_conn_str,
        dest_db_conn_str=dest_db_conn_str,
        source_table_name=os.getenv("MATERIALFETCHER_TRANSFORMER_SOURCE_TABLE_NAME"),
        dest_table_name=os.getenv("MATERIALFETCHER_TRANSFORMER_DEST_TABLE_NAME"),
        batch_size=int(os.getenv("MATERIALFETCHER_TRANSFORMER_BATCH_SIZE", "1000")),
    )
