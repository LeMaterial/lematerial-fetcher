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
    page_offset: int
    page_limit: int


@dataclass
class FetcherConfig(BaseConfig):
    base_url: str
    db_conn_str: str
    table_name: str
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
    batch_size: int
    mp_task_table_name: Optional[str] = None
    mysql_config: Optional[dict] = None


def _load_base_config(
    log_dir: Optional[str] = None,
    max_retries: Optional[int] = None,
    num_workers: Optional[int] = None,
    retry_delay: Optional[int] = None,
    log_every: Optional[int] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    mapping_order = {
        "LEMATERIALFETCHER_LOG_DIR": (log_dir, "./logs"),
        "LEMATERIALFETCHER_MAX_RETRIES": (max_retries, 3),
        "LEMATERIALFETCHER_NUM_WORKERS": (num_workers, 2),
        "LEMATERIALFETCHER_RETRY_DELAY": (retry_delay, 2),
        "LEMATERIALFETCHER_LOG_EVERY": (log_every, 1000),
        "LEMATERIALFETCHER_OFFSET": (offset, 0),
        "LEMATERIALFETCHER_LIMIT": (limit, 10),
    }

    # apply defaults
    for key, value in mapping_order.items():
        if key not in os.environ:
            if value[0] is None:
                os.environ[key] = str(value[1])
            else:
                os.environ[key] = str(value[0])

    return {
        "log_dir": os.getenv("LEMATERIALFETCHER_LOG_DIR"),
        "max_retries": int(os.getenv("LEMATERIALFETCHER_MAX_RETRIES")),
        "num_workers": int(os.getenv("LEMATERIALFETCHER_NUM_WORKERS")),
        "retry_delay": int(os.getenv("LEMATERIALFETCHER_RETRY_DELAY")),
        "log_every": int(os.getenv("LEMATERIALFETCHER_LOG_EVERY")),
        "page_offset": int(os.getenv("LEMATERIALFETCHER_PAGE_OFFSET")),
        "page_limit": int(os.getenv("LEMATERIALFETCHER_PAGE_LIMIT")),
    }


def _create_db_conn_str(
    host_env: str, user_env: str, password_env: str, dbname_env: str
) -> str:
    """Create a database connection string from environment variables."""
    host = os.getenv(host_env)
    if host is None:
        host = "localhost"
    return (
        f"host={host} "
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


def load_fetcher_config(
    base_url: Optional[str] = None,
    db_conn_str: Optional[str] = None,
    table_name: Optional[str] = None,
    mp_bucket_name: Optional[str] = None,
    mp_bucket_prefix: Optional[str] = None,
    oqmd_download_dir: Optional[str] = None,
    mysql_config: Optional[dict] = None,
    **base_config_kwargs: Any,
) -> FetcherConfig:
    """Loads fetcher config from environment variables or passed arguments if they are set."""
    load_dotenv(override=True)

    # check required variables
    required_vars = {
        "base_url": (base_url, "LEMATERIALFETCHER_API_BASE_URL"),
        "table_name": (table_name, "LEMATERIALFETCHER_TABLE_NAME"),
        "db_conn_str": (
            db_conn_str,
            [
                "LEMATERIALFETCHER_DB_USER",
                "LEMATERIALFETCHER_DB_PASSWORD",
                "LEMATERIALFETCHER_DB_NAME",
            ],
        ),
    }

    config_vars = {}

    for var, value in required_vars.items():
        if value[0] is None:
            assert os.getenv(value[1]) is not None, (
                f"{value[1]} is not set, you need to provide it or set it as an environment variable"
            )
            config_vars[var] = os.getenv(value[1])
        else:
            config_vars[var] = value[0]

    if db_conn_str is None:
        db_conn_str = _create_db_conn_str(
            "LEMATERIALFETCHER_DB_HOST",
            "LEMATERIALFETCHER_DB_USER",
            "LEMATERIALFETCHER_DB_PASSWORD",
            "LEMATERIALFETCHER_DB_NAME",
        )

    base_config = _load_base_config(**base_config_kwargs)
    mysql_config = _load_mysql_config() if mysql_config is None else mysql_config

    return FetcherConfig(
        **base_config,
        db_conn_str=db_conn_str,
        base_url=config_vars["base_url"],
        table_name=config_vars["table_name"],
        mp_bucket_name=mp_bucket_name,
        mp_bucket_prefix=mp_bucket_prefix,
        oqmd_download_dir=oqmd_download_dir,
        mysql_config=mysql_config,
    )


def load_transformer_config(
    db_conn_str: Optional[str] = None,
    source_table_name: Optional[str] = None,
    dest_table_name: Optional[str] = None,
    batch_size: Optional[int] = None,
    task_source_table_name: Optional[str] = None,
    mysql_config: Optional[dict] = None,
    **base_config_kwargs: Any,
) -> TransformerConfig:
    load_dotenv(override=True)

    required_vars = {
        "source_db_conn_str": (
            db_conn_str,
            [
                "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME",
                "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER",
                "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD",
            ],
        ),
        "source_table_name": (
            source_table_name,
            "LEMATERIALFETCHER_TRANSFORMER_SOURCE_TABLE_NAME",
        ),
        "dest_db_conn_str": (
            db_conn_str,
            [
                "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_NAME",
                "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_USER",
                "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD",
            ],
        ),
        "dest_table_name": (
            dest_table_name,
            "LEMATERIALFETCHER_TRANSFORMER_DEST_TABLE_NAME",
        ),
        "batch_size": (batch_size, "LEMATERIALFETCHER_TRANSFORMER_BATCH_SIZE"),
    }

    config_vars = {}

    for var, value in required_vars.items():
        if value[0] is None:
            test_vars = [value[1]] if not isinstance(value[1], list) else value[1]
            for env_var in test_vars:
                assert os.getenv(env_var) is not None, (
                    f"{env_var} is not set, you need to provide it or set it as an environment variable"
                )
            config_vars[var] = (
                os.getenv(test_vars[0]) if not isinstance(value[1], list) else None
            )
        else:
            config_vars[var] = value[0]

    if config_vars["source_db_conn_str"] is None:
        config_vars["source_db_conn_str"] = _create_db_conn_str(
            "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_HOST",
            "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_USER",
            "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_PASSWORD",
            "LEMATERIALFETCHER_TRANSFORMER_SOURCE_DB_NAME",
        )

    if config_vars["dest_db_conn_str"] is None:
        config_vars["dest_db_conn_str"] = _create_db_conn_str(
            "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_HOST",
            "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_USER",
            "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_PASSWORD",
            "LEMATERIALFETCHER_TRANSFORMER_DEST_DB_NAME",
        )

    base_config = _load_base_config(**base_config_kwargs)

    mysql_config = _load_mysql_config() if mysql_config is None else mysql_config

    return TransformerConfig(
        **base_config,
        source_db_conn_str=config_vars["source_db_conn_str"],
        dest_db_conn_str=config_vars["dest_db_conn_str"],
        source_table_name=config_vars["source_table_name"],
        dest_table_name=config_vars["dest_table_name"],
        batch_size=int(config_vars["batch_size"]),
        mp_task_table_name=task_source_table_name
        if task_source_table_name is not None
        else os.getenv("LEMATERIALFETCHER_TRANSFORMER_TASK_TABLE_NAME", None),
        mysql_config=mysql_config,
    )


def load_push_config() -> PushConfig:
    load_dotenv(override=True)

    base_config = _load_base_config()

    defaults_kwargs = {
        "chunk_size": int(os.getenv("LEMATERIALFETCHER_PUSH_CHUNK_SIZE", "1000")),
        "max_rows": int(os.getenv("LEMATERIALFETCHER_PUSH_MAX_ROWS", -1)),
        "force_refresh": bool(os.getenv("LEMATERIALFETCHER_PUSH_FORCE_REFRESH", False)),
    }

    conn_str = _create_db_conn_str(
        "LEMATERIALFETCHER_PUSH_DB_USER",
        "LEMATERIALFETCHER_PUSH_DB_PASSWORD",
        "LEMATERIALFETCHER_PUSH_DB_NAME",
    )

    return functools.partial(
        PushConfig,
        **base_config,
        **defaults_kwargs,
        source_db_conn_str=conn_str,
    )
