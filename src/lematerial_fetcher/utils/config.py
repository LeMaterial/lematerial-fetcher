# Copyright 2025 Entalpic
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from lematerial_fetcher.utils.logging import logger

load_dotenv(override=True)


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
    max_offset: Optional[int] = None
    db_fetch_batch_size: Optional[int] = None
    mp_task_table_name: Optional[str] = None
    mysql_config: Optional[dict] = None


@dataclass
class PushConfig(BaseConfig):
    source_db_conn_str: str
    source_table_name: str
    hf_repo_id: str
    hf_token: str | None = None
    data_dir: str | None = None
    chunk_size: int = 1000
    max_rows: int = -1
    force_refresh: bool = False


def _load_base_config(
    log_dir: str = "./logs",
    max_retries: int = 3,
    num_workers: int = os.cpu_count() - 1,
    retry_delay: int = 2,
    log_every: int = 1000,
    offset: int = 0,
    limit: int = 10,
) -> Dict[str, Any]:
    """Load base configuration.

    All values are provided by Click with environment variable defaults already applied.
    """
    return {
        "log_dir": log_dir,
        "max_retries": max_retries,
        "num_workers": num_workers,
        "retry_delay": retry_delay,
        "log_every": log_every,
        "page_offset": offset,
        "page_limit": limit,
    }


def _create_db_conn_str(
    host: str = "localhost",
    user: Optional[str] = None,
    password_env_var: str = None,  # Password environment variable
    dbname: Optional[str] = None,
) -> str:
    """Create a database connection string from individual parameters.

    Passwords are always read from environment variables (unless passed in the connection string).
    """
    password = os.getenv(password_env_var) if password_env_var else None

    missing = []
    if not user:
        missing.append("database username")
    if not password:
        missing.append(f"password (environment variable {password_env_var})")
    if not dbname:
        missing.append("database name")

    if missing:
        raise ValueError(f"Required database credentials missing: {', '.join(missing)}")

    return (
        f"host={host} user={user} password={password} dbname={dbname} sslmode=disable"
    )


def _load_mysql_config(
    mysql_host: str = "localhost",
    mysql_user: Optional[str] = None,
    mysql_password_env_var: str = "LEMATERIALFETCHER_MYSQL_PASSWORD",  # Password environment variable
    mysql_database: str = "lematerial",
    mysql_cert_path: Optional[str] = None,
) -> dict:
    """Load MySQL configuration from parameters."""
    mysql_password = os.getenv(mysql_password_env_var)

    config = {
        "host": mysql_host,
        "user": mysql_user,
        "password": mysql_password,
        "database": mysql_database,
        "cert_path": mysql_cert_path,
    }

    # Only validate if MySQL appears to be configured
    if mysql_user:
        missing = []
        if not mysql_password:
            missing.append(
                f"MySQL password (environment variable {mysql_password_env_var})"
            )
        if not mysql_database:
            missing.append("MySQL database")

        if missing:
            raise ValueError(
                f"Required MySQL configuration missing: {', '.join(missing)}"
            )

    return config


def load_fetcher_config(
    base_url: Optional[str] = None,
    db_conn_str: Optional[str] = None,
    db_user: Optional[str] = None,
    # No password parameter - must be set in environment
    db_host: str = "localhost",
    db_name: Optional[str] = None,
    table_name: Optional[str] = None,
    mp_bucket_name: Optional[str] = None,
    mp_bucket_prefix: Optional[str] = None,
    oqmd_download_dir: Optional[str] = None,
    mysql_host: str = "localhost",
    mysql_user: Optional[str] = None,
    # No MySQL password parameter
    mysql_database: str = "lematerial",
    mysql_cert_path: Optional[str] = None,
    **base_config_kwargs: Any,
) -> FetcherConfig:
    """Loads fetcher config from passed arguments.

    The common workflow is that those arguments will be passed by Click.
    Passwords are always read from environment variables for security.
    """
    base_config = _load_base_config(**base_config_kwargs)

    if db_conn_str is None and db_user is not None:
        try:
            db_conn_str = _create_db_conn_str(
                host=db_host,
                user=db_user,
                password_env_var="LEMATERIALFETCHER_DB_PASSWORD",
                dbname=db_name,
            )
        except ValueError:
            # The validation will happen in the next step
            pass

    config = {
        "base_url": base_url,
        "table_name": table_name,
        "db_conn_str": db_conn_str,
        "mp_bucket_name": mp_bucket_name,
        "mp_bucket_prefix": mp_bucket_prefix,
        "oqmd_download_dir": oqmd_download_dir,
    }

    # Validate required fields
    required_fields = [
        ("base_url", "base_url"),
        ("table_name", "table_name"),
        ("db_conn_str", "db credentials"),
    ]
    missing_fields = [(label, key) for key, label in required_fields if not config[key]]
    if missing_fields:
        missing_labels = [label for label, _ in missing_fields]
        raise ValueError(f"Required configuration missing: {', '.join(missing_labels)}")

    # Handle MySQL config (OQMD only)
    try:
        mysql_config = _load_mysql_config(
            mysql_host=mysql_host,
            mysql_user=mysql_user,
            mysql_password_env_var="LEMATERIALFETCHER_MYSQL_PASSWORD",
            mysql_database=mysql_database,
            mysql_cert_path=mysql_cert_path,
        )
    except ValueError as e:
        logger.info(f"Error loading MySQL config: {e}.")
        mysql_config = None

    return FetcherConfig(
        **base_config,
        **config,
        mysql_config=mysql_config,
    )


def load_transformer_config(
    # Source database params
    db_conn_str: Optional[str] = None,
    db_user: Optional[str] = None,
    # No source password parameter
    db_host: str = "localhost",
    db_name: Optional[str] = None,
    table_name: Optional[str] = None,
    # Destination database params
    dest_db_user: Optional[str] = None,
    # No destination password parameter
    dest_db_host: str = "localhost",
    dest_db_name: Optional[str] = None,
    dest_table_name: Optional[str] = None,
    # Other params
    batch_size: int = 500,
    db_fetch_batch_size: Optional[int] = None,
    max_offset: Optional[int] = None,
    task_source_table_name: Optional[str] = None,
    mysql_host: str = "localhost",
    mysql_user: Optional[str] = None,
    mysql_database: str = "lematerial",
    mysql_cert_path: Optional[str] = None,
    **base_config_kwargs: Any,
) -> TransformerConfig:
    """Loads transformer config from passed arguments.

    The common workflow is that those arguments will be passed by Click.
    Passwords are always read from environment variables for security.

    If destination database credentials are not provided, the source database
    credentials will be used as fallbacks.
    """
    base_config = _load_base_config(**base_config_kwargs)

    # Create source DB connection string if credentials provided
    db_conn_str = None
    if db_user:
        try:
            db_conn_str = _create_db_conn_str(
                host=db_host,
                user=db_user,
                password_env_var="LEMATERIALFETCHER_DB_PASSWORD",
                dbname=db_name,
            )
        except ValueError:
            # The validation will happen later
            pass

    # source database credentials fallback
    dest_db_user_final = dest_db_user or db_user
    dest_db_host_final = dest_db_host if dest_db_user else db_host
    dest_db_name_final = dest_db_name or db_name

    password_env_var = (
        "LEMATERIALFETCHER_DEST_DB_PASSWORD"
        if dest_db_user
        else "LEMATERIALFETCHER_DB_PASSWORD"
    )

    dest_db_conn_str = None
    if dest_db_user_final:
        try:
            dest_db_conn_str = _create_db_conn_str(
                host=dest_db_host_final,
                user=dest_db_user_final,
                password_env_var=password_env_var,
                dbname=dest_db_name_final,
            )
        except ValueError:
            # The validation will happen later
            pass

    config = {
        "source_db_conn_str": db_conn_str,
        "dest_db_conn_str": dest_db_conn_str,
        "source_table_name": table_name,
        "dest_table_name": dest_table_name,
        "batch_size": batch_size,
        "db_fetch_batch_size": db_fetch_batch_size,
        "max_offset": max_offset,
        "mp_task_table_name": task_source_table_name,
    }

    # Validate required fields
    required_fields = [
        ("source_db_conn_str", "database credentials"),
        ("dest_db_conn_str", "destination database credentials"),
        ("source_table_name", "table_name"),
        ("dest_table_name", "dest_table_name"),
    ]
    missing_fields = [(label, key) for key, label in required_fields if not config[key]]
    if missing_fields:
        missing_labels = [label for label, _ in missing_fields]
        raise ValueError(
            f"Required transformer configuration missing: {', '.join(missing_labels)}"
        )

    # Handle MySQL config (only useful for OQMD currently)
    try:
        mysql_config = _load_mysql_config(
            mysql_host=mysql_host,
            mysql_user=mysql_user,
            mysql_password_env_var="LEMATERIALFETCHER_MYSQL_PASSWORD",
            mysql_database=mysql_database,
            mysql_cert_path=mysql_cert_path,
        )
    except ValueError as e:
        logger.info(f"Error loading MySQL config: {e}.")
        mysql_config = None

    return TransformerConfig(
        **base_config,
        **config,
        mysql_config=mysql_config,
    )


def load_push_config(
    db_conn_str: Optional[str] = None,
    db_user: Optional[str] = None,
    db_host: str = "localhost",
    db_name: Optional[str] = None,
    table_name: Optional[str] = None,
    hf_repo_id: Optional[str] = None,
    hf_token: Optional[str] = None,
    data_dir: Optional[str] = None,
    chunk_size: int = 1000,
    max_rows: int = -1,
    force_refresh: bool = False,
    **base_config_kwargs: Any,
) -> PushConfig:
    """Loads push config from passed arguments.

    The common workflow is that those arguments will be passed by Click.
    Passwords are always read from environment variables for security.
    """
    base_config = _load_base_config(**base_config_kwargs)

    if db_conn_str is None and db_user is not None:
        try:
            db_conn_str = _create_db_conn_str(
                host=db_host,
                user=db_user,
                password_env_var="LEMATERIALFETCHER_DB_PASSWORD",
                dbname=db_name,
            )
        except ValueError:
            pass

    config = {
        "source_db_conn_str": db_conn_str,
        "source_table_name": table_name,
        "hf_repo_id": hf_repo_id,
        "hf_token": hf_token,
        "data_dir": data_dir,
        "chunk_size": chunk_size,
        "max_rows": max_rows,
        "force_refresh": force_refresh,
    }

    required_fields = [
        ("source_db_conn_str", "db credentials"),
        ("source_table_name", "table_name"),
        ("hf_repo_id", "hf_repo_id"),
    ]
    missing_fields = [(label, key) for key, label in required_fields if not config[key]]
    if missing_fields:
        missing_labels = [label for label, _ in missing_fields]
        raise ValueError(
            f"Required push configuration missing: {', '.join(missing_labels)}"
        )

    return PushConfig(
        **base_config,
        **config,
    )
