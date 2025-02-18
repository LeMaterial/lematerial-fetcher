import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class Config:
    base_url: str
    db_conn_str: str
    log_dir: str
    table_name: str
    max_retries: int
    num_workers: int
    page_limit: int
    retry_delay: int
    mp_bucket_name: str
    mp_bucket_prefix: str


def load_config() -> Optional[Config]:
    load_dotenv()

    defaults = {
        "MATERIALFETCHER_LOG_DIR": "./logs",
        "MAX_RETRIES": 3,
        "NUM_WORKERS": 2,
        "PAGE_LIMIT": 10,
        "RETRY_DELAY": 2,
    }

    # apply defaults
    for key, value in defaults.items():
        if key not in os.environ:
            os.environ[key] = str(value)

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

    # create database connection string
    db_conn_str = (
        f"user={os.getenv('MATERIALFETCHER_DB_USER')} "
        f"password={os.getenv('MATERIALFETCHER_DB_PASSWORD')} "
        f"dbname={os.getenv('MATERIALFETCHER_DB_NAME')} "
        f"sslmode=disable"
    )

    return Config(
        base_url=os.getenv("MATERIALFETCHER_API_BASE_URL"),
        db_conn_str=db_conn_str,
        log_dir=os.getenv("MATERIALFETCHER_LOG_DIR"),
        table_name=os.getenv("MATERIALFETCHER_TABLE_NAME"),
        max_retries=int(os.getenv("MAX_RETRIES")),
        num_workers=int(os.getenv("NUM_WORKERS")),
        page_limit=int(os.getenv("PAGE_LIMIT")),
        retry_delay=int(os.getenv("RETRY_DELAY")),
        mp_bucket_name=os.getenv("MATERIALFETCHER_MP_BUCKET_NAME"),
        mp_bucket_prefix=os.getenv("MATERIALFETCHER_MP_BUCKET_PREFIX"),
    )
