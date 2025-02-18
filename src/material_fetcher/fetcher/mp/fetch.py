from concurrent.futures import ThreadPoolExecutor
from typing import List

from botocore.client import BaseClient

from material_fetcher.database.postgres import Database
from material_fetcher.fetcher.mp.utils import (
    add_s3_object_to_db,
    is_critical_error,
)
from material_fetcher.utils.aws import (
    get_aws_client,
    list_s3_objects,
)
from material_fetcher.utils.config import Config, load_config
from material_fetcher.utils.logging import logger


def fetch():
    """
    Fetch materials data from the Materials Project API and store it in the database.

    This function retrieves data from S3 buckets and stores it in PostgreSQL. It handles
    the entire pipeline from finding the latest collection version to parallel processing
    of S3 objects.

    Raises
    ------
    Exception
        If any error occurs during the fetching process.
    """
    try:
        cfg = load_config()
        aws_client = get_aws_client()

        # lists all objects in the bucket
        object_keys = list_s3_objects(
            aws_client, cfg.mp_bucket_name, cfg.mp_bucket_prefix
        )
        logger.info(f"Found {len(object_keys)} objects in bucket")

        db = Database(cfg.db_conn_str, cfg.table_name)
        db.create_table()

        process_s3_objects(db, aws_client, cfg, object_keys)

        logger.info("Successfully completed processing S3 objects")

    except Exception as e:
        logger.fatal(f"Error during fetch: {str(e)}")
        raise


def process_s3_objects(
    db: Database, client: BaseClient, cfg: Config, object_keys: List[str]
):
    """
    Coordinate the parallel processing of S3 objects using a thread pool.

    Parameters
    ----------
    db : Database
        Database instance for storing the processed data.
    client : BaseClient
        AWS client instance for S3 operations.
    cfg : Config
        Configuration object containing processing parameters.
    object_keys : List[str]
        List of S3 object keys to process.

    Raises
    ------
    Exception
        If a critical error occurs during processing.
    """
    # filter out non-JSONL files and manifests
    valid_keys = [
        key
        for key in object_keys
        if key.endswith(".jsonl.gz") and "manifest.jsonl.gz" not in key
    ]

    with ThreadPoolExecutor(max_workers=cfg.num_workers) as executor:
        futures = [
            executor.submit(worker, i, db, client, cfg.mp_bucket_name, key)
            for i, key in enumerate(valid_keys, 1)
        ]
        for future in futures:
            try:
                future.result()  # This will raise any exceptions that occurred
            except Exception as e:
                if is_critical_error(e):
                    logger.error(f"Critical error encountered: {str(e)}")
                    executor.shutdown(wait=False)
                    raise


def worker(
    worker_id: int, db: Database, client: BaseClient, bucket_name: str, object_key: str
):
    """
    Process a single S3 object in a worker thread.

    Parameters
    ----------
    worker_id : int
        Identifier for the worker thread.
    db : Database
        Database instance for storing the processed data.
    client : BaseClient
        AWS client instance for S3 operations.
    bucket_name : str
        Name of the S3 bucket.
    object_key : str
        Key of the S3 object to process.

    Raises
    ------
    Exception
        If a critical error occurs during processing.
    """
    logger.info(f"Worker {worker_id} processing file: {object_key}")
    try:
        add_s3_object_to_db(client, bucket_name, object_key, db)
    except Exception as e:
        logger.error(f"Worker {worker_id} error processing {object_key}: {str(e)}")
        # TODO(ramlaoui): is this still needed?
        if is_critical_error(e):
            raise


if __name__ == "__main__":
    fetch()
