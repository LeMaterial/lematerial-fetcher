# Copyright 2025 Entalpic
import gzip
import json

from botocore.client import BaseClient

from material_fetcher.database.postgres import Database
from material_fetcher.model.models import RawStructure
from material_fetcher.utils.logging import logger


def add_s3_object_to_db(
    aws_client: BaseClient, bucket_name: str, object_key: str, db: Database
):
    """
    Process a single S3 object and add it to the database.
    This assumes that the S3 object is a JSONL file that is compressed into a gzip file.

    Parameters
    ----------
    aws_client : BaseClient
        AWS client instance for S3 operations.
    bucket_name : str
        Name of the S3 bucket to download the object from. (e.g. "materialsproject-build")
    object_key : str
        Key of the S3 object to process (e.g. "collections/2025-02-12/materials/nelements=2/symmetry_number=208.jsonl.gz")
    db : Database
        Database instance for storing the data.
    """
    logger.info(f"Starting to process: {object_key}")

    # download and process the S3 object
    response = aws_client.get_object(Bucket=bucket_name, Key=object_key)
    with gzip.GzipFile(fileobj=response["Body"]) as gzipped_file:
        add_jsonl_file_to_db(gzipped_file, db)

    logger.info(f"Completed processing: {object_key}")


def add_jsonl_file_to_db(gzipped_file, db: Database):
    """
    Read a JSONL file line by line and add its contents to the database.
    This assumes that the JSONL file is compressed into a gzip file.

    Parameters
    ----------
    gzipped_file : file object
        A gzipped file object containing JSONL data.
    db : Database
        Database instance for storing the processed data.

    Notes
    -----
    Progress is logged every 1000 records.
    Failed records are logged but do not stop the processing.
    """
    processed = 0

    for line in gzipped_file:
        processed += 1
        try:
            data = json.loads(line)

            if "material_id" not in data:
                # this is a task
                structure = RawStructure(
                    id=data["task_id"], type="mp-task", attributes=data
                )
            else:
                # create a proper Structure instance
                structure = RawStructure(
                    id=data["material_id"], type="mp-material", attributes=data
                )

            db.insert_data(structure)

            if processed % 1000 == 0:
                logger.info(f"Processed {processed} records")

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON line: {e}")
            continue
        except Exception as e:
            logger.warning(f"Failed to insert data: {e}")
            continue

    logger.info(f"Completed processing {processed} records")


def is_critical_error(error: Exception) -> bool:
    """
    Determine if the error should stop all processing.

    Parameters
    ----------
    error : Exception
        The error to evaluate.

    Returns
    -------
    bool
        True if the error is critical, False otherwise.
    """
    if error is None:
        return False

    error_str = str(error).lower()
    critical_conditions = ["connection refused", "no such host", "connection reset"]
    return any(condition in error_str for condition in critical_conditions)
