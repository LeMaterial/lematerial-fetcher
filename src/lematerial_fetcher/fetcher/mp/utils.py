# Copyright 2025 Entalpic
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from botocore.client import BaseClient

from lematerial_fetcher.database.postgres import Database, StructuresDatabase
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional
from lematerial_fetcher.utils.logging import logger

MP_FUNCTIONAL_MAPPING = {
    "GGA": Functional.PBE,
    "GGA+U": Functional.PBE,
    "PBESol": Functional.PBESOL,
    "SCAN": Functional.SCAN,
}


class TaskType(Enum):
    STRUCTURE_OPTIMIZATION = "Structure Optimization"
    DEPRECATED = "Deprecated"


def add_s3_object_to_db(
    aws_client: BaseClient,
    bucket_name: str,
    object_key: str,
    db: Database,
    log_every: int = 1000,
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
        add_jsonl_file_to_db(gzipped_file, db, log_every)

    logger.info(f"Completed processing: {object_key}")


def add_jsonl_file_to_db(gzipped_file, db: Database, log_every: int = 1000):
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
    structures = []

    for line in gzipped_file:
        processed += 1
        try:
            data = json.loads(line)

            last_modified = data.get("last_updated", {}).get("$date", None)

            if "material_id" not in data:
                # this is a task
                structure = RawStructure(
                    id=data["task_id"],
                    type="mp-task",
                    attributes=data,
                    last_modified=last_modified,
                )
            else:
                # create a proper Structure instance
                structure = RawStructure(
                    id=data["material_id"],
                    type="mp-material",
                    attributes=data,
                    last_modified=last_modified,
                )

            structures.append(structure)

            if processed % log_every == 0:
                logger.info(f"Processed {processed} records")
                # Insert batch when we hit the log_every threshold
                if structures:
                    db.batch_insert_data(structures)
                    structures = []

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON line: {e}")
            continue
        except Exception as e:
            logger.warning(f"Failed to insert data: {e}")
            continue

    # Insert any remaining structures
    if structures:
        db.batch_insert_data(structures)

    logger.info(f"Completed processing {processed} records")


def extract_structure_optimization_tasks(
    raw_structure: RawStructure, source_db: StructuresDatabase, task_table_name: str
) -> tuple[dict[str, RawStructure], dict[str, str]]:
    """
    Extract non deprecated structure optimization tasks from a raw Materials Project structure.

    This function retrieves the structure optimization tasks from the task table
    and returns them as a list of OptimadeStructure objects.

    Parameters
    ----------
    raw_structure : RawStructure
        The raw Materials Project structure to extract tasks from.
    source_db : StructuresDatabase
        The source database instance to read from.
    task_table_name : str
        The name of the task table to read from.

    Returns
    -------
    tuple[dict[str, RawStructure], dict[str, str]]
        A tuple of two dictionaries:
        - The first dictionary maps task IDs to RawStructure objects.
        - The second dictionary maps task IDs to the calculation type.
    """

    # This means that the raw structure is a material
    if "task_types" in raw_structure.attributes:
        structure_optimization_tasks = [
            mp_id
            for mp_id, task_type in raw_structure.attributes["task_types"].items()
            if task_type == TaskType.STRUCTURE_OPTIMIZATION.value
        ]
    else:
        raise ValueError(
            "Invalid raw structure type: "
            + raw_structure.type
            + ". Expected 'task_types' in the attributes."
        )

    non_deprecated_task_ids = [
        mp_id
        for mp_id in structure_optimization_tasks
        if mp_id not in raw_structure.attributes["deprecated_tasks"]
    ]
    calc_types = {
        mp_id: raw_structure.attributes["calc_types"][mp_id]
        for mp_id in non_deprecated_task_ids
    }

    tasks = source_db.fetch_items_with_ids(non_deprecated_task_ids, task_table_name)
    tasks = {task.id: task for task in tasks}

    return tasks, calc_types


def map_task_to_functional(
    task: RawStructure, task_calc_type: Optional[str] = None
) -> Functional | str:
    """
    Map a task to a functional by looking at the calculation type.

    Parameters
    ----------
    task : RawStructure
        The task to map to a functional.
    task_calc_type : Optional[str]
        The calculation type of the task.

    Returns
    -------
    Functional | str
        The functional mapped to the task, or the calculation type if no functional is found.
    """
    if task_calc_type is None:
        task_calc_type = task.attributes["calc_type"]

    functional = task_calc_type.split(" " + TaskType.STRUCTURE_OPTIMIZATION.value)[0]
    if functional in MP_FUNCTIONAL_MAPPING:
        return MP_FUNCTIONAL_MAPPING[functional]
    else:
        return task_calc_type


def map_tasks_to_functionals(
    tasks: list[RawStructure], task_calc_types: dict[str, str]
) -> dict[str, RawStructure]:
    """
    Map tasks to functionals, selecting the most appropriate task for each functional.

    For most functionals, the most recent task is selected.
    For PBE, GGA+U is preferred over GGA regardless of date.

    Parameters
    ----------
    tasks : List[RawStructure]
        List of task structures to process

    Returns
    -------
    Dict[str, RawStructure]
        Dictionary mapping functional names to selected task
    """
    functional_tasks = defaultdict(list)

    for task_id, calc_type in task_calc_types.items():
        functional = calc_type.split(" " + TaskType.STRUCTURE_OPTIMIZATION.value)[0]
        if task_id not in tasks:
            logger.warning(
                f"Task {task_id} was not found in your tasks databases, "
                + "this task will be ignored"
            )
            continue

        if functional in MP_FUNCTIONAL_MAPPING:
            if functional == Functional.PBE and calc_type == "GGA+U":
                functional_tasks["GGA+U"].append(tasks[task_id])
            else:
                functional_tasks[MP_FUNCTIONAL_MAPPING[functional]].append(
                    tasks[task_id]
                )

    # For PBE, prefer GGA+U over GGA
    if "GGA+U" in functional_tasks:
        functional_tasks[Functional.PBE] = functional_tasks["GGA+U"]

    selected_tasks = {}

    for functional, task_list in functional_tasks.items():
        selected_task = select_most_recent_task(task_list)

        if selected_task:
            selected_tasks[functional] = selected_task

    return selected_tasks


def select_most_recent_task(tasks: list[RawStructure]) -> Optional[RawStructure]:
    """
    Select the most recent task from a list of tasks.

    Parameters
    ----------
    tasks : List[RawStructure]
        List of tasks to choose from

    Returns
    -------
    Optional[RawStructure]
        The most recent task, or None if no valid tasks
    """
    if not tasks:
        return None

    latest_task = None
    latest_date = datetime.min.replace(tzinfo=timezone.utc)

    for task in tasks:
        # Extract the completion date from task attributes
        date_info = task.attributes.get("last_updated", {})
        date_str = date_info.get("$date", "")
        if not date_str:
            continue

        try:
            # Parse the date string from format: '2016-09-16T06:29:25Z'
            task_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if task_date > latest_date:
                latest_date = task_date
                latest_task = task
        except (ValueError, TypeError):
            logger.warning(f"Could not parse date '{date_str}' for task {task.id}")

    return latest_task if latest_task else None
