# Copyright 2025 Entalpic
import gzip
import json
from collections import defaultdict
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
    "PBEsol": Functional.PBESOL,
    "r2SCAN": Functional.r2SCAN,
    "SCAN": Functional.SCAN,
}


class TaskType(Enum):
    STRUCTURE_OPTIMIZATION = "Structure Optimization"
    STATIC = "Static"
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


def extract_static_structure_optimization_tasks(
    raw_structure: RawStructure,
    source_db: StructuresDatabase,
    task_table_name: str,
    extract_static: bool = True,
    fallback_to_static: bool = False,
) -> tuple[dict[str, RawStructure], dict[str, str]]:
    """
    Extract non deprecated structure optimization and static tasks from a raw Materials Project structure.

    This function retrieves the structure optimization and static tasks from the task table
    and returns them as a list of OptimadeStructure objects.

    Parameters
    ----------
    raw_structure : RawStructure
        The raw Materials Project structure to extract tasks from.
    source_db : StructuresDatabase
        The source database instance to read from.
    task_table_name : str
        The name of the task table to read from.
    extract_static : bool
        Whether to extract static tasks.
    fallback_to_static : bool
        Whether to fallback to static tasks if no structure optimization tasks are found.

    Returns
    -------
    tuple[dict[str, RawStructure], dict[str, str]]
        A tuple of two dictionaries:
        - The first dictionary maps task IDs to RawStructure objects.
        - The second dictionary maps task IDs to the calculation type.
    """
    include_list = [TaskType.STRUCTURE_OPTIMIZATION.value]
    if extract_static:
        include_list.append(TaskType.STATIC.value)

    # This means that the raw structure is a material
    if "task_types" in raw_structure.attributes:
        static_and_structure_optimization_tasks = [
            mp_id
            for mp_id, task_type in raw_structure.attributes["task_types"].items()
            if task_type in include_list
        ]
    else:
        raise ValueError(
            "Invalid raw structure type: "
            + raw_structure.type
            + ". Expected 'task_types' in the attributes."
        )

    non_deprecated_task_ids = [
        mp_id
        for mp_id in static_and_structure_optimization_tasks
        if mp_id not in raw_structure.attributes["deprecated_tasks"]
    ]

    # If no non-deprecated tasks are found, fallback to static tasks
    if not non_deprecated_task_ids and fallback_to_static:
        non_deprecated_task_ids = [
            mp_id
            for mp_id, task_type in raw_structure.attributes["task_types"].items()
            if mp_id not in raw_structure.attributes["deprecated_tasks"]
            and task_type == TaskType.STATIC.value
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

    functional = task_calc_type.split(" ")[0]  # Extracts the functional
    if functional in MP_FUNCTIONAL_MAPPING:
        return MP_FUNCTIONAL_MAPPING[functional]
    else:
        return task_calc_type


def map_tasks_to_functionals(
    tasks: list[RawStructure],
    task_calc_types: dict[str, str],
    keep_all_calculations: bool = False,
) -> dict[str, RawStructure | list[RawStructure]]:
    """
    Map tasks to functionals, selecting the most appropriate task for each functional.

    We follow the Materials Project strategy for selecting the most appropriate
    task for each functional [1]:
    For most functionals, we
    - Only include non-deprecated tasks (valid calculations)
    - Prefer a static calculation over a structure optimization
    - We pick the structure with the lowest energy output
    For PBE, GGA+U is preferred over GGA regardless of energy value.

    Parameters
    ----------
    tasks : List[RawStructure]
        List of task structures to process
    task_calc_types : dict[str, str]
        Dictionary mapping task IDs to calculation types
    keep_all_calculations : bool
        Whether to keep all calculations or only the most appropriate one
        per material. This is useful for extracting trajectories.

    Returns
    -------
    Dict[str, RawStructure]
        Dictionary mapping functional names to selected task

    References
    ----------
    [1] https://github.com/materialsproject/emmet/blob/682277da9f11af40073d5a4fa6b306fda9a1d582/emmet-core/emmet/core/vasp/material.py#L109
    """
    functional_tasks = defaultdict(list)

    for task_id, calc_type in task_calc_types.items():
        functional = calc_type.split(" ")[0]  # Extracts the functional
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
    # Except for trajectories, where we take both
    # and let the filtering decide which steps to keep
    if "GGA+U" in functional_tasks:
        if keep_all_calculations:
            functional_tasks[Functional.PBE].extend(functional_tasks["GGA+U"])
        else:
            functional_tasks[Functional.PBE] = functional_tasks["GGA+U"]

    def _static_lowest_energy(task: RawStructure) -> RawStructure:
        parameters = task.attributes["input"]["parameters"]

        tags_score = sum(
            (parameters.get(tag, False) if parameters else False)
            for tag in ["LASPH", "ISPIN"]
        )

        return (
            -int(task.attributes["task_type"] == TaskType.STATIC.value),
            -tags_score,
            task.attributes["output"]["energy"] / task.attributes["nsites"],
        )

    selected_tasks = {}
    for functional, task_list in functional_tasks.items():
        sorted_tasks = sorted(task_list, key=_static_lowest_energy)

        if keep_all_calculations:
            selected_tasks[functional] = sorted_tasks
        else:
            selected_tasks[functional] = sorted_tasks[0]

    return selected_tasks
