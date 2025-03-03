from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from material_fetcher.database.postgres import OptimadeDatabase, StructuresDatabase
from material_fetcher.model.optimade import OptimadeStructure
from material_fetcher.utils.config import TransformerConfig, load_transformer_config
from material_fetcher.utils.logging import logger


def transform(
    transform_fn: Callable[[dict], OptimadeStructure],
) -> None:
    """
    Transform data from a source database table into OptimadeStructures and store in a target table.

    Parameters
    ----------
    transform_fn : Callable[[dict], OptimadeStructure]
        Function that takes a dictionary of raw data and returns an OptimadeStructure

    Raises
    ------
    Exception
        If any error occurs during the transformation process
    """
    try:
        cfg = load_transformer_config()

        source_db = StructuresDatabase(cfg.source_db_conn_str, cfg.source_table_name)
        target_db = OptimadeDatabase(cfg.dest_db_conn_str, cfg.dest_table_name)

        target_db.create_table()

        process_rows(source_db, target_db, transform_fn, cfg)

        logger.info("Successfully completed transforming database records")

    except Exception as e:
        logger.fatal(f"Error during transform: {str(e)}")
        raise


def process_rows(
    source_db: StructuresDatabase,
    target_db: OptimadeDatabase,
    transform_fn: Callable[[dict], OptimadeStructure],
    cfg: TransformerConfig,
) -> None:
    """
    Process rows from source database in parallel, transform them, and store in target database.
    Processes rows in batches to avoid memory issues.

    Parameters
    ----------
    source_db : StructuresDatabase
        Source database instance to read from
    target_db : OptimadeDatabase
        Target database instance to write to
    transform_fn : Callable[[dict], OptimadeStructure]
        Function to transform raw data into OptimadeStructure
    cfg : TransformerConfig
        Configuration object containing processing parameters

    Raises
    ------
    Exception
        If a critical error occurs during processing
    """
    with ThreadPoolExecutor(max_workers=cfg.num_workers) as executor:
        batch_size = cfg.batch_size
        offset = 0
        total_processed = 0

        while True:
            # get batch of rows from source table
            rows = source_db.fetch_items(offset=offset, batch_size=batch_size)
            if not rows:
                break

            total_processed += len(rows)
            logger.info(
                f"Processing batch of {len(rows)} rows (total processed: {total_processed})"
            )

            # submit transformation tasks for this batch
            futures = [
                executor.submit(
                    worker,
                    total_processed - len(rows) + i,
                    row,
                    transform_fn,
                    target_db,
                    cfg.log_every,
                )
                for i, row in enumerate(rows, 1)
            ]

            # wait for batch to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Critical error encountered: {str(e)}")
                    executor.shutdown(wait=False)
                    raise

            offset += batch_size

        logger.info(f"Completed processing {total_processed} total rows")


def worker(
    worker_id: int,
    row: dict,
    transform_fn: Callable[[dict], OptimadeStructure],
    target_db: OptimadeDatabase,
    log_every: int = 1000,
) -> None:
    """
    Transform a single row and store it in the target database.

    Parameters
    ----------
    worker_id : int
        Identifier for the worker thread
    row : dict
        Raw data row from source database
    transform_fn : Callable[[dict], OptimadeStructure]
        Function to transform raw data into OptimadeStructure
    target_db : OptimadeDatabase
        Target database instance to write to

    Raises
    ------
    Exception
        If an error occurs during transformation or database insertion
    """
    try:
        # transform the row into an OptimadeStructure
        optimade_structure = transform_fn(row)

        target_db.insert_data(optimade_structure)

        if worker_id % log_every == 0:
            logger.info(f"Transformed {worker_id} records")

    except Exception as e:
        logger.error(f"Worker {worker_id} error processing row: {str(e)}")
        raise
