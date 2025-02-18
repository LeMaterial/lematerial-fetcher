from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from material_fetcher.database.postgres import Database, new_db
from material_fetcher.model.models import APIResponse, Structure
from material_fetcher.utils.config import Config, load_config
from material_fetcher.utils.logging import logger


def fetch():
    """
    Fetch materials data from the Alexandria API and store it in the database.

    This function coordinates the entire data fetching pipeline, from setting up the
    database connection to managing parallel workers for data retrieval.

    Raises
    ------
    Exception
        If any critical error occurs during the fetching process.
    """
    try:
        cfg = load_config()
        db = new_db(cfg.db_conn_str, cfg.table_name)
        db.create_table()

        logger.info(
            f"Starting data fetch from Alexandria API with {cfg.num_workers} workers"
        )
        process_data(db, cfg)
        logger.info("Data fetch completed successfully")

    except Exception as e:
        logger.fatal(f"Error during fetch: {str(e)}")
        raise


def process_data(db: Database, cfg: Config):
    """
    Coordinate parallel processing of API data using a thread pool.

    Parameters
    ----------
    db : Database
        Database instance for storing the processed data.
    cfg : Config
        Configuration object containing processing parameters.
    """
    with ThreadPoolExecutor(max_workers=cfg.num_workers) as executor:
        futures = []
        offset = 0

        while True:
            future = executor.submit(
                worker,
                db,
                cfg.base_url,
                offset,
                cfg.page_limit,
                cfg.max_retries,
                cfg.retry_delay,
            )
            futures.append(future)
            offset += cfg.page_limit

            # check if we've reached the end of data
            try:
                if not future.result():
                    break
            except Exception as e:
                logger.error(f"Error in worker: {str(e)}")
                executor.shutdown(wait=False)
                raise

        # wait for remaining futures to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in worker: {str(e)}")
                executor.shutdown(wait=False)
                raise


def worker(
    db: Database,
    base_url: str,
    offset: int,
    limit: int,
    max_retries: int,
    retry_delay: int,
) -> bool:
    """
    Process a batch of data from the API in a worker thread.

    Parameters
    ----------
    db : Database
        Database instance for storing the processed data.
    base_url : str
        Base URL of the API.
    offset : int
        Starting offset for the data batch.
    limit : int
        Maximum number of records to fetch.
    max_retries : int
        Maximum number of retry attempts.
    retry_delay : int
        Delay between retry attempts in seconds.

    Returns
    -------
    bool
        True if data was processed, False if no data was available.

    Raises
    ------
    Exception
        If a critical error occurs during processing.
    """
    session = create_session(max_retries, retry_delay)

    try:
        response = fetch_data(session, base_url, offset, limit)
        if not response.data:
            return False

        for structure in response.data:
            try:
                db.insert_data(structure)
            except Exception as e:
                logger.warning(f"Error inserting data: {str(e)}")
                continue

        logger.info(
            f"Successfully processed {len(response.data)} records at offset {offset}"
        )
        return True

    except Exception as e:
        logger.error(f"Error fetching data at offset {offset}: {str(e)}")
        raise


def create_session(max_retries: int, retry_delay: int) -> requests.Session:
    """
    Create a requests session with retry configuration.

    Parameters
    ----------
    max_retries : int
        Maximum number of retry attempts.
    retry_delay : int
        Delay between retry attempts in seconds.

    Returns
    -------
    requests.Session
        Configured session object.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=retry_delay,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_data(
    session: requests.Session, base_url: str, offset: int, limit: int
) -> Optional[APIResponse]:
    """
    Fetch a batch of data from the API.

    Parameters
    ----------
    session : requests.Session
        Session object for making HTTP requests.
    base_url : str
        Base URL of the API.
    offset : int
        Starting offset for the data batch.
    limit : int
        Maximum number of records to fetch.

    Returns
    -------
    Optional[APIResponse]
        API response object containing the fetched data.

    Raises
    ------
    Exception
        If the API request fails.
    """
    url = f"{base_url}?page_limit={limit}&sort=id&page_offset={offset}"

    response = session.get(url)
    response.raise_for_status()

    data = response.json()
    structures = [
        Structure(id=item["id"], type=item["type"], attributes=item["attributes"])
        for item in data["data"]
    ]

    return APIResponse(data=structures, links=data.get("links", {}))
