# Copyright 2025 Entalpic
import os
import re
import tempfile
from datetime import datetime
from typing import Optional

from lematerial_fetcher.database.mysql import MySQLDatabase, execute_sql_file
from lematerial_fetcher.utils.io import (
    download_file,
    get_page_content,
    list_download_links_from_page,
)
from lematerial_fetcher.utils.logging import logger


def download_and_process_oqmd_sql(
    db_config: dict,
    download_page_url: str = "https://oqmd.org/download/",
    download_dir: str = None,
) -> None:
    """
    Download and process the OQMD SQL database file.

    Parameters
    ----------
    db_config : dict
        Database configuration with keys: host, user, password, database
    download_dir : str, optional
        Directory for temporary files if needed. If None, uses a temporary directory
    """

    # Get the latest available version URL and the update date
    latest_url, modification_date = get_latest_sql_file_url_from_oqmd(
        download_page_url=download_page_url
    )

    db = MySQLDatabase(**db_config)

    try:
        db.connect()

        # Check if we have a stored version and if URLs match we can skip the download
        version_db_config = db_config.copy()
        version_db_config["database"] = f"{db_config['database']}_version"
        version_db = MySQLDatabase(**version_db_config)

        version_db.create_database()
        current_url, current_modification_date = get_oqmd_version_if_exists(version_db)

        if current_url == latest_url and current_modification_date == modification_date:
            logger.info(
                "OQMD database is already at the latest version. Skipping download."
            )
            return

        logger.info("New OQMD version detected. Proceeding with download...")

        # Create temporary directory if none provided
        temp_dir = download_dir or tempfile.mkdtemp()
        os.makedirs(temp_dir, exist_ok=True)

        # Download the gzipped SQL file
        logger.info("Downloading SQL database file...")
        sql_gz_path = os.path.join(temp_dir, "oqmd.sql.gz")
        sql_path = os.path.join(temp_dir, "oqmd.sql")

        if not os.path.exists(sql_path):
            sql_path = download_file(
                latest_url,
                sql_gz_path,
                "Downloading OQMD database. Progress bar shows uncompressed size so may be misleading.",
                decompress="gz",
            )
        else:
            logger.info("SQL database file already exists. Skipping download.")

        logger.info("File decompressed successfully")

        # Create fresh database
        logger.info("Setting up MySQL database")

        user_input = input(
            f"Warning: This will drop the existing database to replace it with the new one from {latest_url}. "
            "Are you sure you want to continue? If you press N, the script will execute with your current database. (y/N): "
        )
        if user_input.lower() != "y":
            logger.info("Database update cancelled by user")
            return

        db.drop_database()
        db.create_database()

        # Import the SQL file using execute_sql_file
        logger.info("Importing SQL file, this may take a while...")
        try:
            execute_sql_file(
                sql_path,
                user=db_config["user"],
                password=db_config["password"],
                database=db_config["database"],
                host=db_config["host"],
            )
        except Exception as e:
            logger.error(f"Error during SQL processing: {str(e)}")
            raise

        # Updates the version tracked
        update_oqmd_version(version_db, db_config, latest_url, modification_date)

        logger.info("SQL import completed successfully")

    except Exception as e:
        logger.error(f"Error during SQL processing: {str(e)}")
        raise

    finally:
        # Clean up
        db.close()
        if (
            not download_dir and "temp_dir" in locals()
        ):  # Only remove if we created the temp directory
            import shutil

            shutil.rmtree(temp_dir)


def get_oqmd_version_if_exists(
    version_db: Optional[MySQLDatabase] = None, db_config: Optional[dict] = None
) -> str | None:
    """Get the version of the OQMD database if it exists.
    Otherwise, creates a new database for storing the versions.

    Parameters
    ----------
    version_db : MySQLDatabase, optional
        Database for storing the versions. If None, a new database is created.
    db_config : dict, optional
        Database configuration with keys: host, user, password, database

    Returns
    -------
    tuple[str, datetime]
        A tuple containing (download_url, modification_date)
    """
    assert version_db is not None or db_config is not None, (
        "Either version_db or db_config must be provided"
    )
    if version_db is None:
        version_db = MySQLDatabase(**db_config)
        version_db.create_database()
        version_db.connect()

    version_db_name = version_db.database

    version_db.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {version_db_name} (
            id INT PRIMARY KEY DEFAULT 1,
            download_url TEXT NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT single_row CHECK (id = 1)
        );
    """)

    result = version_db.fetch_items(
        query=f"SELECT download_url, last_updated FROM {version_db_name} ORDER BY last_updated DESC LIMIT 1"
    )
    version_db.close()

    if not result:
        return None, None
    return result[0]["download_url"], result[0]["last_updated"]


def update_oqmd_version(
    version_db: Optional[MySQLDatabase] = None,
    db_config: Optional[dict] = None,
    latest_url: str = None,
    modification_date: datetime = None,
) -> None:
    """Update the version of the OQMD database.

    Parameters
    ----------
    version_db : MySQLDatabase, optional
        Database for storing the versions. If None, a new database is created.
    db_config : dict, optional
        Database configuration with keys: host, user, password, database
    latest_url : str, optional
        The URL of the latest SQL file. If None, the latest URL is fetched from the OQMD website.
    modification_date : datetime, optional
        The date of the update. If None, the current date is used.
    """
    assert version_db is not None or db_config is not None, (
        "Either version_db or db_config must be provided"
    )
    if version_db is None:
        version_db = MySQLDatabase(**db_config)
        version_db.create_database()
        version_db.connect()

    version_db_name = version_db.database

    version_db.execute_sql(
        f"""
        INSERT INTO {version_db_name} (id, download_url, last_updated)
        VALUES (1, %s, %s)
        ON DUPLICATE KEY UPDATE download_url = %s, last_updated = %s;
    """,
        (latest_url, modification_date, latest_url, modification_date),
    )
    version_db.close()


def get_latest_sql_file_url_from_oqmd(
    download_page_url: str = "https://oqmd.org/download/",
) -> tuple[str, datetime]:
    """Get the latest SQL file URL and its modification date from the OQMD download page.

    Returns
    -------
    tuple[str, datetime]
        A tuple containing (download_url, modification_date)
    """

    logger.info(f"Fetching OQMD download page: {download_page_url}")
    # Look for links containing SQL database files
    sql_links = list_download_links_from_page(
        download_page_url,
        pattern=r"\.sql\.gz\s*$",
    )
    sql_links = [link["url"] for link in sql_links]

    if not sql_links:
        raise ValueError("No SQL database files found on the download page")

    # Sort links by version if possible, otherwise take the first one
    def extract_version(url):
        match = re.search(r"v(\d+)_(\d+)", url)  # this matches v1_0, v1_1, etc.
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return (0, 0)

    # Sort links by version if possible, otherwise take the first one
    sql_links.sort(key=extract_version, reverse=True)
    sql_download_url = sql_links[0]

    # Ensure the URL is absolute
    if not sql_download_url.startswith(("http://", "https://")):
        sql_download_url = (
            f"https://oqmd.org{sql_download_url}"
            if not sql_download_url.startswith("/")
            else f"https://oqmd.org/{sql_download_url}"
        )

    # Extract the version from the URL to find the corresponding date
    version_match = re.search(r"v(\d+)_(\d+)", sql_download_url)
    if version_match:
        version = f"v{version_match.group(1)}.{version_match.group(2)}"
        # Extract the date from the page content
        page_content = get_page_content(download_page_url)
        date_pattern = f"OQMD {version}.*?Database updated on: ([^\\n]+)"
        date_match = re.search(date_pattern, page_content, re.DOTALL)
        if date_match:
            date_str = date_match.group(1).strip()
            modification_date = parse_oqmd_date(date_str)
        else:
            modification_date = datetime.now()
    else:
        modification_date = datetime.now()

    logger.info(f"Found SQL database file: {sql_download_url}")
    logger.info(f"Database last modified: {modification_date.strftime('%Y-%m-%d')}")

    return sql_download_url, modification_date


def parse_oqmd_date(date_str: str) -> datetime:
    """Parse the OQMD date string into a datetime object.

    Parameters
    ----------
    date_str : str
        Date string in the format "Month, Year" (e.g., "November, 2023") with possible HTML tags

    Returns
    -------
    datetime
        Parsed datetime object, or today's date if parsing fails
    """
    try:
        # Convert month name to number
        month_map = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12,
        }

        date_str = re.sub(r"<[^>]+>", "", date_str)  # Remove HTML tags
        date_str = " ".join(date_str.split())  # Normalize whitespace

        # Split the date string and clean it
        month_str, year_str = date_str.split(",")
        month_str = month_str.strip()
        year_str = year_str.strip()

        # Convert to datetime
        return datetime(int(year_str), month_map[month_str], 1)
    except (ValueError, KeyError, AttributeError) as e:
        logger.warning(
            f"Could not parse date string: {date_str}. Error: {str(e)}. Using today's date."
        )
        return datetime.now()
