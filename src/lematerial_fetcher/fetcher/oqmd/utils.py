import os
import re
import tempfile
from datetime import datetime
from typing import Optional

from lematerial_fetcher.database.mysql import MySQLDatabase, execute_sql_file
from lematerial_fetcher.utils.io import (
    download_file,
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

    # Get the latest available version URL
    latest_url = get_latest_sql_file_url_from_oqmd(download_page_url=download_page_url)

    # Connect to database to check version
    db = MySQLDatabase(**db_config)

    try:
        # First ensure version tracking table exists
        db.connect()

        # Check if we have a stored version and if URLs match we can skip the download
        version_db_config = db_config.copy()
        version_db_config["database"] = f"{db_config['database']}_version"
        version_db = MySQLDatabase(**version_db_config)
        version_db.create_database()
        current_url = get_oqmd_version_if_exists(version_db)

        if current_url == latest_url:
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

        # Update version tracking
        today = datetime.now().strftime("%Y-%m-%d")
        update_oqmd_version(version_db, db_config, latest_url, today)

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
    str
        The version of the OQMD database.
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
        query=f"SELECT download_url FROM {version_db_name} ORDER BY last_updated DESC LIMIT 1"
    )
    version_db.close()

    if not result:
        return None
    return result[0]["download_url"]


def update_oqmd_version(
    version_db: Optional[MySQLDatabase] = None,
    db_config: Optional[dict] = None,
    latest_url: str = None,
    today: str = None,
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
    today : str, optional
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
        (latest_url, today, latest_url, today),
    )
    version_db.close()


def get_latest_sql_file_url_from_oqmd(
    download_page_url: str = "https://oqmd.org/download/",
) -> str:
    """Get the latest SQL file URL from the OQMD download page."""

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

    logger.info(f"Found SQL database file: {sql_download_url}")

    return sql_download_url
