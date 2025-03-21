import logging
from typing import Any, Dict, List, Optional

import mysql.connector
from mysql.connector import Error

logger = logging.getLogger(__name__)


class MySQLDatabase:
    """A minimal MySQL database handler for dumping and fetching data."""

    def __init__(self, host: str, user: str, password: str, database: str):
        """
        Initialize the MySQL database connection.

        Parameters
        ----------
        host : str
            The database host
        user : str
            The database user
        password : str
            The database password
        database : str
            The database name
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self) -> None:
        """Establish connection to the database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host, user=self.user, password=self.password
            )
            logger.info("Successfully connected to MySQL server")
        except Error as e:
            logger.error(f"Error connecting to MySQL server: {e}")
            raise

    def create_database(self) -> None:
        """Create the database if it doesn't exist."""
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}")
            self.connection.commit()
            logger.info(f"Database '{self.database}' created or already exists")
        except Error as e:
            logger.error(f"Error creating database: {e}")
            raise
        finally:
            cursor.close()

    def execute_sql(self, query: str, params: tuple = None) -> None:
        """
        Execute a SQL query with optional parameters.

        Parameters
        ----------
        query : str
            The SQL query to execute
        params : tuple, optional
            Query parameters to substitute
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE {self.database}")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
        except Error as e:
            logger.error(f"Error executing SQL query: {e}")
            raise
        finally:
            cursor.close()

    def fetch_one(self, query: str, params: tuple = None) -> tuple:
        """
        Execute a SQL query and fetch one result.

        Parameters
        ----------
        query : str
            The SQL query to execute
        params : tuple, optional
            Query parameters to substitute

        Returns
        -------
        tuple
            The first row of results, or None if no results
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE {self.database}")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchone()
        except Error as e:
            logger.error(f"Error executing SQL query: {e}")
            raise
        finally:
            cursor.close()

    def fetch_items(
        self, query_or_table: str, params: tuple = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch items from the database using either a custom query or table name.

        Parameters
        ----------
        query_or_table : str
            Either a complete SQL query or just a table name
        params : tuple, optional
            Query parameters to substitute if using a custom query
        limit : Optional[int]
            Maximum number of items to fetch. Only used if query_or_table is a table name.

        Returns
        -------
        List[Dict[str, Any]]
            List of fetched items
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"USE {self.database}")

            # Check if input is just a table name or a complete query
            if " " not in query_or_table:  # Simple table name
                query = f"SELECT * FROM {query_or_table}"
                if limit:
                    query += f" LIMIT {limit}"
                cursor.execute(query)
            else:  # Custom query
                if params:
                    cursor.execute(query_or_table, params)
                else:
                    cursor.execute(query_or_table)

            items = cursor.fetchall()
            return items
        except Error as e:
            logger.error(f"Error fetching items: {e}")
            raise
        finally:
            cursor.close()

    def drop_database(self) -> None:
        """Delete the entire database."""
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {self.database}")
            self.connection.commit()
            logger.info(f"Database '{self.database}' dropped successfully")
        except Error as e:
            logger.error(f"Error dropping database: {e}")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")


def execute_sql_file(
    sql_file_path: str,
    user: str = "newuser",
    password: str = "password",
    database: str = "database_name",
    host: str = "localhost",
    port: int = 3306,
) -> None:
    """
    Launch a subprocess to execute a SQL file.

    Parameters
    ----------
    sql_file_path : str
        The path to the SQL file to execute
    """
    import subprocess

    with open(sql_file_path, "r") as f:
        process = subprocess.Popen(
            ["mysql", "-u", user, "-p" + password, database],
            stdin=f,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout, stderr = process.communicate()

    if process.returncode == 0:
        print("SQL file executed successfully.")
    else:
        print("Error executing SQL file:", stderr)
