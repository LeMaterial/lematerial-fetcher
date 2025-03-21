import logging
from pathlib import Path
from typing import Any, Optional

import mysql.connector
from mysql.connector import Error

logger = logging.getLogger(__name__)


class MySQLDatabase:
    """A minimal MySQL database handler for dumping and fetching data."""

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        table_name: Optional[str] = None,
        cert_path: Optional[str] = None,
    ):
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
        table_name : Optional[str]
            The table name to fetch items from
        cert_path : Optional[str]
            The path to the SSL certificate
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table_name = table_name
        self.connection = None
        self.cert_path = cert_path

    def connect(self) -> None:
        """Establish connection to the database."""
        try:
            ssl_ca = (
                str((Path(self.cert_path) / "server-ca.pem").resolve())
                if self.cert_path
                else None
            )
            ssl_cert = (
                str((Path(self.cert_path) / "client-cert.pem").resolve())
                if self.cert_path
                else None
            )
            ssl_key = (
                str((Path(self.cert_path) / "client-key.pem").resolve())
                if self.cert_path
                else None
            )
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                ssl_ca=ssl_ca,
                ssl_cert=ssl_cert,
                ssl_key=ssl_key,
            )
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
        self,
        offset: Optional[int] = 0,
        batch_size: Optional[int] = 100,
        table_name: Optional[str] = None,
        query: str = "",
        params: tuple = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch items from the database using either a custom query or table name.

        Parameters
        ----------
        offset : Optional[int]
            The offset to start fetching items from
        batch_size : Optional[int]
            The number of items to fetch in each batch
        table_name : Optional[str]
            The name of the table to fetch items from (overrides self.table_name if provided)
        query : str
            Custom SQL query to execute (takes precedence over table_name)
        params : tuple, optional
            Query parameters to substitute if using a custom query

        Returns
        -------
        list[dict[str, Any]]
            List of fetched items
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"USE {self.database}")

            if query:
                # Custom query takes precedence
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
            else:
                # Use table-based query if no custom query provided
                effective_table = table_name or self.table_name

                if not effective_table:
                    raise ValueError(
                        "Either table_name, self.table_name, or query must be provided"
                    )

                # Build the SQL query based on provided parameters
                sql_query = f"SELECT * FROM {effective_table}"

                # Add LIMIT and OFFSET clauses if provided
                if offset is not None and batch_size is not None:
                    sql_query += f" LIMIT {batch_size} OFFSET {offset}"
                elif batch_size is not None:
                    sql_query += f" LIMIT {batch_size}"
                elif offset is not None:
                    raise ValueError("Offset is not supported without batch_size")

                cursor.execute(sql_query)

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
