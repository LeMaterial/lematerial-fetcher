# Copyright 2025 Entalpic
import json
from typing import Any, Optional

import psycopg2
from psycopg2.extras import Json

from material_fetcher.model.models import RawStructure
from material_fetcher.model.optimade import OptimadeStructure


class Database:
    """
    Base database class for handling PostgreSQL connections and table operations.

    Parameters
    ----------
    conn_str : str
        PostgreSQL connection string
    table_name : str
        Name of the database table to operate on

    Attributes
    ----------
    conn : psycopg2.extensions.connection
        PostgreSQL database connection
    table_name : str
        Name of the database table
    columns : dict
        Dictionary defining table column names and their SQL types
    """

    def __init__(self, conn_str: str, table_name: str):
        self.conn = psycopg2.connect(conn_str)
        self.table_name = table_name
        self.columns = {"id": "TEXT PRIMARY KEY", "type": "TEXT", "attributes": "JSONB"}

    def create_table(self) -> None:
        """
        Create a new table if it doesn't exist.

        Creates a table with columns defined in self.columns dictionary.
        """
        with self.conn.cursor() as cur:
            columns_sql = ", ".join(
                f"{name} {type_}" for name, type_ in self.columns.items()
            )
            query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {columns_sql}
            );"""
            cur.execute(query)
            self.conn.commit()


class StructuresDatabase(Database):
    """
    Database class for handling raw structure data.

    Inherits from Database class to provide specific functionality for storing
    and retrieving structure information.
    """

    def insert_data(self, structure: RawStructure) -> None:
        """
        Insert a new structure into the database.

        Parameters
        ----------
        structure : RawStructure
            Structure object containing id, type, and attributes to be stored

        Raises
        ------
        Exception
            If there's an error during JSON encoding or database insertion
        """
        with self.conn.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(self.columns))
            columns = ", ".join(self.columns.keys())
            query = f"""
            INSERT INTO {self.table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO NOTHING;"""

            try:
                attributes_json = json.dumps(structure.attributes)
                cur.execute(query, (structure.id, structure.type, attributes_json))
                self.conn.commit()
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error inserting data for ID {structure.id}: {str(e)}")

    def fetch_items(self, offset: int = 0, batch_size: int = 100) -> list[RawStructure]:
        """
        Fetch items from the database with pagination support.

        Parameters
        ----------
        offset : int, optional
            Number of items to skip, by default 0
        batch_size : int, optional
            Maximum number of items to return, by default 100

        Returns
        -------
        list[RawStructure]
            List of Structure objects

        Raises
        ------
        Exception
            If there's an error during database query or JSON decoding
        """
        with self.conn.cursor() as cur:
            columns = ", ".join(self.columns.keys())
            query = f"""
            SELECT {columns}
            FROM {self.table_name}
            ORDER BY id
            LIMIT %s OFFSET %s;"""

            try:
                cur.execute(query, (batch_size, offset))
                rows = cur.fetchall()

                structures = []
                for row in rows:
                    id_, type_, attributes = row

                    structures.append(
                        RawStructure(id=id_, type=type_, attributes=attributes)
                    )

                return structures
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error fetching items: {str(e)}")


class OptimadeDatabase(StructuresDatabase):
    """
    Database class for handling OPTIMADE-compliant structure data.

    Inherits from StructuresDatabase and implements specific functionality
    for OPTIMADE structure storage and retrieval.

    Parameters
    ----------
    conn_str : str
        PostgreSQL connection string
    table_name : str
        Name of the database table to operate on
    """

    def __init__(self, conn_str: str, table_name: str):
        super().__init__(conn_str, table_name)
        self.columns = {
            "id": "TEXT PRIMARY KEY",
            "source": "TEXT",
            "elements": "TEXT[]",
            "nelements": "INTEGER",
            "elements_ratios": "FLOAT[]",
            "nsites": "INTEGER",
            "cartesian_site_positions": "FLOAT[][]",
            "species_at_sites": "TEXT[][]",
            "species": "TEXT[]",
            "chemical_formula_anonymous": "TEXT",
            "chemical_formula_descriptive": "TEXT",
            "dimension_types": "INTEGER[]",
            "nperiodic_dimensions": "INTEGER",
            "immutable_id": "TEXT",
            "last_modified": "TIMESTAMP",
            "stress_tensor": "FLOAT[][]",
            "energy": "FLOAT",
            "magnetic_moments": "FLOAT[]",
            "forces": "FLOAT[][]",
            "total_magnetization": "FLOAT",
            "dos_ef": "FLOAT",
            "functional": "TEXT",
            "cross_compatibility": "BOOLEAN",
            "entalpic_fingerprint": "FLOAT[]",
        }

    def _prepare_species_data(self, species: list[dict[str, Any]]) -> list[Json]:
        """
        Convert species dictionaries to JSONB format.

        Parameters
        ----------
        species : list[dict[str, Any]]
            List of species dictionaries containing chemical species information

        Returns
        -------
        list[Json]
            List of species data converted to PostgreSQL JSONB format
        """
        return [Json(s) for s in species]

    def insert_data(self, structure: OptimadeStructure) -> None:
        """
        Insert an OPTIMADE structure into the database.

        Parameters
        ----------
        structure : OptimadeStructure
            OPTIMADE-compliant structure object containing all required fields

        Raises
        ------
        Exception
            If there's an error during data insertion or JSON encoding
        """
        with self.conn.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(self.columns))
            columns = ", ".join(self.columns.keys())
            query = f"""
            INSERT INTO {self.table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO NOTHING;"""

            try:
                species_data = self._prepare_species_data(structure.species)

                input_data = (
                    structure.id,
                    structure.source,
                    structure.elements,
                    structure.nelements,
                    structure.elements_ratios,
                    structure.nsites,
                    structure.cartesian_site_positions,
                    structure.species_at_sites,
                    species_data,
                    structure.chemical_formula_anonymous,
                    structure.chemical_formula_descriptive,
                    structure.dimension_types,
                    structure.nperiodic_dimensions,
                    structure.immutable_id,
                    structure.last_modified,
                    structure.stress_tensor,
                    structure.energy,
                    structure.magnetic_moments,
                    structure.forces,
                    structure.total_magnetization,
                    structure.dos_ef,
                    structure.functional,
                    structure.cross_compatibility,
                    structure.entalpic_fingerprint,
                )
                cur.execute(query, input_data)
                self.conn.commit()
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error inserting data for ID {structure.id}: {str(e)}")


def new_db(conn_str: str, table_name: str) -> Optional[Database]:
    """
    Create a new database connection.

    Parameters
    ----------
    conn_str : str
        PostgreSQL connection string
    table_name : str
        Name of the database table to operate on

    Returns
    -------
    Optional[Database]
        New Database instance if connection successful

    Raises
    ------
    Exception
        If database connection fails
    """
    try:
        return Database(conn_str, table_name)
    except psycopg2.Error as e:
        raise Exception(f"Failed to connect to database: {str(e)}") from e
