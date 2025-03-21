# Copyright 2025 Entalpic
import json
from typing import Any, List, Optional

import psycopg2
from psycopg2.extras import Json, execute_values

from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import OptimadeStructure
from lematerial_fetcher.models.trajectories import Trajectory


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
        self.columns = {
            "id": "TEXT PRIMARY KEY",
            "type": "TEXT",
            "attributes": "JSONB",
            "last_modified": "TIMESTAMP NULL",
        }

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

    def fetch_items_with_ids(
        self, ids: List[str], table_name: Optional[str] = None
    ) -> List[RawStructure]:
        """
        Fetch items from the database that match the given list of IDs.

        Parameters
        ----------
        ids : List[str]
            List of IDs to retrieve from the database

        Returns
        -------
        List[RawStructure]
            List of RawStructure objects matching the requested IDs
        table_name : str, optional
            Name of the table to fetch items from, by default None

        Raises
        ------
        Exception
            If there's an error during database query execution
        """
        if not ids:
            return []

        if not table_name:
            table_name = self.table_name

        with self.conn.cursor() as cur:
            # Use parameterized query with ANY to safely handle the list of IDs
            placeholders = ",".join(["%s"] * len(ids))
            query = f"""
            SELECT id, type, attributes, last_modified
            FROM {table_name}
            WHERE id IN ({placeholders});
            """

            try:
                cur.execute(query, ids)
                results = []
                for row in cur.fetchall():
                    id_val, type_val, attributes_json, last_modified = row
                    # Parse the JSON attributes
                    attributes = (
                        json.loads(attributes_json)
                        if isinstance(attributes_json, str)
                        else attributes_json
                    )
                    results.append(
                        RawStructure(
                            id=id_val,
                            type=type_val,
                            attributes=attributes,
                            last_modified=last_modified,
                        )
                    )
                return results
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error fetching items with IDs: {str(e)}")

    def count_items(self) -> int:
        """
        Count the number of items in the table.
        """
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            return cur.fetchone()[0]

    def close(self) -> None:
        """
        Close the database connection.
        """
        self.conn.close()


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
            # Create SET clause for all columns except id
            set_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in self.columns.keys() if col != "id"
            )
            query = f"""
            INSERT INTO {self.table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET {set_clause};"""

            try:
                attributes_json = json.dumps(structure.attributes)
                cur.execute(
                    query,
                    (
                        structure.id,
                        structure.type,
                        attributes_json,
                        structure.last_modified,
                    ),
                )
                self.conn.commit()
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error inserting data for ID {structure.id}: {str(e)}")

    def batch_insert_data(
        self, structures: List[RawStructure], batch_size: int = 1000
    ) -> None:
        """
        Insert multiple structures into the database in batches using execute_values.

        Parameters
        ----------
        structures : List[RawStructure]
            List of structure objects to insert
        batch_size : int, optional
            Number of structures to insert in each batch, by default 1000

        Raises
        ------
        Exception
            If there's an error during JSON encoding or database insertion
        """
        if not structures:
            return

        with self.conn.cursor() as cur:
            # Process structures in batches
            for i in range(0, len(structures), batch_size):
                batch = structures[i : i + batch_size]
                # Create a list of value tuples for the batch
                values = []
                for structure in batch:
                    attributes_json = json.dumps(structure.attributes)
                    values.append(
                        (
                            structure.id,
                            structure.type,
                            attributes_json,
                            structure.last_modified,
                        )
                    )

                columns = ", ".join(self.columns.keys())
                # Create SET clause for all columns except id
                set_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in self.columns.keys()
                    if col != "id"
                )
                query = f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET {set_clause};"""

                try:
                    execute_values(cur, query, values)
                    self.conn.commit()
                except (json.JSONDecodeError, psycopg2.Error) as e:
                    raise Exception(f"Error during batch insert: {str(e)}")

    def fetch_items(
        self, offset: int = 0, batch_size: int = 100, table_name: Optional[str] = None
    ) -> list[RawStructure]:
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
        table_name : str, optional
            Name of the table to fetch items from, by default None

        Raises
        ------
        Exception
            If there's an error during database query or JSON decoding
        """
        if not table_name:
            table_name = self.table_name

        with self.conn.cursor() as cur:
            columns = ", ".join(self.columns.keys())
            query = f"""
            SELECT {columns}
            FROM {table_name}
            ORDER BY id
            LIMIT %s OFFSET %s;"""

            try:
                cur.execute(query, (batch_size, offset))
                rows = cur.fetchall()

                structures = []
                for row in rows:
                    id_, type_, attributes, last_modified = row

                    structures.append(
                        RawStructure(
                            id=id_,
                            type=type_,
                            attributes=attributes,
                            last_modified=last_modified,
                        )
                    )

                return structures
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error fetching items: {str(e)}")


class OptimadeDatabase(StructuresDatabase):
    """
    Base database class for handling OPTIMADE-compliant structure data.
    Contains common functionality shared between OptimadeDatabase and TrajectoriesDatabase.

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
            # Create SET clause for all columns except id
            set_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in self.columns.keys() if col != "id"
            )
            query = f"""
            INSERT INTO {self.table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET {set_clause};"""

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

    def batch_insert_data(
        self, structures: List[OptimadeStructure], batch_size: int = 1000
    ) -> None:
        """
        Insert multiple OPTIMADE structures into the database in batches using execute_values.

        Parameters
        ----------
        structures : List[OptimadeStructure]
            List of OptimadeStructure objects to insert
        batch_size : int, optional
            Number of structures to insert in each batch, by default 1000

        Raises
        ------
        Exception
            If there's an error during data insertion or JSON encoding
        """
        if not structures:
            return

        with self.conn.cursor() as cur:
            # Process structures in batches
            for i in range(0, len(structures), batch_size):
                batch = structures[i : i + batch_size]
                # Create a list of value tuples for the batch
                values = []
                for structure in batch:
                    species_data = self._prepare_species_data(structure.species)
                    values.append(
                        (
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
                    )

                columns = ", ".join(self.columns.keys())
                # Create SET clause for all columns except id
                set_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in self.columns.keys()
                    if col != "id"
                )
                # Update all columns except id on conflict with the new values
                query = f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET {set_clause};"""

                try:
                    execute_values(cur, query, values)
                    self.conn.commit()
                except (json.JSONDecodeError, psycopg2.Error) as e:
                    raise Exception(f"Error during batch insert: {str(e)}")


class TrajectoriesDatabase(OptimadeDatabase):
    """
    Database class for handling trajectory data.
    Inherits common functionality from OptimadeDatabase.
    """

    def __init__(self, conn_str: str, table_name: str):
        super().__init__(conn_str, table_name)
        # trajectory-specific columns
        self.columns.update(
            {
                "relaxation_step": "INTEGER",
                "relaxation_number": "INTEGER",
            }
        )

    def insert_data(self, structure: Trajectory) -> None:
        """
        Insert a trajectory structure into the database.

        Parameters
        ----------
        structure : Trajectory
            Trajectory structure object containing all required fields including trajectory-specific ones

        Raises
        ------
        Exception
            If there's an error during data insertion or JSON encoding
        """
        with self.conn.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(self.columns))
            columns = ", ".join(self.columns.keys())
            # Create SET clause for all columns except id
            set_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in self.columns.keys() if col != "id"
            )
            query = f"""
            INSERT INTO {self.table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT (id) DO UPDATE SET {set_clause};"""

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
                    # trajectory-specific fields
                    structure.relaxation_step,
                    structure.relaxation_number,
                )
                cur.execute(query, input_data)
                self.conn.commit()
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error inserting data for ID {structure.id}: {str(e)}")

    def batch_insert_data(
        self, structures: List[Trajectory], batch_size: int = 1000
    ) -> None:
        """
        Insert multiple Trajectory objects into the database in batches using execute_values.

        Parameters
        ----------
        structures : List[Trajectory]
            List of Trajectory objects to insert
        batch_size : int, optional
            Number of structures to insert in each batch, by default 1000

        Raises
        ------
        Exception
            If there's an error during data insertion or JSON encoding
        """
        if not structures:
            return

        with self.conn.cursor() as cur:
            # Process structures in batches
            for i in range(0, len(structures), batch_size):
                batch = structures[i : i + batch_size]
                # Create a list of value tuples for the batch
                values = []
                for structure in batch:
                    species_data = self._prepare_species_data(structure.species)
                    values.append(
                        (
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
                            # trajectory-specific fields
                            structure.relaxation_step,
                            structure.relaxation_number,
                        )
                    )

                columns = ", ".join(self.columns.keys())
                # Create SET clause for all columns except id
                set_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in self.columns.keys()
                    if col != "id"
                )
                # Update all columns except id on conflict with the new values
                query = f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET {set_clause};"""

                try:
                    execute_values(cur, query, values)
                    self.conn.commit()
                except (json.JSONDecodeError, psycopg2.Error) as e:
                    raise Exception(f"Error during batch insert: {str(e)}")


class DatasetVersions(Database):
    """
    Database class for tracking dataset versions and sync status.
    """

    def __init__(self, conn_str: str):
        super().__init__(conn_str, "dataset_versions")
        self.columns = {
            "dataset_name": "TEXT PRIMARY KEY",
            "last_synced_version": "TEXT",
            "last_sync_date": "TIMESTAMP",
            "sync_status": "TEXT",
        }

    def update_version(self, dataset_name: str, version: str) -> None:
        """
        Update the version information for a dataset.

        Parameters
        ----------
        dataset_name : str
            Name of the dataset (e.g., 'mp_structures')
        version : str
            Version identifier (e.g., '2025-03-14')
        """
        with self.conn.cursor() as cur:
            query = f"""
            INSERT INTO {self.table_name} (dataset_name, last_synced_version, last_sync_date, sync_status)
            VALUES (%s, %s, NOW(), 'completed')
            ON CONFLICT (dataset_name) 
            DO UPDATE SET 
                last_synced_version = EXCLUDED.last_synced_version,
                last_sync_date = EXCLUDED.last_sync_date,
                sync_status = EXCLUDED.sync_status;
            """
            cur.execute(query, (dataset_name, version))
            self.conn.commit()

    def get_last_synced_version(self, dataset_name: str) -> Optional[str]:
        """
        Get the last synced version for a dataset.

        Parameters
        ----------
        dataset_name : str
            Name of the dataset

        Returns
        -------
        Optional[str]
            The last synced version, or None if dataset hasn't been synced
        """
        with self.conn.cursor() as cur:
            query = f"""
            SELECT last_synced_version 
            FROM {self.table_name} 
            WHERE dataset_name = %s;
            """
            cur.execute(query, (dataset_name,))
            result = cur.fetchone()
            return result[0] if result else None


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
