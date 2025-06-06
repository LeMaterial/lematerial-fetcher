# Copyright 2025 Entalpic
import itertools
import json
import time
from typing import Any, Generator, List, Optional

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
            self.create_indexes(cur)
            self.conn.commit()

    def create_indexes(self, cur) -> None:
        """
        Create indexes for the table. Override this method in child classes to add specific indexes.

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Database cursor
        """
        if "id" in self.columns:
            query = f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_id 
            ON {self.table_name} (id);
            """
            cur.execute(query)

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

    def get_id_at_offset(
        self, offset: int, table_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Get the ID of the record at the specified offset using index-only scan.

        Parameters
        ----------
        offset : int
            The offset position to get the ID from
        table_name : str, optional
            Name of the table to fetch from, by default None (uses self.table_name)

        Returns
        -------
        Optional[str]
            The ID at the specified offset, or None if no record exists at that offset
        """
        if not table_name:
            table_name = self.table_name

        with self.conn.cursor() as cur:
            # Simple query that can use index-only scan
            query = f"""
            SELECT id 
            FROM {table_name}
            ORDER BY id
            OFFSET %s
            LIMIT 1;
            """
            # Add hint to force index scan instead of sequential scan
            cur.execute("SET enable_seqscan = off;")
            cur.execute(query, (offset,))
            result = cur.fetchone()
            return result[0] if result else None


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

    def fetch_items_iter(
        self,
        offset: int = 0,
        limit: Optional[int] = None,  # Total number of rows to process
        batch_size: int = 100,  # Number of rows to fetch in each database round-trip
        table_name: Optional[str] = None,
        cursor_name: Optional[str] = None,
    ) -> Generator[RawStructure, None, None]:
        """
        Fetch items from the database using a server-side cursor, yielding results one at a time.
        This is memory efficient for large result sets as it doesn't load all results into memory at once.
        Uses ID-based pagination for better performance on large offsets.

        Parameters
        ----------
        offset : int, optional
            Number of items to skip, by default 0
        limit : Optional[int], optional
            Total number of items to process, by default None (process all available items)
        batch_size : int, optional
            Number of items to fetch in each database round-trip, by default 100
        table_name : str, optional
            Name of the table to fetch from, by default None (uses self.table_name)
        cursor_name : str, optional
            Name for the server-side cursor, by default None (auto-generated)

        Yields
        ------
        RawStructure
            One structure at a time from the result set

        Raises
        ------
        Exception
            If there's an error during database query execution
        """
        if not table_name:
            table_name = self.table_name

        # Create a unique cursor name if none provided
        if cursor_name is None:
            cursor_name = f"fetch_items_cursor_{id(self)}_{time.time_ns()}"

        try:
            with self.conn.cursor(name=cursor_name) as cur:  # Server-side cursor
                # Get the starting ID if offset > 0
                start_id = None
                if offset > 0:
                    start_id = self.get_id_at_offset(offset, table_name)
                    if not start_id:  # No results at this offset
                        return

                # Construct the query based on whether we have a start_id
                if start_id:
                    query = f"""
                    SELECT id, type, attributes, last_modified
                    FROM {table_name}
                    WHERE id > %s
                    ORDER BY id
                    {f"LIMIT {limit}" if limit is not None else ""}
                    """
                    cur.execute(query, (start_id,))
                else:
                    query = f"""
                    SELECT id, type, attributes, last_modified
                    FROM {table_name}
                    ORDER BY id
                    {f"LIMIT {limit}" if limit is not None else ""}
                    """
                    cur.execute(query)

                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        id_val, type_val, attributes_json, last_modified = row
                        # Parse the JSON attributes
                        attributes = (
                            json.loads(attributes_json)
                            if isinstance(attributes_json, str)
                            else attributes_json
                        )
                        yield RawStructure(
                            id=id_val,
                            type=type_val,
                            attributes=attributes,
                            last_modified=last_modified,
                        )

                        del row
                        del attributes_json

                    del rows

        except (json.JSONDecodeError, psycopg2.Error) as e:
            raise Exception(f"Error fetching items: {str(e)}")

    def fetch_items(
        self,
        offset: int = 0,
        batch_size: int = 100,
        table_name: Optional[str] = None,
    ) -> List[RawStructure]:
        """
        Fetch a batch of items from the database.
        This method uses fetch_items_iter internally but returns a list for backward compatibility.

        Parameters
        ----------
        offset : int, optional
            Number of items to skip, by default 0
        batch_size : int, optional
            Maximum number of items to return, by default 100
        table_name : str, optional
            Name of the table to fetch from, by default None (uses self.table_name)

        Returns
        -------
        List[RawStructure]
            List of RawStructure objects

        Raises
        ------
        Exception
            If there's an error during database query execution
        """
        return list(
            itertools.islice(
                self.fetch_items_iter(
                    offset=offset, batch_size=batch_size, table_name=table_name
                ),
                batch_size,
            )
        )

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
                for row in cur:
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
        self.columns = OptimadeDatabase.columns()

    def create_index(self) -> None:
        """
        Create an index on the id column.
        """
        self.super().create_index()
        # Create index on (id, functional, cross_compatibility) for faster exports
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE INDEX idx_id_functional_cross_compatibility ON {self.table_name} (id, functional, cross_compatibility);"
            )
            self.conn.commit()

    @classmethod
    def columns(cls) -> dict[str, str]:
        return {
            "id": "TEXT PRIMARY KEY",
            "source": "TEXT",
            "elements": "TEXT[]",
            "nelements": "INTEGER",
            "elements_ratios": "FLOAT[]",
            "nsites": "INTEGER",
            "cartesian_site_positions": "FLOAT[][]",
            "lattice_vectors": "FLOAT[][]",
            "species_at_sites": "TEXT[][]",
            "species": "JSONB",
            "chemical_formula_anonymous": "TEXT",
            "chemical_formula_reduced": "TEXT",
            "chemical_formula_descriptive": "TEXT",
            "dimension_types": "INTEGER[]",
            "nperiodic_dimensions": "INTEGER",
            "immutable_id": "TEXT",
            "last_modified": "TIMESTAMP",
            "stress_tensor": "FLOAT[][]",
            "energy": "FLOAT",
            "energy_corrected": "FLOAT",
            "magnetic_moments": "FLOAT[]",
            "forces": "FLOAT[][]",
            "total_magnetization": "FLOAT",
            "dos_ef": "FLOAT",
            "charges": "FLOAT[]",
            "band_gap_indirect": "FLOAT",
            "functional": "TEXT",
            "space_group_it_number": "INTEGER",
            "cross_compatibility": "BOOLEAN",
            "bawl_fingerprint": "TEXT",
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
        Json
            Species data converted to PostgreSQL JSONB format
        """
        return Json(species)

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
                    structure.lattice_vectors,
                    structure.species_at_sites,
                    species_data,
                    structure.chemical_formula_anonymous,
                    structure.chemical_formula_reduced,
                    structure.chemical_formula_descriptive,
                    structure.dimension_types,
                    structure.nperiodic_dimensions,
                    structure.immutable_id,
                    structure.last_modified,
                    structure.stress_tensor,
                    structure.energy,
                    structure.energy_corrected,
                    structure.magnetic_moments,
                    structure.forces,
                    structure.total_magnetization,
                    structure.dos_ef,
                    structure.charges,
                    structure.band_gap_indirect,
                    structure.functional,
                    structure.space_group_it_number,
                    structure.cross_compatibility,
                    structure.bawl_fingerprint,
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
                            structure.lattice_vectors,
                            structure.species_at_sites,
                            species_data,
                            structure.chemical_formula_anonymous,
                            structure.chemical_formula_reduced,
                            structure.chemical_formula_descriptive,
                            structure.dimension_types,
                            structure.nperiodic_dimensions,
                            structure.immutable_id,
                            structure.last_modified,
                            structure.stress_tensor,
                            structure.energy,
                            structure.energy_corrected,
                            structure.magnetic_moments,
                            structure.forces,
                            structure.total_magnetization,
                            structure.dos_ef,
                            structure.charges,
                            structure.band_gap_indirect,
                            structure.functional,
                            structure.space_group_it_number,
                            structure.cross_compatibility,
                            structure.bawl_fingerprint,
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
        self.columns = TrajectoriesDatabase.columns()

    @classmethod
    def columns(cls) -> dict[str, str]:
        return OptimadeDatabase.columns() | {
            "relaxation_step": "INTEGER",
            "relaxation_number": "INTEGER",
        }

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
                    structure.lattice_vectors,
                    structure.species_at_sites,
                    species_data,
                    structure.chemical_formula_anonymous,
                    structure.chemical_formula_reduced,
                    structure.chemical_formula_descriptive,
                    structure.dimension_types,
                    structure.nperiodic_dimensions,
                    structure.immutable_id,
                    structure.last_modified,
                    structure.stress_tensor,
                    structure.energy,
                    structure.energy_corrected,
                    structure.magnetic_moments,
                    structure.forces,
                    structure.total_magnetization,
                    structure.dos_ef,
                    structure.charges,
                    structure.band_gap_indirect,
                    structure.functional,
                    structure.space_group_it_number,
                    structure.cross_compatibility,
                    structure.bawl_fingerprint,
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
                            structure.lattice_vectors,
                            structure.species_at_sites,
                            species_data,
                            structure.chemical_formula_anonymous,
                            structure.chemical_formula_reduced,
                            structure.chemical_formula_descriptive,
                            structure.dimension_types,
                            structure.nperiodic_dimensions,
                            structure.immutable_id,
                            structure.last_modified,
                            structure.stress_tensor,
                            structure.energy,
                            structure.energy_corrected,
                            structure.magnetic_moments,
                            structure.forces,
                            structure.total_magnetization,
                            structure.dos_ef,
                            structure.charges,
                            structure.band_gap_indirect,
                            structure.functional,
                            structure.space_group_it_number,
                            structure.cross_compatibility,
                            structure.bawl_fingerprint,
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
