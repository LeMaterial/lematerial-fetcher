# Copyright 2025 Entalpic
import json
from dataclasses import dataclass
from typing import Optional

import psycopg2


@dataclass
class Structure:
    id: str
    type: str
    attributes: dict


class Database:
    def __init__(self, conn_str: str, table_name: str):
        self.conn = psycopg2.connect(conn_str)
        self.table_name = table_name

    def create_table(self) -> None:
        with self.conn.cursor() as cur:
            query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id TEXT PRIMARY KEY,
                type TEXT,
                attributes JSONB
            );"""
            cur.execute(query)
            self.conn.commit()

    def insert_data(self, structure: Structure) -> None:
        with self.conn.cursor() as cur:
            query = f"""
            INSERT INTO {self.table_name} (id, type, attributes)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;"""

            try:
                attributes_json = json.dumps(structure.attributes)
                cur.execute(query, (structure.id, structure.type, attributes_json))
                self.conn.commit()
            except (json.JSONDecodeError, psycopg2.Error) as e:
                raise Exception(f"Error inserting data for ID {structure.id}: {str(e)}")


def new_db(conn_str: str, table_name: str) -> Optional[Database]:
    try:
        return Database(conn_str, table_name)
    except psycopg2.Error as e:
        raise Exception(f"Failed to connect to database: {str(e)}") from e
