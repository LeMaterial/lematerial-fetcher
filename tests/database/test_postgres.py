# Copyright 2025 Entalpic
"""Regression tests for offset pagination in ``StructuresDatabase``.

These tests run against a real PostgreSQL server, because the bug they guard
against lives in the SQL itself (``WHERE id > start_id`` vs ``WHERE id >= start_id``)
and a mocked cursor cannot catch it.

They are skipped automatically when no server is reachable. To run them locally:

.. code-block:: bash

    docker run -d --name lemat-test-pg \\
        -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -e POSTGRES_DB=lematerial \\
        -p 5432:5432 postgres:16
    uv run pytest tests/database/ -m postgres

Set ``LEMATERIALFETCHER_TEST_DB_CONN_STR`` to point at a different server.
"""

import os
import uuid

import psycopg2
import pytest

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.models.models import RawStructure

DEFAULT_CONN_STR = (
    "host=localhost user=root password=root dbname=lematerial sslmode=disable"
)
CONN_STR = os.environ.get("LEMATERIALFETCHER_TEST_DB_CONN_STR", DEFAULT_CONN_STR)

pytestmark = pytest.mark.postgres


def _postgres_available() -> bool:
    try:
        psycopg2.connect(CONN_STR).close()
        return True
    except psycopg2.OperationalError:
        return False


@pytest.fixture
def db():
    """A StructuresDatabase backed by a uniquely-named, throwaway table."""
    if not _postgres_available():
        pytest.skip(
            "No PostgreSQL server available "
            "(set LEMATERIALFETCHER_TEST_DB_CONN_STR or start a local server)"
        )
    table_name = f"test_structures_{uuid.uuid4().hex[:12]}"
    database = StructuresDatabase(CONN_STR, table_name)
    database.create_table()
    yield database
    with database.conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    database.conn.commit()
    database.close()


def insert_rows(db: StructuresDatabase, n: int) -> list[str]:
    """Insert n rows with ids that sort in insertion order, return the ids."""
    ids = [f"struct-{i:03d}" for i in range(n)]
    db.batch_insert_data(
        [
            RawStructure(
                id=id_,
                type="test-structure",
                attributes={"index": i},
                last_modified=None,
            )
            for i, id_ in enumerate(ids)
        ]
    )
    return ids


def test_fetch_items_iter_starts_at_offset(db):
    """fetch_items_iter(offset=k) must start at the row at rank k, not k+1."""
    ids = insert_rows(db, 10)
    got = [row.id for row in db.fetch_items_iter(offset=4, limit=3)]
    assert got == ids[4:7]


def test_fetch_items_returns_row_at_offset(db):
    ids = insert_rows(db, 10)
    rows = db.fetch_items(offset=3, batch_size=1)
    assert [row.id for row in rows] == [ids[3]]


def test_batched_pagination_covers_every_row_exactly_once(db):
    """Reading a table in consecutive (offset, batch_size) windows, the way
    BaseTransformer does, must yield every row exactly once.

    Regression test: fetch_items_iter used to resolve the id *at* the offset
    and then scan ``WHERE id > start_id``, silently dropping the row at rank
    ``batch_size`` from every multi-batch read.
    """
    ids = insert_rows(db, 10)
    batch_size = 3

    seen = []
    offset = 0
    while True:
        batch = db.fetch_items(offset=offset, batch_size=batch_size)
        if not batch:
            break
        seen.extend(row.id for row in batch)
        offset += batch_size

    assert seen == ids


def test_offset_zero_reads_from_first_row(db):
    ids = insert_rows(db, 5)
    got = [row.id for row in db.fetch_items_iter(offset=0)]
    assert got == ids


def test_offset_past_end_yields_nothing(db):
    insert_rows(db, 5)
    assert list(db.fetch_items_iter(offset=5)) == []
    assert db.fetch_items(offset=99, batch_size=10) == []
