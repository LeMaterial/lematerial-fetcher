# Copyright 2026 Entalpic
"""Publish a fully ingested staging table and its version in one transaction."""

import re


def identifier(value):
    """Only simple lower-case PostgreSQL identifiers, without truncation."""
    if not isinstance(value, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value):
        raise ValueError("expected a simple lower-case table identifier, max 63 chars")
    return value


def publish(conn, staging, destination, version):
    staging, destination = identifier(staging), identifier(destination)
    try:
        with conn.cursor() as cursor:
            # No replacement: an existing destination causes rollback. Choose a
            # new release table rather than overwrite a prior ingestion.
            cursor.execute(f'ALTER TABLE "{staging}" RENAME TO "{destination}"')
            cursor.execute(
                "INSERT INTO dataset_versions "
                "(dataset_name, last_synced_version, last_sync_date, sync_status) "
                "VALUES (%s, %s, NOW(), 'completed') "
                "ON CONFLICT (dataset_name) DO UPDATE SET "
                "last_synced_version=EXCLUDED.last_synced_version, "
                "last_sync_date=EXCLUDED.last_sync_date, sync_status=EXCLUDED.sync_status",
                (destination, version),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def ingest(db, records, batch_size, receipt, destination, version):
    """Committed batches stay in staging until all parsing and version writes pass.

    A process kill can leave an orphan stage; it cannot publish a partial target.
    Ordinary failures drop that stage and always close its connection.
    """
    from lematerial_fetcher.fetcher.jarvis.archive import store_records

    staging = identifier(db.table_name)
    try:
        db.create_table()
        count = store_records(records, db.batch_insert_data, batch_size, receipt)
        publish(db.conn, staging, destination, version)
        return count
    except Exception:
        db.conn.rollback()
        with db.conn.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{staging}"')
        db.conn.commit()
        raise
    finally:
        db.close()
