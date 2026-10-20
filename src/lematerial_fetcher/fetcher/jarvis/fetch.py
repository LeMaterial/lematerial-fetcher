# Copyright 2026 Entalpic
"""Raw JARVIS-DFT properties via the official versioned Figshare release."""

import uuid

from lematerial_fetcher.database.postgres import DatasetVersions, StructuresDatabase
from lematerial_fetcher.fetcher.jarvis.archive import (
    ARCHIVE_MD5,
    ARCHIVE_URL,
    ARCHIVE_VERSION,
    cache_archive,
    file_hash,
    iter_records,
)
from lematerial_fetcher.fetcher.jarvis.publication import identifier, ingest
from lematerial_fetcher.utils.config import load_fetcher_config
from lematerial_fetcher.utils.io import create_session
from lematerial_fetcher.utils.logging import get_cache_dir, logger


class JarvisFetcher:
    """Fetch into a fresh release table with atomic table/version publication.

    This archive owns its lifecycle instead of BaseFetcher's per-item/version
    hooks: committing its version separately from its table is unsafe. Raw source
    fields are preserved; no standardised property transform is implied.
    """

    def __init__(self, config=None, debug=False):
        self.config = config or load_fetcher_config(base_url=ARCHIVE_URL)
        self.debug = debug
        if self.config.base_url != ARCHIVE_URL:
            raise ValueError("JARVIS fetch requires the pinned official archive URL")
        identifier(self.config.table_name)
        if not 1 <= self.config.page_limit <= 10000 or self.config.page_offset != 0:
            raise ValueError("JARVIS requires batch size 1..10000 and offset zero")

    def fetch(self):
        # A fresh target prevents mixing releases or retaining stale rows. A
        # concurrent publisher is also protected by PostgreSQL's rename check.
        versions = DatasetVersions(self.config.db_conn_str)
        try:
            versions.create_table()
            with versions.conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s)", (self.config.table_name,))
                if cursor.fetchone()[0] is not None:
                    raise ValueError(
                        "JARVIS destination exists; choose a fresh release table"
                    )
        finally:
            versions.close()
        session = create_session()
        try:
            path = cache_archive(
                session,
                ARCHIVE_URL,
                get_cache_dir() / "jarvis-64391379.zip",
                ARCHIVE_MD5,
            )
            receipt = {
                "source": "jarvis_dft",
                "version": ARCHIVE_VERSION,
                "release_date": "2025-09-24",
                "url": ARCHIVE_URL,
                "archive_md5": ARCHIVE_MD5,
                "archive_sha256": file_hash(path, "sha256"),
                "scope": "raw source values; functional/protocol-specific labels",
            }
            staging = "jarvis_stage_" + uuid.uuid4().hex
            db = StructuresDatabase(self.config.db_conn_str, staging)
            count = ingest(
                db,
                iter_records(path),
                self.config.page_limit,
                receipt,
                self.config.table_name,
                ARCHIVE_VERSION + "-" + ARCHIVE_MD5,
            )
            logger.info(f"Stored {count} raw JARVIS-DFT records from {ARCHIVE_VERSION}")
            return count
        finally:
            session.close()
