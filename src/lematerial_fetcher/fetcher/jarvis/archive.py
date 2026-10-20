# Copyright 2026 Entalpic
"""Bounded, checksum-verified JARVIS-DFT raw archive handling."""

import hashlib
import io
import os
import tempfile
import zipfile
from pathlib import Path

from lematerial_fetcher.models.models import RawStructure

ARCHIVE_URL = "https://ndownloader.figshare.com/files/64391379"
ARCHIVE_MD5 = "c3179161ef0d029cf1fa70da463aac6e"
ARCHIVE_VERSION = "jarvis-dft-3d-2025-09-24-file64391379"
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_JSON_BYTES = 2 * 1024 * 1024 * 1024


def file_hash(path, algorithm="md5"):
    """Publisher MD5 checks identity; also retain SHA256 ingestion receipts."""
    digest = hashlib.new(algorithm)
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cache_archive(session, url, destination, expected_md5):
    """Publish only a complete, pinned archive; bound compressed bytes and I/O waits."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.stat().st_size > MAX_ARCHIVE_BYTES:
            raise ValueError("cached archive exceeds byte limit")
        if file_hash(destination) != expected_md5:
            raise ValueError("cached archive checksum mismatch")
        return destination
    temporary = None
    try:
        with session.get(url, stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()
            if int(response.headers.get("content-length", 0)) > MAX_ARCHIVE_BYTES:
                raise ValueError("archive exceeds byte limit")
            fd, temporary = tempfile.mkstemp(
                prefix=destination.name + ".", suffix=".part", dir=destination.parent
            )
            with os.fdopen(fd, "wb") as stream:
                size = 0
                for chunk in response.iter_content(chunk_size=65536):
                    size += len(chunk)
                    if size > MAX_ARCHIVE_BYTES:
                        raise ValueError("archive exceeds byte limit")
                    stream.write(chunk)
            if file_hash(temporary) != expected_md5:
                raise ValueError("downloaded archive checksum mismatch")
            os.replace(temporary, destination)
        return destination
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)


def iter_records(path, expected_md5=ARCHIVE_MD5, *, parser=None):
    """Stream raw rows without merging OPT/TBmBJ labels or applying MP corrections.

    ``parser`` is injectable for small offline fixtures; production uses the
    existing ijson dependency and does not load the whole array in memory.
    """
    if Path(path).stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError("archive exceeds byte limit")
    if file_hash(path) != expected_md5:
        raise ValueError("archive checksum mismatch")
    if parser is None:
        import ijson

        def parser(stream):
            return ijson.items(stream, "item", use_float=True)

    with zipfile.ZipFile(path) as archive:
        members = [v for v in archive.infolist() if not v.is_dir()]
        if len(members) != 1 or not members[0].filename.endswith(".json"):
            raise ValueError("expected exactly one JSON archive member")
        if members[0].file_size > MAX_JSON_BYTES or members[0].flag_bits & 1:
            raise ValueError("unsupported JSON member size/encryption")
        seen = set()
        # UTF-8 decoding is strict; malformed source data must fail the fetch.
        with (
            archive.open(members[0]) as binary,
            io.TextIOWrapper(binary, encoding="utf-8") as stream,
        ):
            # Require an array root even when a parser would accept an object
            # with a property named "item". ZIP streams support rewind.
            char = stream.read(1)
            while char and char.isspace():
                char = stream.read(1)
            if char != "[":
                raise ValueError("expected a JSON array root")
            stream.seek(0)
            for row in parser(stream):
                if (
                    not isinstance(row, dict)
                    or not isinstance(row.get("jid"), str)
                    or not row["jid"].startswith("JVASP-")
                    or not isinstance(row.get("atoms"), dict)
                ):
                    raise ValueError("invalid JARVIS row identity/atoms")
                if row["jid"] in seen:
                    raise ValueError("duplicate JARVIS identity in pinned archive")
                seen.add(row["jid"])
                yield row


def store_records(records, insert_batch, batch_size, receipt):
    """Bound database batches; preserve raw properties and label-specific methods."""
    if not 1 <= batch_size <= 10000:
        raise ValueError("JARVIS batch size must be1..10000")
    pending = []
    count = 0
    for row in records:
        if "_lematerial_ingestion" in row:
            raise ValueError("reserved ingestion metadata key already present")
        pending.append(
            RawStructure(
                id=row["jid"],
                type="structures",
                attributes=dict(row, _lematerial_ingestion=dict(receipt)),
                last_modified=None,
            )
        )
        count += 1
        if len(pending) >= batch_size:
            insert_batch(pending)
            pending = []
    if not count:
        raise ValueError("no JARVIS rows; version not published")
    if pending:
        insert_batch(pending)
    return count
