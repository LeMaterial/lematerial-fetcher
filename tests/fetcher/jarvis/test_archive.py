"""Small offline fixtures only; no live database/archive download."""

import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from typing import ClassVar


class ArchiveTests(unittest.TestCase):
    def fixture(self, directory, rows):
        path = Path(directory) / "tiny.zip"
        with zipfile.ZipFile(path, "w") as z:
            z.writestr("data.json", json.dumps(rows))
        return path, hashlib.md5(path.read_bytes()).hexdigest()

    def test_raw_properties_remain_separate_and_zero_is_not_missing(self):
        from lematerial_fetcher.fetcher.jarvis.archive import iter_records

        row = {
            "jid": "JVASP-1",
            "atoms": {"elements": ["Si"]},
            "optb88vdw_bandgap": 0.0,
            "mbj_bandgap": 1.2,
            "bulk_modulus_kv": 97.1,
            "elastic_tensor": {"cij": [[1.0]]},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path, md5 = self.fixture(tmp, [row])
            records = list(iter_records(path, md5, parser=lambda f: iter(json.load(f))))
            self.assertEqual(records, [row])

    def test_bad_checksum_and_duplicate_identity_fail(self):
        from lematerial_fetcher.fetcher.jarvis.archive import iter_records

        row = {"jid": "JVASP-1", "atoms": {}}
        with tempfile.TemporaryDirectory() as tmp:
            path, md5 = self.fixture(tmp, [row, row])
            with self.assertRaisesRegex(ValueError, "checksum"):
                list(iter_records(path, "0" * 32, parser=lambda f: iter(json.load(f))))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                list(iter_records(path, md5, parser=lambda f: iter(json.load(f))))

    def test_multiple_json_members_and_unknown_id_refused(self):
        from lematerial_fetcher.fetcher.jarvis.archive import iter_records

        with tempfile.TemporaryDirectory() as tmp:
            path, md5 = self.fixture(tmp, [{"jid": "bad", "atoms": {}}])
            with self.assertRaisesRegex(ValueError, "identity"):
                list(iter_records(path, md5, parser=lambda f: iter(json.load(f))))
            with zipfile.ZipFile(path, "a") as z:
                z.writestr("second.json", "[]")
            md5 = hashlib.md5(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, "one JSON"):
                list(iter_records(path, md5, parser=lambda f: iter(json.load(f))))

    def test_download_failure_does_not_publish_partial_cache(self):
        from lematerial_fetcher.fetcher.jarvis.archive import cache_archive

        class Response:
            headers: ClassVar[dict] = {}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size):
                yield b"partial"
                raise OSError("fixture interruption")

        class Session:
            def get(self, *args, **kwargs):
                assert kwargs["timeout"] == (10, 60)
                return Response()

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(OSError):
                cache_archive(
                    Session(),
                    "https://example.test/pinned.zip",
                    Path(tmp) / "archive.zip",
                    "0" * 32,
                )
            self.assertFalse((Path(tmp) / "archive.zip").exists())
            self.assertFalse(list(Path(tmp).glob("*.part")))

    def test_download_byte_limit_is_enforced(self):
        from lematerial_fetcher.fetcher.jarvis.archive import cache_archive

        class Response:
            headers: ClassVar[dict] = {"content-length": "999999999999"}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def raise_for_status(self):
                pass

        class Session:
            def get(self, *args, **kwargs):
                return Response()

        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaisesRegex(ValueError, "byte limit"),
        ):
            cache_archive(
                Session(),
                "https://example.test/pinned.zip",
                Path(tmp) / "archive.zip",
                "0" * 32,
            )

    def test_bounded_store_preserves_provenance_without_relabeling(self):
        from lematerial_fetcher.fetcher.jarvis.archive import store_records

        rows = [
            {
                "jid": "JVASP-" + str(i),
                "atoms": {},
                "optb88vdw_bandgap": 0,
                "mbj_bandgap": 1.2,
            }
            for i in range(5)
        ]
        batches = []
        count = store_records(
            iter(rows),
            lambda batch: batches.append(list(batch)),
            2,
            {"source": "jarvis_dft", "archive_sha256": "fixture"},
        )
        self.assertEqual(count, 5)
        self.assertEqual([len(batch) for batch in batches], [2, 2, 1])
        self.assertEqual(batches[0][0].attributes["optb88vdw_bandgap"], 0)
        self.assertEqual(
            batches[0][0].attributes["_lematerial_ingestion"]["source"], "jarvis_dft"
        )
        self.assertNotIn("_lematerial_ingestion", rows[0])

    def test_empty_source_or_insert_failure_never_returns_success(self):
        from lematerial_fetcher.fetcher.jarvis.archive import store_records

        with self.assertRaisesRegex(ValueError, "no JARVIS"):
            store_records(iter([]), lambda batch: None, 2, {})

        def fail(batch):
            raise RuntimeError("database fixture")

        with self.assertRaisesRegex(RuntimeError, "database fixture"):
            store_records(iter([{"jid": "JVASP-1", "atoms": {}}]), fail, 1, {})

    def test_wrong_root_is_rejected_and_release_is_not_row_timestamp(self):
        from lematerial_fetcher.fetcher.jarvis.archive import (
            iter_records,
            store_records,
        )

        with tempfile.TemporaryDirectory() as tmp:
            path, md5 = self.fixture(tmp, {"item": {"jid": "JVASP-1", "atoms": {}}})
            with self.assertRaisesRegex(ValueError, "array root"):
                list(iter_records(path, md5, parser=lambda f: [json.load(f)["item"]]))
        batches = []
        store_records(
            [{"jid": "JVASP-1", "atoms": {}}],
            batches.extend,
            1,
            {"release_date": "2025-09-24"},
        )
        self.assertIsNone(batches[0].last_modified)

    def test_production_ijson_parser(self):
        try:
            import ijson
        except ImportError:
            self.skipTest(
                "ijson unavailable in offline runtime; upstream dependencies required"
            )
        from lematerial_fetcher.fetcher.jarvis.archive import iter_records

        with tempfile.TemporaryDirectory() as tmp:
            row = {"jid": "JVASP-1", "atoms": {}}
            path, md5 = self.fixture(tmp, [row])
            self.assertEqual(list(iter_records(path, md5)), [row])
            for raw in [
                '[{"jid":"JVASP-1","atoms":{}}',
                "[] []",
                '{"item":{"jid":"JVASP-1","atoms":{}}}',
            ]:
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("data.json", raw)
                md5 = hashlib.md5(path.read_bytes()).hexdigest()
                with self.assertRaises((ValueError, ijson.JSONError)):
                    list(iter_records(path, md5))
