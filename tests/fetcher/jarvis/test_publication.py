"""Offline transaction contract: no partial target or successful version."""

import unittest
from unittest.mock import MagicMock


class PublicationTests(unittest.TestCase):
    def test_publish_table_and_version_use_one_commit(self):
        from lematerial_fetcher.fetcher.jarvis.publication import publish

        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        publish(conn, "jarvis_stage_123", "jarvis_2025", "version")
        statements = [str(call.args[0]) for call in cursor.execute.call_args_list]
        self.assertIn("RENAME", statements[0])
        self.assertIn("dataset_versions", statements[1])
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()

    def test_version_failure_rolls_back_rename(self):
        from lematerial_fetcher.fetcher.jarvis.publication import publish

        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [None, RuntimeError("version write failed")]
        with self.assertRaisesRegex(RuntimeError, "version write failed"):
            publish(conn, "jarvis_stage_123", "jarvis_2025", "version")
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    def test_reject_unsafe_identifiers_before_sql(self):
        from lematerial_fetcher.fetcher.jarvis.publication import publish

        conn = MagicMock()
        for name in ["public.table", "bad;DROP", "x" * 64, "MixedCase"]:
            with self.assertRaises(ValueError):
                publish(conn, "jarvis_stage_123", name, "version")
        conn.cursor.assert_not_called()

    def test_late_parse_failure_drops_stage_without_publish_and_closes(self):
        from lematerial_fetcher.fetcher.jarvis.publication import ingest

        db = MagicMock()
        db.table_name = "jarvis_stage_123"

        def rows():
            yield {"jid": "JVASP-1", "atoms": {}}
            raise ValueError("late malformed row")

        with self.assertRaisesRegex(ValueError, "late malformed row"):
            ingest(db, rows(), 1, {}, "jarvis_2025", "version")
        db.batch_insert_data.assert_called_once()
        calls = str(
            db.conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        )
        self.assertIn("DROP TABLE", calls)
        self.assertNotIn("RENAME", calls)
        db.close.assert_called_once()

    def test_successful_ingest_closes_after_publish(self):
        from lematerial_fetcher.fetcher.jarvis.publication import ingest

        db = MagicMock()
        db.table_name = "jarvis_stage_123"
        count = ingest(
            db, iter([{"jid": "JVASP-1", "atoms": {}}]), 1, {}, "jarvis_2025", "version"
        )
        self.assertEqual(count, 1)
        db.create_table.assert_called_once()
        db.close.assert_called_once()
        self.assertIn(
            "RENAME",
            str(
                db.conn.cursor.return_value.__enter__.return_value.execute.call_args_list
            ),
        )
