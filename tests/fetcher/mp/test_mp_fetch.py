# Copyright 2025 Entalpic
import gzip
import json
from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from lematerial_fetcher.database.postgres import DatasetVersions, StructuresDatabase
from lematerial_fetcher.fetch import ItemsInfo
from lematerial_fetcher.fetcher.mp.fetch import MPFetcher
from lematerial_fetcher.fetcher.mp.utils import (
    add_jsonl_file_to_db,
    add_s3_object_to_db,
)
from lematerial_fetcher.utils.config import FetcherConfig


@pytest.fixture
def mock_aws_client():
    client = MagicMock()
    return client


@pytest.fixture
def mock_db():
    db = MagicMock(spec=StructuresDatabase)
    return db


@pytest.fixture
def mock_version_db():
    """Create a mock version database."""
    db = MagicMock(spec=DatasetVersions)
    return db


@pytest.fixture
def sample_structure_data():
    return {
        "material_id": "mp-123",
        "last_updated": {"$date": "2024-03-14"},
        "elements": ["Si", "O"],
        "nelements": 2,
    }


@pytest.fixture
def mock_config():
    return FetcherConfig(
        base_url="https://api.test.com",
        db_conn_str="postgresql://test:test@localhost:5432/test",
        table_name="test_table",
        page_limit=10,
        page_offset=0,
        mp_bucket_name="test-bucket",
        mp_bucket_prefix="test/prefix",
        log_dir="./logs",
        max_retries=3,
        num_workers=2,
        retry_delay=2,
        log_every=100,
        cache_dir="/tmp/lematerial_fetcher",
    )


@pytest.fixture
def sample_task_data():
    return {
        "task_id": "mp-task-123",
        "last_updated": {"$date": "2024-03-14"},
        "elements": ["Si", "O"],
        "nelements": 2,
    }


def create_gzipped_jsonl(data_list):
    """Helper function to create gzipped JSONL content"""
    json_lines = "\n".join(json.dumps(d) for d in data_list)
    bio = BytesIO()
    with gzip.GzipFile(fileobj=bio, mode="wb") as gz:
        gz.write(json_lines.encode())
    bio.seek(0)
    return {"Body": bio}


def test_add_s3_object_to_db_structure(mock_aws_client, mock_db, sample_structure_data):
    """Test processing a structure S3 object"""
    bucket_name = "test-bucket"
    object_key = "test/path/data.jsonl.gz"
    mock_aws_client.get_object.return_value = create_gzipped_jsonl(
        [sample_structure_data]
    )

    add_s3_object_to_db(mock_aws_client, bucket_name, object_key, mock_db)

    mock_aws_client.get_object.assert_called_once_with(
        Bucket=bucket_name, Key=object_key
    )
    mock_db.batch_insert_data.assert_called_once()


def test_add_jsonl_file_to_db_structure(mock_db, sample_structure_data):
    """Test processing a JSONL file containing structure data"""
    gzipped_data = create_gzipped_jsonl([sample_structure_data])["Body"]

    # decompress the gzipped data before passing to add_jsonl_file_to_db
    with gzip.GzipFile(fileobj=gzipped_data, mode="rb") as gz:
        decompressed_data = BytesIO(gz.read())

    add_jsonl_file_to_db(decompressed_data, mock_db)

    mock_db.batch_insert_data.assert_called_once()


def test_add_jsonl_file_handles_invalid_json(mock_db):
    """Test handling of invalid JSON data"""
    invalid_json = b'{"invalid": "json"\n{"broken": "line"}'
    bio = BytesIO()
    with gzip.GzipFile(fileobj=bio, mode="wb") as gz:
        gz.write(invalid_json)
    bio.seek(0)

    add_jsonl_file_to_db(bio, mock_db)

    mock_db.insert_data.assert_not_called()


def test_is_critical_error():
    """Test critical error detection"""
    assert MPFetcher.is_critical_error(Exception("Connection refused"))
    assert MPFetcher.is_critical_error(Exception("No such host"))
    assert MPFetcher.is_critical_error(Exception("Connection reset"))

    assert not MPFetcher.is_critical_error(Exception("Other error"))
    assert not MPFetcher.is_critical_error(None)


class TestMPFetcher:
    def test_setup_resources(
        self, mock_config, mock_aws_client, mock_db, mock_version_db
    ):
        """Test resource setup initializes AWS client and database"""
        with (
            patch(
                "lematerial_fetcher.fetcher.mp.fetch.get_aws_client"
            ) as mock_get_client,
            patch("lematerial_fetcher.fetch.StructuresDatabase") as mock_db_class,
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
        ):
            mock_get_client.return_value = mock_aws_client
            mock_db_class.return_value = mock_db
            mock_version_db_class.return_value = mock_version_db

            fetcher = MPFetcher(config=mock_config)
            fetcher.setup_resources()

            assert fetcher.aws_client is not None
            assert fetcher.db is not None
            mock_get_client.assert_called_once()
            assert mock_db_class.call_count == 2
            mock_version_db_class.assert_called_once()

    def test_get_items_to_process_empty(
        self, mock_aws_client, mock_config, mock_version_db
    ):
        """Test handling of empty bucket"""
        with (
            patch("lematerial_fetcher.utils.aws.get_aws_client") as mock_get_client,
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
        ):
            mock_get_client.return_value = mock_aws_client
            mock_version_db_class.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            # Setup paginator to return empty list
            mock_paginator = MagicMock()
            mock_aws_client.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [{"Contents": []}]

            fetcher = MPFetcher(config=mock_config)
            fetcher.aws_client = mock_aws_client

            items_info = fetcher.get_items_to_process()
            assert items_info.total_count == 0
            assert len(items_info.items) == 0

    def test_get_items_to_process_filters_correctly(
        self, mock_aws_client, mock_config, mock_version_db
    ):
        """Test filtering of S3 objects"""
        now = datetime.now()

        # Setup paginator with test data
        mock_paginator = MagicMock()
        mock_aws_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "test/prefix/data1.jsonl.gz",
                        "LastModified": now - timedelta(days=1),
                    },
                    {
                        "Key": "test/prefix/manifest.jsonl.gz",
                        "LastModified": now,
                    },
                    {
                        "Key": "test/prefix/data2.txt",
                        "LastModified": now,
                    },
                    {
                        "Key": "test/prefix/data3.jsonl.gz",
                        "LastModified": now,
                    },
                ]
            }
        ]

        with (
            patch("lematerial_fetcher.utils.aws.get_aws_client") as mock_get_client,
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
        ):
            mock_get_client.return_value = mock_aws_client
            mock_version_db_class.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            fetcher = MPFetcher(config=mock_config)
            fetcher.aws_client = mock_aws_client

            items_info = fetcher.get_items_to_process()

            mock_aws_client.get_paginator.assert_called_once_with("list_objects_v2")
            mock_paginator.paginate.assert_called_once_with(
                Bucket=mock_config.mp_bucket_name, Prefix=mock_config.mp_bucket_prefix
            )

            assert items_info.total_count == 2
            assert all(key.endswith(".jsonl.gz") for key in items_info.items)
            assert "test/prefix/manifest.jsonl.gz" not in items_info.items
            assert set(items_info.items) == {
                "test/prefix/data1.jsonl.gz",
                "test/prefix/data3.jsonl.gz",
            }

    def test_get_items_to_process_with_version(
        self, mock_aws_client, mock_config, mock_version_db
    ):
        """Test filtering of S3 objects with version date"""
        now = datetime.now()
        version_date = now - timedelta(days=2)

        # Setup paginator with test data
        mock_paginator = MagicMock()
        mock_aws_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "test/prefix/data1.jsonl.gz",
                        "LastModified": now - timedelta(days=3),  # older than version
                    },
                    {
                        "Key": "test/prefix/data2.jsonl.gz",
                        "LastModified": now - timedelta(days=1),  # newer than version
                    },
                ]
            }
        ]

        with (
            patch("lematerial_fetcher.utils.aws.get_aws_client") as mock_get_client,
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
        ):
            mock_get_client.return_value = mock_aws_client
            mock_version_db_class.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = (
                version_date.strftime("%Y-%m-%d")
            )

            fetcher = MPFetcher(config=mock_config)
            fetcher.aws_client = mock_aws_client

            items_info = fetcher.get_items_to_process()

            assert items_info.total_count == 1
            assert items_info.items == ["test/prefix/data2.jsonl.gz"]

    def test_process_items_handles_errors(
        self, mock_aws_client, mock_config, mock_db, mock_version_db, caplog
    ):
        """Test error handling during item processing"""
        with (
            patch(
                "lematerial_fetcher.fetcher.mp.fetch.get_aws_client"
            ) as mock_get_client,
            patch("lematerial_fetcher.fetch.StructuresDatabase") as mock_db_class,
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
        ):
            mock_get_client.return_value = mock_aws_client
            mock_db_class.return_value = mock_db
            mock_version_db_class.return_value = mock_version_db
            mock_aws_client.get_object.side_effect = Exception("Test error")

            fetcher = MPFetcher(config=mock_config)
            fetcher.setup_resources()  # This will properly set up the database connections

            items_info = ItemsInfo(
                start_offset=0, total_count=1, items=["test/key.jsonl.gz"]
            )

            fetcher.process_items(items_info)

            mock_aws_client.get_object.side_effect = Exception("Connection refused")
            fetcher.process_items(items_info)

    def test_get_new_version(self, mock_config, mock_version_db):
        """Test getting new version from latest modified timestamp"""
        now = datetime.now()
        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
            patch("lematerial_fetcher.fetch.StructuresDatabase") as mock_db_class,
        ):
            mock_version_db_class.return_value = mock_version_db
            mock_db_class.return_value = mock_db_class

            fetcher = MPFetcher(config=mock_config)
            fetcher.setup_resources()
            fetcher.latest_modified = now

            version = fetcher.get_new_version()
            assert version == now.strftime("%Y-%m-%d")

    def test_get_new_version_fallback(self, mock_config, mock_version_db):
        """Test getting new version fallback when no timestamp available"""
        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_version_db_class,
            patch("lematerial_fetcher.fetch.StructuresDatabase") as mock_db_class,
        ):
            mock_version_db_class.return_value = mock_version_db
            mock_db_class.return_value = mock_db_class

            fetcher = MPFetcher(config=mock_config)
            fetcher.setup_resources()
            fetcher.latest_modified = None

            version = fetcher.get_new_version()
            today = datetime.now().strftime("%Y-%m-%d")
            assert version == today
