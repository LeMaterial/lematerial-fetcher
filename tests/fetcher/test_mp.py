import gzip
import json
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from material_fetcher.database.postgres import StructuresDatabase
from material_fetcher.fetcher.mp.utils import (
    add_jsonl_file_to_db,
    add_s3_object_to_db,
    is_critical_error,
)


@pytest.fixture
def mock_aws_client():
    client = MagicMock()
    return client


@pytest.fixture
def mock_db():
    db = MagicMock(spec=StructuresDatabase)
    return db


@pytest.fixture
def sample_structure_data():
    return {
        "material_id": "mp-123",
        "last_updated": "2024-03-14",
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
    mock_db.insert_data.assert_called_once()


def test_add_jsonl_file_to_db_structure(mock_db, sample_structure_data):
    """Test processing a JSONL file containing structure data"""
    gzipped_data = create_gzipped_jsonl([sample_structure_data])["Body"]

    # decompress the gzipped data before passing to add_jsonl_file_to_db
    with gzip.GzipFile(fileobj=gzipped_data, mode="rb") as gz:
        decompressed_data = BytesIO(gz.read())

    add_jsonl_file_to_db(decompressed_data, mock_db)

    mock_db.insert_data.assert_called_once()


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
    assert is_critical_error(Exception("Connection refused"))
    assert is_critical_error(Exception("No such host"))
    assert is_critical_error(Exception("Connection reset"))

    assert not is_critical_error(Exception("Other error"))
    assert not is_critical_error(None)
