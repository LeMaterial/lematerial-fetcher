# Copyright 2025 Entalpic
import io

import pytest
from botocore import UNSIGNED
from botocore.response import StreamingBody
from botocore.stub import Stubber

from lematerial_fetcher.utils.aws import (
    download_s3_object,
    get_aws_client,
    get_latest_collection_version_prefix,
    list_s3_objects,
)


def test_get_aws_client():
    """Test that get_aws_client returns a properly configured S3 client"""
    client = get_aws_client()

    assert client._client_config.signature_version == UNSIGNED
    assert client._client_config.region_name == "us-east-1"


@pytest.fixture
def mock_s3_client():
    """Fixture to create a stubbed S3 client"""
    client = get_aws_client()
    stubber = Stubber(client)
    yield stubber, client
    stubber.deactivate()


def test_get_latest_collection_version_prefix_success(mock_s3_client):
    """Test successful retrieval of latest collection version prefix"""
    stubber, client = mock_s3_client

    expected_params = {
        "Bucket": "test-bucket",
        "Prefix": "collections/",
        "Delimiter": "/",
    }
    response = {
        "CommonPrefixes": [
            {"Prefix": "collections/2023-01-01/"},
            {"Prefix": "collections/2023-02-01/"},
            {"Prefix": "collections/2023-03-01/"},
        ],
        "Name": "test-bucket",
        "Prefix": "collections/",
        "Delimiter": "/",
        "MaxKeys": 1000,
        "EncodingType": "url",
    }

    stubber.add_response("list_objects_v2", response, expected_params)

    with stubber:  # This will handle activate/deactivate
        result = get_latest_collection_version_prefix(
            client, "test-bucket", "collections", "materials"
        )

    assert result == "collections/2023-03-01/materials"


def test_get_latest_collection_version_prefix_no_dates(mock_s3_client):
    """Test handling of empty collections directory"""
    stubber, client = mock_s3_client

    expected_params = {
        "Bucket": "test-bucket",
        "Prefix": "collections/",
        "Delimiter": "/",
    }
    response = {
        "Name": "test-bucket",
        "Prefix": "collections/",
        "Delimiter": "/",
        "MaxKeys": 1000,
        "EncodingType": "url",
    }

    stubber.add_response("list_objects_v2", response, expected_params)

    with pytest.raises(ValueError, match="No date directories found in collections/"):
        with stubber:
            get_latest_collection_version_prefix(
                client, "test-bucket", "collections", "materials"
            )


def test_list_s3_objects_success(mock_s3_client):
    """Test successful listing of S3 objects with pagination"""
    stubber, client = mock_s3_client

    expected_params = {"Bucket": "test-bucket", "Prefix": "prefix"}
    response_1 = {
        "Contents": [
            {
                "Key": "prefix/file1.json",
                "LastModified": "2023-01-01T00:00:00.000Z",
                "ETag": '"abc123"',
                "Size": 100,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "prefix/file2.json",
                "LastModified": "2023-01-01T00:00:00.000Z",
                "ETag": '"def456"',
                "Size": 200,
                "StorageClass": "STANDARD",
            },
        ],
        "IsTruncated": True,
        "NextContinuationToken": "token123",
    }

    expected_params_2 = {
        "Bucket": "test-bucket",
        "Prefix": "prefix",
        "ContinuationToken": "token123",
    }
    response_2 = {
        "Contents": [
            {
                "Key": "prefix/file3.json",
                "LastModified": "2023-01-01T00:00:00.000Z",
                "ETag": '"ghi789"',
                "Size": 300,
                "StorageClass": "STANDARD",
            }
        ],
        "IsTruncated": False,
    }

    stubber.add_response("list_objects_v2", response_1, expected_params)
    stubber.add_response("list_objects_v2", response_2, expected_params_2)

    with stubber:
        result = list_s3_objects(client, "test-bucket", "prefix")

    assert result[0]["key"] == "prefix/file1.json"
    assert result[0]["metadata"]["LastModified"] == "2023-01-01T00:00:00.000Z"


def test_list_s3_objects_empty(mock_s3_client):
    """Test listing when no objects exist"""
    stubber, client = mock_s3_client

    expected_params = {"Bucket": "test-bucket", "Prefix": "prefix"}
    response = {"Contents": []}

    stubber.add_response("list_objects_v2", response, expected_params)

    with stubber:
        result = list_s3_objects(client, "test-bucket", "prefix")

    assert result == []


def test_download_s3_object_success(mock_s3_client):
    """Test successful download of S3 object"""
    stubber, client = mock_s3_client

    expected_params = {"Bucket": "test-bucket", "Key": "test-key.json"}

    test_content = b"test content"
    response = {
        "Body": StreamingBody(io.BytesIO(test_content), len(test_content)),
        "ContentLength": len(test_content),
        "LastModified": "2023-01-01T00:00:00.000Z",
        "ETag": '"abc123"',
        "ContentType": "application/json",
    }

    stubber.add_response("get_object", response, expected_params)

    with stubber:
        result = download_s3_object(client, "test-bucket", "test-key.json")

    assert isinstance(result, StreamingBody)
    assert result.read() == test_content


def test_download_s3_object_error(mock_s3_client):
    """Test error handling during S3 object download"""
    stubber, client = mock_s3_client

    expected_params = {"Bucket": "test-bucket", "Key": "test-key.json"}

    stubber.add_client_error(
        "get_object",
        service_error_code="NoSuchKey",
        service_message="The specified key does not exist.",
        http_status_code=404,
        expected_params=expected_params,
    )

    with pytest.raises(Exception):
        with stubber:
            download_s3_object(client, "test-bucket", "test-key.json")
