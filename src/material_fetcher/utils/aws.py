# Copyright 2025 Entalpic
import io
from typing import Any, List

import boto3
from botocore import UNSIGNED
from botocore.config import Config


def get_aws_client(region_name: str = "us-east-1"):
    """Returns a configured S3 client for accessing Materials Project data

    Parameters
    ----------
    region_name: str, default='us-east-1'
        The region of the S3 bucket. By default, the Materials Project bucket is in us-east-1.

    Returns
    -------
    s3_client: boto3.client
        A configured S3 client with anonymous credentials
    """
    # configure the client with anonymous credentials
    s3_client = boto3.client(
        "s3", config=Config(signature_version=UNSIGNED, region_name=region_name)
    )
    return s3_client


def get_latest_collection_version_prefix(
    client, bucket_name: str, bucket_prefix: str, collections_prefix: str
) -> str:
    """Returns the latest version prefix path from a collections directory in S3.

    Parameters
    ----------
    client : boto3.client
        The configured S3 client
    bucket_name : str
        Name of the S3 bucket
    bucket_prefix : str
        Base prefix path in the bucket (e.g. 'collections')
    collections_prefix : str
        Additional prefix to append after the date (e.g. 'materials')

    Returns
    -------
    str
        Complete S3 prefix path including the latest date directory
        Format: {bucket_prefix}/YYYY-MM-DD/{collections_prefix}

    Raises
    ------
    ValueError
        If no date directories are found under the bucket_prefix
    """
    response = client.list_objects_v2(
        Bucket=bucket_name, Prefix=f"{bucket_prefix}/", Delimiter="/"
    )

    if "CommonPrefixes" not in response or not response["CommonPrefixes"]:
        raise ValueError("No date directories found in collections/")

    latest_prefix = max(prefix["Prefix"] for prefix in response["CommonPrefixes"])

    if latest_prefix.endswith("/"):
        latest_prefix = latest_prefix[:-1]

    # construct the final path: collections/YYYY-MM-DD/materials
    return f"{latest_prefix}/{collections_prefix}"


def list_s3_objects(client, bucket_name: str, prefix: str) -> List[dict[str, Any]]:
    """Lists all objects in the specified S3 bucket with the given prefix.

    Parameters
    ----------
    client : boto3.client
        The configured S3 client
    bucket_name : str
        Name of the S3 bucket
    prefix : str
        Prefix path to list objects from

    Returns
    -------
    List[dict[str, Any]]
        List of objects matching the prefix, with both full path and metadata
    """
    object_keys = []
    paginator = client.get_paginator("list_objects_v2")

    # paginate through the objects
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            object_keys.extend(
                {
                    "key": obj["Key"],
                    "metadata": {
                        "LastModified": obj.get("LastModified"),
                        "ContentLength": obj.get("ContentLength"),
                        "ETag": obj.get("ETag"),
                    },
                }
                for obj in page["Contents"]
            )

    return object_keys


def download_s3_object(client, bucket_name: str, object_key: str) -> io.IOBase:
    """Downloads an object from S3 and returns it as a file-like object.

    Parameters
    ----------
    client : boto3.client
        The configured S3 client
    bucket_name : str
        Name of the S3 bucket
    object_key : str
        Full path/key of the object to download

    Returns
    -------
    io.IOBase
        File-like object containing the downloaded data
    """
    response = client.get_object(Bucket=bucket_name, Key=object_key)
    return response["Body"]


def get_s3_object_metadata(client: Any, bucket: str, key: str) -> dict:
    """
    Get metadata for an S3 object.

    Parameters
    ----------
    client : Any
        Boto3 S3 client
    bucket : str
        Name of the S3 bucket
    key : str
        Key of the S3 object

    Returns
    -------
    dict
        Object metadata including LastModified timestamp
    """
    try:
        response = client.head_object(Bucket=bucket, Key=key)
        return {
            "LastModified": response.get("LastModified"),
            "ContentLength": response.get("ContentLength"),
            "ETag": response.get("ETag"),
        }
    except Exception as e:
        raise Exception(f"Error getting metadata for {key}: {str(e)}")
