"""Amazon S3 storage backend helpers.

This module wraps common S3 operations using ``boto3`` and exposes a
simple interface similar to the local backend. The helpers return
local temporary paths for read operations to keep the interface
consistent across backends.
"""

import tempfile
import uuid
from pathlib import Path

import boto3
from botocore.client import Config

from App.Utils.Storage.Core.storage_exceptions import S3OperationException


class S3StorageBackend:
    """Adapter that performs file and directory operations on S3.

    The implementation uses temporary local files/directories for
    read operations and raises :class:`S3OperationException` when
    boto3 calls fail.
    """
    TEMP_DIR = Path(tempfile.gettempdir())

    @staticmethod
    def temp_path_for_key(key: str):
        """Return a temporary file path for the given S3 key.

        :param key: S3 object key (used to preserve extension).
        :type key: str
        :returns: Path to a temporary filename in the system temp directory.
        :rtype: pathlib.Path
        """
        ext = Path(key).suffix
        file_name = f"s3reader-{uuid.uuid4()}{ext}"
        return S3StorageBackend.TEMP_DIR / file_name

    @staticmethod
    def create_client(endpoint: str | None):
        """Create and return a boto3 S3 client.

        When an ``endpoint`` is provided the client is configured to use
        that URL (useful for LocalStack or custom S3-compatible endpoints).

        :param endpoint: Optional S3 endpoint URL.
        :type endpoint: str | None
        :returns: Configured boto3 S3 client.
        """
        if endpoint:
            return boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id="test",
                aws_secret_access_key="test",
                region_name="us-east-1",
                config=Config(s3={'addressing_style': 'path'})
            )
        return boto3.client("s3")

    @staticmethod
    def exists(bucket: str, key: str, endpoint: str | None = None) -> bool:
        """Return True if the object or prefix exists in S3.

        The method first attempts a head_object on the exact key; if that
        fails it falls back to list_objects_v2 to check for prefixes
        (useful for directory-like keys).

        :param bucket: S3 bucket name.
        :type bucket: str
        :param key: S3 key or prefix to check.
        :type key: str
        :param endpoint: Optional endpoint URL for custom S3 backends.
        :type endpoint: str | None
        :returns: True when the object or prefix exists, False otherwise.
        :rtype: bool
        """
        client = S3StorageBackend.create_client(endpoint)

        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            pass

        if not key.endswith("/"):
            prefix = key + "/"
        else:
            prefix = key

        try:
            resp = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )
            return "Contents" in resp
        except Exception as ex:
            print(ex)
            return False

    @staticmethod
    def read(bucket: str, key: str, endpoint: str | None = None):
        """Download a single S3 object into a temporary local file and return its path.

        :param bucket: S3 bucket name.
        :type bucket: str
        :param key: S3 object key.
        :type key: str
        :param endpoint: Optional endpoint URL for custom S3 backends.
        :type endpoint: str | None
        :returns: Path to the downloaded temporary file.
        :rtype: pathlib.Path
        :raises S3OperationException: When the download fails.
        """
        client = S3StorageBackend.create_client(endpoint)
        tmp = S3StorageBackend.temp_path_for_key(key)

        try:
            client.download_file(bucket, key, str(tmp))
            return tmp
        except Exception as ex:
            raise S3OperationException(bucket, key, ex)

    @staticmethod
    def read_directory(bucket: str, prefix: str, endpoint: str | None = None):
        """Download all objects under ``prefix`` into a temporary directory.

        The method paginates over objects under the prefix and downloads
        each non-directory object into the returned temporary directory.

        :param bucket: S3 bucket name.
        :type bucket: str
        :param prefix: Prefix that represents a directory-like key.
        :type prefix: str
        :param endpoint: Optional endpoint URL for custom S3 backends.
        :type endpoint: str | None
        :returns: Filesystem path to the directory containing downloaded objects.
        :rtype: str
        :raises S3OperationException: When any download fails.
        """
        client = S3StorageBackend.create_client(endpoint)

        target_dir = tempfile.mkdtemp()

        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith("/"):
                    continue

                dst = Path(target_dir) / Path(key).name
                try:
                    client.download_file(bucket, key, str(dst))
                except Exception as ex:
                    raise S3OperationException(bucket, key, ex)

        return target_dir

    @staticmethod
    def write(bucket: str, key: str, local_path: Path, endpoint: str | None = None):
        """Upload a local file to S3.

        :param bucket: S3 bucket name.
        :type bucket: str
        :param key: Destination S3 key.
        :type key: str
        :param local_path: Local file path to upload.
        :type local_path: pathlib.Path
        :param endpoint: Optional endpoint URL for custom S3 backends.
        :type endpoint: str | None
        :raises S3OperationException: When the upload fails.
        """
        client = S3StorageBackend.create_client(endpoint)

        try:
            client.upload_file(str(local_path), bucket, key)
        except Exception as ex:
            raise S3OperationException(bucket, key, ex)
