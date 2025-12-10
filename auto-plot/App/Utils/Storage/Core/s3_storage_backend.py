import tempfile
import uuid
from pathlib import Path

import boto3
from botocore.client import Config

from App.Utils.Storage.Core.storage_exceptions import S3OperationException


class S3StorageBackend:
    TEMP_DIR = Path(tempfile.gettempdir())

    @staticmethod
    def temp_path_for_key(key: str):
        ext = Path(key).suffix
        file_name = f"s3reader-{uuid.uuid4()}{ext}"
        return S3StorageBackend.TEMP_DIR / file_name

    @staticmethod
    def create_client(endpoint: str | None):
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
        client = S3StorageBackend.create_client(endpoint)
        tmp = S3StorageBackend.temp_path_for_key(key)

        try:
            client.download_file(bucket, key, str(tmp))
            return tmp
        except Exception as ex:
            raise S3OperationException(bucket, key, ex)

    @staticmethod
    def read_directory(bucket: str, prefix: str, endpoint: str | None = None):
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
        client = S3StorageBackend.create_client(endpoint)

        try:
            client.upload_file(str(local_path), bucket, key)
        except Exception as ex:
            raise S3OperationException(bucket, key, ex)
