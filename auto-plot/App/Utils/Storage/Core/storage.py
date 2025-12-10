from pathlib import Path

from App.Utils.Storage.Core.local_storage_backend import LocalStorageBackend
from App.Utils.Storage.Core.s3_storage_backend import S3StorageBackend
from App.Utils.Storage.Core.storage_exceptions import StorageException
from App.Utils.Storage.Core.storage_exceptions import StoragePrefixNotFound


class Storage:

    def __init__(self, storage_prefix: str | None, s3_endpoint: str | None = None):
        if storage_prefix is None:
            raise StoragePrefixNotFound()

        self.prefix = storage_prefix
        self.s3_endpoint = s3_endpoint

    def parse_s3(self, path: str):
        clean = path.removeprefix("s3://")
        parts = clean.split("/", 1)

        if len(parts) != 2 or not parts[0].strip():
            raise ValueError(f"Invalid S3 path: {path}. Expected s3://bucket/key")

        return parts[0], parts[1]

    def select_s3_or_local(self, path: str):
        if path.startswith("s3://"):
            bucket, key = self.parse_s3(path)
            return bucket, key, True
        return "", path, False

    def exists(self, path: str) -> bool:
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)

        try:
            if is_s3:
                return S3StorageBackend.exists(bucket, key, self.s3_endpoint)
            else:
                return LocalStorageBackend.exists(key)
        except Exception:
            return False

    def read(self, path: str) -> Path:
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)
        try:
            if is_s3:
                return S3StorageBackend.read(bucket, key, self.s3_endpoint)
            return LocalStorageBackend.read(key)
        except Exception as ex:
            raise StorageException(f"Failed to read from path: {self.prefix + path}", ex)

    def read_directory(self, path: str) -> Path:
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)
        try:
            if is_s3:
                return S3StorageBackend.read_directory(bucket, key, self.s3_endpoint)
            else:
                return LocalStorageBackend.read_directory(key)
        except Exception as ex:
            raise StorageException(f"Failed to read directory from path: {self.prefix + path}", ex)

    def write(self, path: str, local_file: Path):
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)
        try:
            if is_s3:
                S3StorageBackend.write(bucket, key, local_file, self.s3_endpoint)
            else:
                LocalStorageBackend.write(key, local_file)
        except Exception as ex:
            raise StorageException(f"Failed to write to path: {self.prefix + path}", ex)

    def copy(self, src: str, dst: str):
        try:
            tmp = self.read(src)
            self.write(dst, tmp)
        except Exception as ex:
            raise StorageException(f"Failed to copy from {src} to {dst}", ex)
