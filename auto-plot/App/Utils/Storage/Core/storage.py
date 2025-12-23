"""High-level storage wrapper that routes paths to local or S3 backends.

The :class:`Storage` object prefixes logical paths with a configured
storage prefix and dispatches operations to either the local filesystem
or an S3-compatible backend depending on the path scheme.
"""

from pathlib import Path

from App.Utils.Storage.Core.local_storage_backend import LocalStorageBackend
from App.Utils.Storage.Core.s3_storage_backend import S3StorageBackend
from App.Utils.Storage.Core.storage_exceptions import StorageException
from App.Utils.Storage.Core.storage_exceptions import StoragePrefixNotFound


class Storage:
    """Main facade that exposes read/write/copy operations over storage.

    :param storage_prefix: Prefix applied to every logical path managed by this storage.
    :type storage_prefix: str | None
    :param s3_endpoint: Optional S3-compatible endpoint used for S3 operations.
    :type s3_endpoint: str | None
    """

    def __init__(self, storage_prefix: str | None, s3_endpoint: str | None = None):
        """Create a new :class:`Storage` instance.

        The constructor requires ``storage_prefix`` which will be prepended
        to every path passed to the instance methods. When ``storage_prefix``
        is None a :class:`StoragePrefixNotFound` exception is raised.
        """
        if storage_prefix is None:
            raise StoragePrefixNotFound()

        self.prefix = storage_prefix
        self.s3_endpoint = s3_endpoint

    def parse_s3(self, path: str):
        """Parse an ``s3://bucket/key`` style path and return (bucket, key).

        :param path: S3 path in the form ``s3://bucket/key``.
        :type path: str
        :returns: Tuple of (bucket, key).
        :rtype: tuple[str, str]
        :raises ValueError: When the path is not a valid S3 URL.
        """
        clean = path.removeprefix("s3://")
        parts = clean.split("/", 1)

        if len(parts) != 2 or not parts[0].strip():
            raise ValueError(f"Invalid S3 path: {path}. Expected s3://bucket/key")

        return parts[0], parts[1]

    def select_s3_or_local(self, path: str):
        """Return (bucket, key, is_s3) for the provided logical path.

        If the path starts with ``s3://`` the method returns the parsed
        (bucket, key, True). Otherwise it returns ("", key, False) where
        ``key`` is the local path component.
        """
        if path.startswith("s3://"):
            bucket, key = self.parse_s3(path)
            return bucket, key, True
        return "", path, False

    def exists(self, path: str) -> bool:
        """Return True when the object at ``path`` exists in the selected backend.

        :param path: Logical path that will be prefixed with the storage prefix.
        :type path: str
        :returns: True when the object exists, False otherwise.
        :rtype: bool
        """
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)

        try:
            if is_s3:
                return S3StorageBackend.exists(bucket, key, self.s3_endpoint)
            else:
                return LocalStorageBackend.exists(key)
        except Exception:
            return False

    def read(self, path: str) -> Path:
        """Read a file referenced by ``path`` and return a local Path to it.

        The method delegates to the chosen backend and wraps exceptions in
        :class:`StorageException` when errors occur.
        """
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)
        try:
            if is_s3:
                return S3StorageBackend.read(bucket, key, self.s3_endpoint)
            return LocalStorageBackend.read(key)
        except Exception as ex:
            raise StorageException(f"Failed to read from path: {self.prefix + path}", ex)

    def read_directory(self, path: str) -> Path:
        """Read a directory (or S3 prefix) and return a temporary directory path.

        :param path: Logical path representing a directory or S3 prefix.
        :type path: str
        :returns: Local temporary directory containing the downloaded files.
        :rtype: pathlib.Path
        :raises StorageException: When the backend read fails.
        """
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)
        try:
            if is_s3:
                return S3StorageBackend.read_directory(bucket, key, self.s3_endpoint)
            else:
                return LocalStorageBackend.read_directory(key)
        except Exception as ex:
            raise StorageException(f"Failed to read directory from path: {self.prefix + path}", ex)

    def write(self, path: str, local_file: Path):
        """Write the local file ``local_file`` to the logical destination ``path``.

        :param path: Logical destination path.
        :type path: str
        :param local_file: Local temporary file to be written to the backend.
        :type local_file: pathlib.Path
        :raises StorageException: When the backend write fails.
        """
        bucket, key, is_s3 = self.select_s3_or_local(self.prefix + path)
        try:
            if is_s3:
                S3StorageBackend.write(bucket, key, local_file, self.s3_endpoint)
            else:
                LocalStorageBackend.write(key, local_file)
        except Exception as ex:
            raise StorageException(f"Failed to write to path: {self.prefix + path}", ex)

    def copy(self, src: str, dst: str):
        """Copy a file or directory from ``src`` to ``dst`` using the storage APIs.

        The implementation reads ``src`` into a temporary local path and writes
        it to ``dst``.
        """
        try:
            tmp = self.read(src)
            self.write(dst, tmp)
        except Exception as ex:
            raise StorageException(f"Failed to copy from {src} to {dst}", ex)
