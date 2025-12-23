"""Local filesystem storage backend helpers.

This module implements simple helpers that operate on the local
filesystem. They are used by :class:`App.Utils.Storage.Core.storage.Storage`
when the requested path resolves to a local file instead of S3.
"""

import shutil
import tempfile
from pathlib import Path

from App.Utils.Storage.Core.storage_exceptions import StorageException, LocalFileNotFoundException


class LocalStorageBackend:
    """Backend helper that reads/writes files from the local filesystem.

    All methods are static and return temporary locations when a read is
    performed to keep a consistent interface with the S3 backend.
    """
    TEMP_DIR = Path(tempfile.gettempdir())

    @staticmethod
    def exists(path: str):
        """Return True when the given local ``path`` exists on disk.

        :param path: Local filesystem path to check.
        :type path: str
        :returns: True when the path exists, False otherwise.
        :rtype: bool
        """
        p = Path(path)
        return p.exists()

    @staticmethod
    def read(path: str):
        """Read a single local file and copy it to a temporary location.

        :param path: Local filesystem path to the source file.
        :type path: str
        :returns: Path to the temporary copy of the file.
        :rtype: pathlib.Path
        :raises LocalFileNotFoundException: When the source file does not exist.
        :raises StorageException: When the copy operation fails.
        """
        src = Path(path)
        if not src.exists():
            raise LocalFileNotFoundException(path)

        dst = LocalStorageBackend.TEMP_DIR / src.name

        try:
            shutil.copy(src, dst)
            return dst
        except Exception as ex:
            raise StorageException(f"Failed to read local file: {path}", ex)

    @staticmethod
    def read_directory(path: str):
        """Read a directory and copy files to a temporary directory.

        The method copies every regular file from ``path`` into a newly
        created temporary directory and returns the path to that
        directory.

        :param path: Local directory path to read.
        :type path: str
        :returns: Path to the temporary directory containing copied files.
        :rtype: str
        :raises LocalFileNotFoundException: When the path does not exist or is not a directory.
        """
        src_dir = Path(path)
        if not src_dir.exists() or not src_dir.is_dir():
            raise LocalFileNotFoundException(path)

        target_dir = tempfile.mkdtemp()

        for file in src_dir.iterdir():
            if file.is_file():
                shutil.copy(file, Path(target_dir) / file.name)

        return target_dir

    @staticmethod
    def write(path: str, local_path: Path):
        """Write a local file from ``local_path`` to the destination ``path``.

        The destination directory is created if it does not exist.

        :param path: Destination local filesystem path where the file will be written.
        :type path: str
        :param local_path: Path to the local temporary file to copy.
        :type local_path: pathlib.Path
        :raises StorageException: When the write/copy operation fails.
        """
        dst = Path(path)
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local_path, dst)
        except Exception as ex:
            raise StorageException(f"Failed to write local file: {path}", ex)
