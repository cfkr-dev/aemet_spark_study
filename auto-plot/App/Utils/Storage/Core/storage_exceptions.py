"""Storage-related exception classes.

This module contains small exception types used by the storage helpers to
provide clearer error semantics (for example when an S3 operation
fails or a requested local file does not exist).
"""


class StorageException(Exception):
    """Base exception for storage-related errors.

    :param message: Human-readable error description.
    :param cause: Optional original exception instance that triggered this error.
    """
    def __init__(self, message, cause=None):
        super().__init__(message)
        self.cause = cause


class StoragePrefixNotFound(StorageException):
    """Raised when the configured storage prefix is missing or invalid."""
    def __init__(self):
        super().__init__("Storage prefix not valid")


class LocalFileNotFoundException(StorageException):
    """Raised when a requested local file or directory is not found.

    :param path: The missing filesystem path.
    :type path: str
    """
    def __init__(self, path: str):
        super().__init__(f"Local file not found: {path}")


class S3OperationException(StorageException):
    """Raised when an S3 operation (download/upload) fails.

    :param bucket: S3 bucket involved in the failed operation.
    :param key: S3 key involved in the failed operation.
    :param cause: Optional underlying exception.
    """
    def __init__(self, bucket: str, key: str, cause=None):
        super().__init__(f"S3 operation failed for s3://{bucket}/{key}", cause)
