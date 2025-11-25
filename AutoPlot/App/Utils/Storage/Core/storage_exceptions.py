class StorageException(Exception):
    def __init__(self, message, cause=None):
        super().__init__(message)
        self.cause = cause


class StoragePrefixNotFound(StorageException):
    def __init__(self):
        super().__init__("Storage prefix not valid")


class LocalFileNotFoundException(StorageException):
    def __init__(self, path: str):
        super().__init__(f"Local file not found: {path}")


class S3OperationException(StorageException):
    def __init__(self, bucket: str, key: str, cause=None):
        super().__init__(f"S3 operation failed for s3://{bucket}/{key}", cause)
