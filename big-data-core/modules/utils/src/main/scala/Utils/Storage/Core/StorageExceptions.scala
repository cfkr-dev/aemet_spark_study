package Utils.Storage.Core

class StorageException(message: String, cause: Throwable = null)
  extends Exception(message, cause)

class StoragePrefixNotFound()
  extends StorageException("Storage prefix not valid")

class LocalFileNotFoundException(path: String)
  extends StorageException(s"Local file not found: $path")

class S3OperationException(bucket: String, key: String, cause: Throwable = null)
  extends StorageException(s"S3 operation failed for s3://$bucket/$key", cause)
