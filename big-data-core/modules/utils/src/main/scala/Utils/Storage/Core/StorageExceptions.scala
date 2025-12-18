package Utils.Storage.Core

/**
 * Base exceptions used by the storage backends.
 */
class StorageException(message: String, cause: Throwable = null)
  extends Exception(message, cause)

/**
 * Thrown when the configured storage prefix is missing or invalid.
 */
class StoragePrefixNotFound()
  extends StorageException("Storage prefix not valid")

/**
 * Thrown when a local file expected to exist is not found.
 *
 * @param path the missing file path
 */
class LocalFileNotFoundException(path: String)
  extends StorageException(s"Local file not found: $path")

/**
 * Thrown when an S3 operation (read/write) fails.
 *
 * @param bucket S3 bucket name
 * @param key S3 object key
 * @param cause optional underlying cause
 */
class S3OperationException(bucket: String, key: String, cause: Throwable = null)
  extends StorageException(s"S3 operation failed for s3://$bucket/$key", cause)
