package Utils.Storage.Core

import java.nio.file.{Files, Path, Paths}

/**
 * Abstraction over storage backends that can handle both local filesystem
 * paths and `s3://` URIs. The `storagePrefix` is prepended to requested
 * paths and an optional `s3Endpoint` can be provided for testing.
 *
 * @param storagePrefix optional prefix to prepend to paths (throws if missing)
 * @param s3Endpoint optional S3 endpoint for testing/localstack
 */
case class Storage(storagePrefix: Option[String], s3Endpoint: Option[String] = None) {

  private val checkedPrefix = storagePrefix.getOrElse(throw new StoragePrefixNotFound())

  /**
   * Parse an `s3://bucket/key` style path into `(bucket, key)`.
   *
   * @param path S3 path string
   * @return tuple `(bucket, key)`
   */
  private def parseS3Path(path: String): (String, String) = {
    val parts = path.stripPrefix("s3://").split("/", 2)

    if (parts.length != 2 || parts(0).trim.isEmpty)
      throw new IllegalArgumentException(s"Invalid S3 path: $path. Expected s3://bucket/key")

    (parts(0), parts(1))
  }

  /**
   * Determine whether a path refers to S3 or local storage.
   *
   * @param path input path (may be prefixed)
   * @return `(bucket, key, isS3)` where `isS3` is true for S3 URIs
   */
  def selectS3orLocal(path: String): (String, String, Boolean) = {
    if (path.startsWith("s3://")) {
      val (bucket, key) = parseS3Path(path)
      (bucket, key, true)
    } else {
      ("", path, false)
    }
  }

  /**
   * Read a file from storage (S3 or local) and return a temporary `Path`.
   *
   * @param path storage path relative to the configured prefix
   * @param prefix optional prefix to override the configured prefix
   * @return temporary `Path` pointing to the read file
   * @throws StorageException on IO failures
   */
  def read(path: String, prefix: String = checkedPrefix): Path = {
    val (bucket, key, isS3) = selectS3orLocal(prefix + path)
    try {
      if (isS3) S3StorageBackend.read(bucket, key, s3Endpoint) else LocalStorageBackend.read(key)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to read from path: $path", exception)
    }
  }

  /**
   * Read a directory (or prefix) recursively and return a temporary directory `Path`.
   *
   * @param dirPath directory path or prefix relative to the configured prefix
   * @param includeDirs list of subdirectories to include (defaults to all)
   * @param prefix optional prefix to override the configured prefix
   * @return temporary `Path` containing the requested subset
   * @throws StorageException on IO failures
   */
  def readDirectoryRecursive(dirPath: String, includeDirs: Seq[String] = Seq.empty, prefix: String = checkedPrefix): Path = {
    val (bucket, key, isS3) = selectS3orLocal(prefix + dirPath)
    try {
      if (isS3) S3StorageBackend.readDirectoryRecursive(bucket, key, s3Endpoint, includeDirs)
      else LocalStorageBackend.readDirectoryRecursive(key, includeDirs)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to read directory recursively: $dirPath", exception)
    }
  }

  /**
   * Write a local temporary file to the storage backend under `path`.
   *
   * @param path destination storage path relative to the configured prefix
   * @param localFile local temporary `Path` to upload
   * @param prefix optional prefix to override the configured prefix
   * @throws StorageException on IO failures
   */
  def write(path: String, localFile: Path, prefix: String = checkedPrefix): Unit = {
    val (bucket, key, isS3) = selectS3orLocal(prefix + path)
    try {
      if (isS3) S3StorageBackend.write(bucket, key, localFile, s3Endpoint) else LocalStorageBackend.write(key, localFile)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to write to path: $path", exception)
    }
  }

  /**
   * Copy a file from `src` to `dst` using the configured storage backend.
   *
   * @param src source path (relative to prefix)
   * @param dst destination path (relative to prefix)
   * @throws StorageException on failures
   */
  def copy(src: String, dst: String): Unit = {
    try {
      val tmp = read(src)
      write(dst, tmp)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to copy from $src to $dst", exception)
    }
  }

  /**
   * Delete a local directory recursively. This operation is only performed
   * on the local filesystem and will be ignored if the path does not exist.
   *
   * @param path local directory path to remove
   */
  def deleteLocalDirectoryRecursive(path: String): Unit = {
    val pathAsPath = Paths.get(path)
    if (Files.exists(pathAsPath)) {
      Files.walk(pathAsPath)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(p => Files.deleteIfExists(p))
    }
  }
}
