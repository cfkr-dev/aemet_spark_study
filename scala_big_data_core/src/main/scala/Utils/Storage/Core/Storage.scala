package Utils.Storage.Core

import java.nio.file.Path

case class Storage(storagePrefix: Option[String], s3Endpoint: Option[String] = None) {

  private val checkedPrefix = storagePrefix.getOrElse(throw new StoragePrefixNotFound())

  private def parseS3Path(path: String): (String, String) = {
    val parts = path.stripPrefix("s3://").split("/", 2)

    if (parts.length != 2 || parts(0).trim.isEmpty)
      throw new IllegalArgumentException(s"Invalid S3 path: $path. Expected s3://bucket/key")

    (parts(0), parts(1))
  }

  def selectS3orLocal(path: String): (String, String, Boolean) = {
    if (path.startsWith("s3://")) {
      val (bucket, key) = parseS3Path(path)
      (bucket, key, true)
    } else {
      ("", path, false)
    }
  }

  def read(path: String): Path = {
    val (bucket, key, isS3) = selectS3orLocal(checkedPrefix + path)
    try {
      if (isS3) S3StorageBackend.read(bucket, key, s3Endpoint) else LocalStorageBackend.read(key)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to read from path: $path", exception)
    }
  }

  def write(path: String, localFile: Path): Unit = {
    val (bucket, key, isS3) = selectS3orLocal(checkedPrefix + path)
    try {
      if (isS3) S3StorageBackend.write(bucket, key, localFile, s3Endpoint) else LocalStorageBackend.write(key, localFile)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to write to path: $path", exception)
    }
  }

  def copy(src: String, dst: String): Unit = {
    try {
      val tmp = read(src)
      write(dst, tmp)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to copy from $src to $dst", exception)
    }
  }
}
