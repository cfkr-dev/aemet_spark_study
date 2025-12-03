package Utils.Storage.Core

import java.nio.file.{Files, Path, Paths}

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

  def read(path: String, prefix: String = checkedPrefix): Path = {
    val (bucket, key, isS3) = selectS3orLocal(prefix + path)
    try {
      if (isS3) S3StorageBackend.read(bucket, key, s3Endpoint) else LocalStorageBackend.read(key)
    } catch {
      case exception: Exception =>
        throw new StorageException(s"Failed to read from path: $path", exception)
    }
  }

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

  def write(path: String, localFile: Path, prefix: String = checkedPrefix): Unit = {
    val (bucket, key, isS3) = selectS3orLocal(prefix + path)
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

  def deleteLocalDirectoryRecursive(path: String): Unit = {
    val pathAsPath = Paths.get(path)
    if (Files.exists(pathAsPath)) {
      Files.walk(pathAsPath)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(p => Files.deleteIfExists(p))
    }
  }
}
