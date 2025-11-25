package Utils.Storage.Core

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

object LocalStorageBackend {

  private val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))

  def read(path: String): Path = {
    val src = Paths.get(path)
    if (!Files.exists(src)) throw new LocalFileNotFoundException(path)

    val dst = tempDir.resolve(src.getFileName.toString)
    try {
      Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case exception: Exception => throw new StorageException(s"Failed to read local file: $path", exception)
    }
    dst
  }

  def write(path: String, localPath: Path): Unit = {
    val dst = Paths.get(path)
    try {
      Option(dst.getParent).foreach(p => Files.createDirectories(p))
      Files.copy(localPath, dst, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case exception: Exception => throw new StorageException(s"Failed to write local file: $path", exception)
    }
  }
}