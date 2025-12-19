package Utils.Storage.Core

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

/**
 * Local storage backend using the filesystem. Methods return `Path` to a
 * temporary copy for reading and perform atomic copies for writing.
 */
object LocalStorageBackend {

  private val tempDirBase: Path = Paths.get(System.getProperty("java.io.tmpdir"))

  /**
   * Read a local file into a temporary path and return the temporary `Path`.
   *
   * @param path local filesystem path to read
   * @return temporary `Path` where the file content can be accessed
   * @throws LocalFileNotFoundException if the source file does not exist
   * @throws StorageException on IO errors while copying
   */
  def read(path: String): Path = {
    val src = Paths.get(path)
    if (!Files.exists(src)) throw new LocalFileNotFoundException(path)

    val dst = tempDirBase.resolve(src.getFileName.toString)
    try {
      Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case exception: Exception => throw new StorageException(s"Failed to read local file: $path", exception)
    }
    dst
  }

  /**
   * Recursively copy a subset of a directory to a temporary directory and
   * return the temporary root `Path`.
   *
   * @param dirPath root directory path to copy
   * @param includeDirs sequence of directory paths (relative to `dirPath`) to include
   * @return temporary `Path` that mirrors the requested subset
   * @throws LocalFileNotFoundException if `dirPath` is missing or not a directory
   */
  def readDirectoryRecursive(dirPath: String, includeDirs: Seq[String]): Path = {
    val srcDir = Paths.get(dirPath)
    if (!Files.exists(srcDir) || !Files.isDirectory(srcDir)) {
      throw new LocalFileNotFoundException(dirPath)
    }

    val tempDirBasePath = Files.createTempDirectory(tempDirBase, null)
    val tempDir = tempDirBasePath.resolve(srcDir.getFileName)
    Files.createDirectories(tempDir)

    val rootName = srcDir.getFileName.toString

    val includePaths: Set[Path] = includeDirs.map { p =>
      val normalized = p.replace('\\', '/').stripPrefix("/").stripSuffix("/")
      val parts = normalized.split("/").toList

      parts match {
        case head :: tail if head == rootName =>
          Paths.get(tail.mkString("/"))
        case _ =>
          throw new IllegalArgumentException(
            s"includeDir must start with root directory '$rootName': $p"
          )
      }
    }.toSet

    Files.walk(srcDir).forEach { path =>
      val relative = srcDir.relativize(path)
      val target = tempDir.resolve(relative)

      val includeThis =
        includePaths.isEmpty || includePaths.exists(dir => relative.startsWith(dir))

      if (Files.isDirectory(path)) {
        if (includeThis) Files.createDirectories(target)
      } else {
        if (includeThis) {
          Option(target.getParent).foreach(Files.createDirectories(_))
          Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING)
        }
      }
    }

    tempDir
  }

  /**
   * Write a file from `localPath` to the destination `path` on the local filesystem.
   *
   * @param path destination path (absolute or relative)
   * @param localPath temporary `Path` to copy from
   * @throws StorageException on IO errors while writing
   */
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