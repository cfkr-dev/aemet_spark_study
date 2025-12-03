package Utils.Storage.Core

import java.nio.file.{Files, Path, Paths, StandardCopyOption}

object LocalStorageBackend {

  private val tempDirBase: Path = Paths.get(System.getProperty("java.io.tmpdir"))

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