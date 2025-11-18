package Utils

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.io.Source

object FileUtils {

  // TODO ELIMINAR Y USAR LECTURA DE JSONUTILS
  def getContentFromPath(path: String): Either[Exception, String] = {
    val file = new File(path)

    if (!file.exists())
      return Left(new Exception("Error in finding file (%s)".format(file.toString)))

    val source = Source.fromFile(file)
    val content = source.getLines().mkString("\n")

    source.close()

    Right(content)
  }

  // TODO LLEVAR A JSONUTILS
  def saveContentToPath[T](
    path: String,
    fileName: String,
    content: T,
    appendContent: Boolean,
    writer: (File, T, Boolean) => Either[Exception, String]
  ): Either[Exception, String] = {
    val dir = new File(path)

    if (!dir.exists())
      if (!dir.mkdirs())
        return Left(new Exception("Error in directory creation (%s)".format(dir.toString)))

    writer(new File(dir, fileName), content, appendContent) match {
      case Right(filePath: String) => Right(filePath)
      case Left(error: Exception) => Left(error)
    }

  }

  def fileExists(path: String): Boolean = {
    new File(path).exists()
  }

  def copyFile(sourceFilePath: String, destFilePath: String): Either[Exception, String] = {
    try {
      val source: Path = Paths.get(sourceFilePath)
      val dest: Path = Paths.get(destFilePath)

      Files.createDirectories(dest.getParent)
      Right(Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING).toString)
    } catch {
      case exception: Exception => Left(exception)
    }
  }

}
