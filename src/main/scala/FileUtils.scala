import java.io.File
import scala.io.Source

object FileUtils {
  def getContentFromPath(path: String): Either[Exception, String] = {
    val file = new File(path)

    if (!file.exists())
      return Left(new Exception(s"Error in finding file (${file.toString})"))

    val source = Source.fromFile(file)
    val content = source.getLines().mkString("\n")

    source.close()

    Right(content)
  }

  def saveContentToPath[T](path: String, fileName: String, content: T, appendContent: Boolean, writer: (File, T, Boolean) => Either[Exception, String]): Either[Exception, String] = {
    val dir = new File(path)

    if (!dir.exists())
      if (!dir.mkdirs())
        return Left(new Exception(s"Error in directory creation (${dir.toString})"))

    writer(new File(dir, fileName), content, appendContent) match {
      case Right(filePath: String) => Right(filePath)
      case Left(error: Exception) => Left(error)
    }

  }

  def fileExists(path: String): Boolean = {
    new File(path).exists()
  }

}
