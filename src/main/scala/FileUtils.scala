import java.io.File
import scala.io.Source

object FileUtils {

  def getContentFromPath(path: String): Option[String] = {
    val file = new File(path)

    if (file.exists()) {
      try {
        val source = Source.fromFile(file)

        try
          Some(source.getLines().mkString("\n"))
        finally
          source.close()
      } catch {
        case _: Exception => None
      }
    } else None
  }

}
