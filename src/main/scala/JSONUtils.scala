import ujson.{Value, write}

import java.io.{BufferedWriter, File, FileWriter}

object JSONUtils {

  def writeJSON(file: File, json: ujson.Value): Either[Exception, String] = {
    try {
      val bufferedWriter = new BufferedWriter(new FileWriter(file))

      bufferedWriter.write(write(json, 2))
      bufferedWriter.flush()
      bufferedWriter.close()

      Right(file.getPath)
    } catch {
      case exception: Exception => Left(exception)
    }
  }
}