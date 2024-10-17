object ConsoleUtils {

  object ConsoleColor extends Enumeration {
    type ConsoleColor = Value

    val Reset: Value = Value("\u001B[0m")
    val Red: Value = Value("\u001B[31m")
    val Green: Value = Value("\u001B[32m")
    val Yellow: Value = Value("\u001B[33m")
    val Blue: Value = Value("\u001B[34m")
    val Magenta: Value = Value("\u001B[35m")
    val Cyan: Value = Value("\u001B[36m")
    val White: Value = Value("\u001B[37m")
  }

  object HTTP {
    object Method extends Enumeration {
      val methodGet = colorString(ConsoleColor.Yellow, "GET")
    }

    object Response {
      val response200Ok = colorString(ConsoleColor.Green, "200 OK")
    }

    def methodMessage(uri: String, method: ConsoleUtils.HTTP.Method): String = s"$methodGet -> $uri"
    def responseMessage(uri: String): String = s"$uri -> $response200Ok"
  }

  def colorString(color: ConsoleColor.ConsoleColor, string: String): String = {
    color.toString + string + ConsoleColor.Reset.toString
  }

  def colorPrintln(color: ConsoleColor.ConsoleColor, string: String): Unit = {
    println(color.toString + string + ConsoleColor.Reset.toString)
  }
}
