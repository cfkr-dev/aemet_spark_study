import sttp.model.{StatusCode, Uri}
import fansi._
import sttp.client4.Response

object EnhancedConsoleLog {
  object Method {
    def printlnGet(uri: Uri): Unit = println(
      Color.Yellow("GET").overlay(Bold.On) +
      " => " +
      uri.toString() +
      "\n"
    )
  }

  object Response {
    def printlnResponse[T](response: Response[T]): Unit =
      println(
        response.request.uri.toString() +
        " => " +
        getCodeAndStatusEnhancedString(response.code, response.statusText) +
        "\n"
      )

    private def getCodeAndStatusEnhancedString(code: StatusCode, statusText: String): Str = {
      if (code.isSuccess)
        Color.Green(code.toString() + " " + statusText).overlay(Bold.On)
      else if (code.isClientError || code.isServerError)
        Color.Red(code.toString() + " " + statusText).overlay(Bold.On)
      else
        Color.Blue(code.toString() + " " + statusText).overlay(Bold.On)
    }
  }






}
