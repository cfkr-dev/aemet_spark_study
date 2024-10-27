import sttp.model.{StatusCode, Uri}
import fansi.{Color, _}
import sttp.client4.Response

object EnhancedConsoleLog {
  object Method {
    def printlnGet(uri: Uri): Unit = println(Color.Yellow("GET").overlay(Bold.On) ++ " => " ++ Color.LightMagenta(uri.toString()).overlay(Underlined.On))
  }

  object Response {
    def printlnResponse[T](response: Response[T]): Unit = {
      val uriEnhancedString = Color.LightMagenta(response.request.uri.toString()).overlay(Underlined.On)
      val codeEnhancedString = getCodeEnhancedString(response.code)
      val statusEnhancedString = getStatusEnhancedString(response.code, response.statusText)

      println(uriEnhancedString ++ " => " ++ codeEnhancedString ++ " " ++ statusEnhancedString)
    }

    private def getStatusEnhancedString(code: StatusCode, statusText: String): Str = {
      if (code.isSuccess)
        Color.Green(statusText).overlay(Bold.On)
      else if (code.isClientError || code.isServerError)
        Color.Red(statusText).overlay(Bold.On)
      else
        Color.Blue(statusText).overlay(Bold.On)
    }

    private def getCodeEnhancedString(code: StatusCode): Str = {
      if (code.isSuccess)
        Color.Green(code.toString()).overlay(Bold.On)
      else if (code.isClientError || code.isServerError)
        Color.Red(code.toString()).overlay(Bold.On)
      else
        Color.Blue(code.toString()).overlay(Bold.On)
    }
  }






}
