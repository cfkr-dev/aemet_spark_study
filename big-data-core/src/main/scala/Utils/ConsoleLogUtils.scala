package Utils

import fansi._
import sttp.client4.Response
import sttp.model.{StatusCode, Uri}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object ConsoleLogUtils {

  object Method {
    object HTTPMethod extends Enumeration {
      type HTTPMethod = Value
      val GET: HTTPMethod = Value("GET")
      val POST: HTTPMethod = Value("POST")
    }

    import HTTPMethod._

    def printlnHTTPMethod(uri: Uri, httpMethod: HTTPMethod): Unit = println(
      Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"))).overlay(Bold.On) +
        " | " +
        Color.Yellow(httpMethod.toString).overlay(Bold.On) +
        " => " +
        uri.toString() +
        "\n"
    )

    def printlnGet(uri: Uri): Unit = printlnHTTPMethod(uri, HTTPMethod.GET)

    def printlnPost(uri: Uri): Unit = printlnHTTPMethod(uri, HTTPMethod.POST)
  }

  object Response {
    def printlnResponse[T](response: Response[T]): Unit =
      println(
        Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"))).overlay(Bold.On) +
          " | " +
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

  object Message {
    object NotificationType extends Enumeration {
      type NotificationType = Value
      val Error: NotificationType = Value("ERR")
      val Warning: NotificationType = Value("WARN")
      val Information: NotificationType = Value("INFO")
    }

    import NotificationType._

    private def getNotificationTypeString(notificationType: NotificationType): fansi.EscapeAttr = {
      notificationType match {
        case Error => Color.Red
        case Warning => Color.Yellow
        case Information => Color.Blue
      }
    }

    def printlnConsoleMessage(notificationType: NotificationType, message: String): Unit = {
      val color: fansi.EscapeAttr = getNotificationTypeString(notificationType)

      println(
        Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"))).overlay(Bold.On) +
          " | " +
          color(notificationType.toString + ": ").overlay(Bold.On) +
          color(message) +
          "\n"
      )
    }

    def printlnConsoleEnclosedMessage(
      notificationType: NotificationType,
      message: String,
      encloseString: String = "-",
      encloseHalfLength: Int = 30
    ): Unit = {
      val color: fansi.EscapeAttr = getNotificationTypeString(notificationType)

      println(
        color(encloseString * encloseHalfLength) +
          " " +
          Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"))).overlay(Bold.On) +
          " | " +
          color(notificationType.toString + ": ").overlay(Bold.On) +
          color(message) +
          " " +
          color(encloseString * encloseHalfLength) +
          "\n"
      )
    }
  }


}
