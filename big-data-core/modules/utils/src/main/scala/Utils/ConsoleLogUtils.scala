package Utils

import fansi._
import sttp.client4.Response
import sttp.model.{StatusCode, Uri}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * Console logging utilities used across the project.
 *
 * Provides small helpers to print colored, timestamped messages for HTTP
 * methods, HTTP responses and generic notifications. Uses backticks for
 * technical terms where appropriate (`GET`, `POST`, `URI`, `StatusCode`).
 */
object ConsoleLogUtils {

  /**
   * Helpers related to HTTP method logging.
   */
  object Method {
    /**
     * Enumeration of supported HTTP method labels used for logging.
     */
    object HTTPMethod extends Enumeration {
      /**
       * Type alias for values of the `HTTPMethod` enumeration.
       */
      type HTTPMethod = Value
      val GET: HTTPMethod = Value("GET")
      val POST: HTTPMethod = Value("POST")
    }

    import HTTPMethod._

    /**
     * Print a colored, timestamped line describing the HTTP method and `Uri`.
     *
     * @param uri the request `Uri` to log
     * @param httpMethod the HTTP method label to print (`GET` or `POST`)
     */
    def printlnHTTPMethod(uri: Uri, httpMethod: HTTPMethod): Unit = println(
      Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"))).overlay(Bold.On) +
        " | " +
        Color.Yellow(httpMethod.toString).overlay(Bold.On) +
        " => " +
        uri.toString() +
        "\n"
    )

    /**
     * Print a `GET` method log line for the given `Uri`.
     *
     * @param uri the request `Uri`
     */
    def printlnGet(uri: Uri): Unit = printlnHTTPMethod(uri, HTTPMethod.GET)

    /**
     * Print a `POST` method log line for the given `Uri`.
     *
     * @param uri the request `Uri`
     */
    def printlnPost(uri: Uri): Unit = printlnHTTPMethod(uri, HTTPMethod.POST)
  }

  /**
   * Helpers to format and print HTTP `Response`s to the console.
   */
  object Response {
    /**
     * Print a timestamped log line summarizing an HTTP `Response`.
     *
     * @param response the HTTP `Response` to log
     */
    def printlnResponse[T](response: Response[T]): Unit =
      println(
        Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"))).overlay(Bold.On) +
          " | " +
          response.request.uri.toString() +
          " => " +
          getCodeAndStatusEnhancedString(response.code, response.statusText) +
          "\n"
      )

    /**
     * Return a colored `Str` representing the numeric `StatusCode` and its text.
     * Uses color coding for success, client/server error and other codes.
     *
     * @param code the HTTP `StatusCode`
     * @param statusText the human-readable status text
     * @return colored `Str` with `code` and `statusText`
     */
    private def getCodeAndStatusEnhancedString(code: StatusCode, statusText: String): Str = {
      if (code.isSuccess)
        Color.Green(code.toString() + " " + statusText).overlay(Bold.On)
      else if (code.isClientError || code.isServerError)
        Color.Red(code.toString() + " " + statusText).overlay(Bold.On)
      else
        Color.Blue(code.toString() + " " + statusText).overlay(Bold.On)
    }
  }

  /**
   * Generic console message helpers and `NotificationType` definitions.
   */
  object Message {
    /**
     * Notification types used for console messages (`ERR`, `WARN`, `INFO`).
     */
    object NotificationType extends Enumeration {
      /**
       * Type alias for values of the `NotificationType` enumeration.
       */
      type NotificationType = Value
      val Error: NotificationType = Value("ERR")
      val Warning: NotificationType = Value("WARN")
      val Information: NotificationType = Value("INFO")
    }

    import NotificationType._

    /**
     * Map a `NotificationType` to a `fansi.EscapeAttr` used for coloring messages.
     *
     * @param notificationType the `NotificationType` to map
     * @return a `fansi.EscapeAttr` color for the given `notificationType`
     */
    private def getNotificationTypeString(notificationType: NotificationType): fansi.EscapeAttr = {
      notificationType match {
        case Error => Color.Red
        case Warning => Color.Yellow
        case Information => Color.Blue
      }
    }

    /**
     * Print a timestamped and colored console message with a `NotificationType` prefix.
     *
     * @param notificationType the message `NotificationType` (`Error`, `Warning`, `Information`)
     * @param message the textual message to print
     */
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

    /**
     * Print a visually enclosed, timestamped, colored message useful for stage separators.
     *
     * @param notificationType the message `NotificationType`
     * @param message the textual message to print
     * @param encloseString the character used to build the enclosure line (default `-`)
     * @param encloseHalfLength number of enclosure characters on each side (default `30`)
     */
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
