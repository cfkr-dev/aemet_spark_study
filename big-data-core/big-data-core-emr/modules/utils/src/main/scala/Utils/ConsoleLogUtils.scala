package Utils

import fansi._

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
