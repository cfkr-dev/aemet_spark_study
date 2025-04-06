package Utils

import fansi._
import sttp.client4.Response
import sttp.model.{StatusCode, Uri}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object ConsoleLogUtils {
  private val ctsLogsGlobal = Config.ConstantsV2.Logs.Global
  
  object Method {
    def printlnGet(uri: Uri): Unit = println(
      Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern(ctsLogsGlobal.EnhancedConsoleLog.format.dateHour))).overlay(Bold.On) +
      ctsLogsGlobal.EnhancedConsoleLog.Decorators.spaceVerticalDividerSpace +
      Color.Yellow(ctsLogsGlobal.EnhancedConsoleLog.Method.methodGet).overlay(Bold.On) +
      ctsLogsGlobal.EnhancedConsoleLog.Decorators.spaceBigArrowSpace +
      uri.toString() +
      "\n"
    )
  }

  object Response {
    def printlnResponse[T](response: Response[T]): Unit =
      println(
        Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern(ctsLogsGlobal.EnhancedConsoleLog.format.dateHour))).overlay(Bold.On) +
        ctsLogsGlobal.EnhancedConsoleLog.Decorators.spaceVerticalDividerSpace +
        response.request.uri.toString() +
        ctsLogsGlobal.EnhancedConsoleLog.Decorators.spaceBigArrowSpace +
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
      val Error: NotificationType = Value(ctsLogsGlobal.EnhancedConsoleLog.Message.notificationError)
      val Warning: NotificationType = Value(ctsLogsGlobal.EnhancedConsoleLog.Message.notificationWarning)
      val Information: NotificationType = Value(ctsLogsGlobal.EnhancedConsoleLog.Message.notificationInformation)
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
        Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern(ctsLogsGlobal.EnhancedConsoleLog.format.dateHour))).overlay(Bold.On) +
        ctsLogsGlobal.EnhancedConsoleLog.Decorators.spaceVerticalDividerSpace +
        color(notificationType.toString + ctsLogsGlobal.EnhancedConsoleLog.Decorators.colonSpace).overlay(Bold.On) +
        color(message) +
        "\n"
      )
    }

    def printlnConsoleEnclosedMessage(notificationType: NotificationType,
                                      message: String,
                                      encloseString: String = ctsLogsGlobal.EnhancedConsoleLog.Decorators.horizontalCenterLine,
                                      encloseHalfLength: Int = 30
                                     ): Unit = {
      val color: fansi.EscapeAttr = getNotificationTypeString(notificationType)

      println(
        color(encloseString * encloseHalfLength) +
        " " +
        Color.Magenta(ZonedDateTime.now().format(DateTimeFormatter.ofPattern(ctsLogsGlobal.EnhancedConsoleLog.format.dateHour))).overlay(Bold.On) +
        ctsLogsGlobal.EnhancedConsoleLog.Decorators.spaceVerticalDividerSpace +
        color(notificationType.toString + ctsLogsGlobal.EnhancedConsoleLog.Decorators.colonSpace).overlay(Bold.On) +
        color(message) +
        " " +
        color(encloseString * encloseHalfLength) +
        "\n"
      )
    }
  }






}
