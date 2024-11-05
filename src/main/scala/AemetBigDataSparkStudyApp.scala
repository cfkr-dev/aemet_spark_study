import sttp.model.Uri

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import EnhancedConsoleLog._

object AemetBigDataSparkStudyApp extends App {
  AemetAPIClient.saveAllStationsMeteorologicalDataBetweenDates()
}
