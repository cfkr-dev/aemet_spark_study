import sttp.model.Uri

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object AemetBigDataSparkStudyApp extends App {
  val data = HTTPAemetAPIClient.getAllStationsMeteorologicalDataBetweenDates(
    ZonedDateTime.parse(Constants.startDate).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")),
    ZonedDateTime.parse(Constants.startDate).plusDays(14).plusHours(23).plusMinutes(59).plusSeconds(59).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")))

  println(data)
}