import sttp.model.Uri

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import EnhancedConsoleLog._

object AemetBigDataSparkStudyApp extends App {
  val data = AemetAPIClient.getAllStationsMeteorologicalDataBetweenDates(
    ZonedDateTime.parse(Constants.startDate).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")),
    ZonedDateTime.parse(Constants.startDate).plusDays(14).plusHours(23).plusMinutes(59).plusSeconds(59).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")))

  val result = data match {
    case Right(data: ujson.Value) => data
    case Left(exception: Exception) => throw exception
  }

  FileUtils.saveContentToPath("./test_json", "test.json", result, true, JSONUtils.writeJSON)

  //println(data)
}
