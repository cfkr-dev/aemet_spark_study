import HTTPUtils._
import sttp.client4.UriContext
import sttp.model.Uri
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object AemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

//  def getAllStationsMeteorologicalDataBetweenDates(startDate: String, endDate: String): Either[Exception, ujson.Value] = {
//    sendRequest(
//      buildUrl(
//        Constants.aemetAPIURL,
//        List(
//          "valores",
//          "climatologicos",
//          "diarios",
//          "datos",
//          "fechaini",
//          startDate,
//          "fechafin",
//          endDate,
//          "todasestaciones",
//          ""
//        ),
//        List(
//          ("api_key", APIKey match {
//            case Right(apiKey) => apiKey
//            case Left(exception) => return Left(exception)
//          })
//        )
//      )
//    ) match {
//      case Right(response) =>
//        val dataParsedToJSON = ujson.read(response.body)
//        dataParsedToJSON("estado").num.toInt match {
//          case 200 =>
//            sendRequest(uri"${dataParsedToJSON("datos").str}") match {
//              case Right(response) =>
//                Right(ujson.read(response.body))
//              case Left(exception) =>
//                Left(exception)
//            }
//          case _ => Left(new Exception("Fail on getting data JSON"))
//        }
//      case Left(exception) => Left(exception)
//    }
//  }
  def getAllStationsMeteorologicalDataBetweenDates(): Unit = {
    val startDate: String = ZonedDateTime
      .parse(Constants.startDate)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'"))

    val endDate: String = ZonedDateTime
      .parse(Constants.startDate)
      .plusDays(14)
      .plusHours(23)
      .plusMinutes(59)
      .plusSeconds(59)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'"))

    getAllStationsMeteorologicalDataBetweenDatesAction(startDate, endDate, getMetadata = true)
  }

  @scala.annotation.tailrec
  private def getAllStationsMeteorologicalDataBetweenDatesAction(startDate: String, endDate: String, getMetadata: Boolean): Unit = {
    val startTime = System.nanoTime()

    getAemetAPIResource(
      buildUrl(
        Constants.aemetAllStationsMeteorologicalDataBetweenDatesEndpoint,
        List(startDate, endDate),
        List(
          ("api_key", APIKey match {
            case Right(apiKey) => apiKey
            case Left(exception) => throw exception
          })
        )
      ),
      getMetadata
    ) match {
      case Left(exception: Exception) => println(exception)
      case Right((json, metadata)) =>
        if (getMetadata) {
          metadata match {
            case Left(exception: Exception) => println(exception)
            case Right(Some(metadata)) => FileUtils.saveContentToPath(
              Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates,
              "metadata.json",
              metadata,
              appendContent = false,
              JSONUtils.writeJSON
            )
          }
        }

//        FileUtils.saveContentToPath(
//          Constants.aemetJSONAllStationsMeteorologicalDataBetweenDates,
//          s"${startDate}_${endDate}_data.json",
//          JsonUtils.castJSON(json),
//          appendContent = false,
//          JSONUtils.writeJSON
//        )

        FileUtils.saveContentToPath(
          Constants.aemetJSONAllStationsMeteorologicalDataBetweenDates,
          s"${startDate}_${endDate}_data.json",
          json,
          appendContent = false,
          JSONUtils.writeJSON
        )
    }

    val newStartDate: ZonedDateTime = ZonedDateTime
      .parse(endDate)
      .plusSeconds(1)

    if (newStartDate.isBefore(ZonedDateTime.parse(Constants.endDate))) {
      val temporalEndDate: ZonedDateTime = newStartDate
        .plusDays(14)
        .plusHours(23)
        .plusMinutes(59)
        .plusSeconds(59)

      val newEndDate = if (temporalEndDate.isAfter(ZonedDateTime.parse(Constants.endDate)))
        ZonedDateTime.parse(Constants.endDate)
      else temporalEndDate

      val endTime = System.nanoTime()
      val lapse = (endTime - startTime) / 1e6

      if (lapse < Constants.minimumMillisBetweenRequest)
        Thread.sleep((Constants.minimumMillisBetweenRequest - lapse).toLong)

      getAllStationsMeteorologicalDataBetweenDatesAction(
        newStartDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")),
        newEndDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")),
        getMetadata = false
      )
    }
  }

  def getAemetAPIResource(uri: Uri, getMetadata: Boolean = false): Either[Exception, (ujson.Value, Either[Exception, Option[ujson.Value]])] = {
    sendRequest(uri) match {
      case Right(response) =>
        val dataParsedToJSON = ujson.read(response.body)
        dataParsedToJSON("estado").num.toInt match {
          case 200 =>
            val metadata = if (getMetadata) sendRequest(uri"${dataParsedToJSON("metadatos").str}") match {
              case Right(response) =>
                Right(Some(ujson.read(response.body)))
              case Left(exception) =>
                Left(exception)
            } else Right(None)

            sendRequest(uri"${dataParsedToJSON("datos").str}") match {
              case Right(response) =>
                Right((ujson.read(response.body), metadata))
              case Left(exception) =>
                Left(exception)
            }
          case _ => Left(new Exception("Fail on getting data JSON"))
        }
      case Left(exception) => Left(exception)
    }
  }
}
