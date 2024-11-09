import HTTPUtils._
import sttp.client4.UriContext
import sttp.model.Uri
import ujson.Obj

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec

object AemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

  def saveAllStationsMeteorologicalDataBetweenDates(): Unit = {
    val dates: (ZonedDateTime, ZonedDateTime) = FileUtils.getContentFromPath(Constants.aemetAllStationsMeteorologicalMetadataBetweenDatesEndpointLastSavedDatesJSON) match {
      case Left(exception: Exception) =>
        println(exception)

        (
          DateUtils.getDate(Constants.startDate),
          DateUtils.addTime(DateUtils.getDate(Constants.startDate), 14, 23, 59, 59)
        )

      case Right(json) =>
        val endDate = DateUtils.getDate(ujson.read(json)("last_end_date").str)

        (
          DateUtils.addTime(endDate, 1),
          DateUtils.capDate(DateUtils.addTime(DateUtils.addTime(endDate, 1), 14, 23, 59, 59), DateUtils.getDate(Constants.endDate))
        )
    }

    saveAllStationsMeteorologicalDataBetweenDatesAction(dates._1, dates._2, !FileUtils.fileExists(Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates + "metadata.json"))
  }

  @scala.annotation.tailrec
  private def saveAllStationsMeteorologicalDataBetweenDatesAction(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean): Unit = {
    def getAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean): Boolean = {
      getAemetAPIResource(
        buildUrl(
          Constants.aemetAllStationsMeteorologicalDataBetweenDatesEndpoint,
          List(startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'")), endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'UTC'"))),
          List(
            ("api_key", APIKey match {
              case Right(apiKey) => apiKey
              case Left(exception) => throw exception
            })
          )
        ),
        getMetadata
      ) match {
        case Left(exception: Exception) =>
          println(exception)
          false
        case Right((json, metadata)) =>
          if (getMetadata) {
            metadata match {
              case Left(exception: Exception) =>
                println(exception)
                return false
              case Right(Some(metadata)) =>
                FileUtils.saveContentToPath(
                  Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates,
                  "metadata.json",
                  metadata,
                  appendContent = false,
                  JSONUtils.writeJSON
                )
            }
          }

          FileUtils.saveContentToPath(
            Constants.aemetJSONAllStationsMeteorologicalDataBetweenDates,
            s"${startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}_${endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}_data.json",
            JSONUtils.cast(json, Constants.aemetTypesToJSONCasting, JSONUtils.Aemet.metadataToNamesToTypes(FileUtils.getContentFromPath(Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates + "metadata.json") match {
              case Left(exception: Exception) =>
                println(exception)
                return false
              case Right(json) => ujson.read(json)
            })),
            appendContent = false,
            JSONUtils.writeJSON
          )

          /*FileUtils.saveContentToPath(
            Constants.aemetJSONAllStationsMeteorologicalDataBetweenDates,
            s"${startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}_${endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}_data.json",
            json,
            appendContent = false,
            JSONUtils.writeJSON
          )*/

          FileUtils.saveContentToPath(
            Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates,
            "last_saved_dates.json",
            Obj(
              "last_start_date" -> startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")),
              "last_end_date" -> endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            ),
            appendContent = false,
            JSONUtils.writeJSON
          )

          true
      }
    }

    @tailrec
    def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean): Unit = {
      if (!getAndSave(startDate, endDate, getMetadata)) {
        ChronoUtils.await(Constants.minimumMillisBetweenRequestMetadata)
        doWhileWithGetAndSave(startDate, endDate, getMetadata)
      }
    }

    if (!startDate.isBefore(DateUtils.getDate(Constants.endDate))) return

    ChronoUtils.executeAndAwaitIfTimeNotExceedMinimum(if (getMetadata) Constants.minimumMillisBetweenRequestMetadata else Constants.minimumMillisBetweenRequest) {
      doWhileWithGetAndSave(startDate, endDate, getMetadata)
    }

    saveAllStationsMeteorologicalDataBetweenDatesAction(
      DateUtils.addTime(endDate, 1),
      DateUtils.capDate(DateUtils.addTime(DateUtils.addTime(endDate, 1), 14, 23, 59, 59), DateUtils.getDate(Constants.endDate)),
      getMetadata = false
    )
  }

  private def getAemetAPIResource(uri: Uri, getMetadata: Boolean = false): Either[Exception, (ujson.Value, Either[Exception, Option[ujson.Value]])] = {
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
          case _ => Left(new Exception(s"Fail on getting JSON (${uri.toString()})"))
        }
      case Left(exception) => Left(exception)
    }
  }
}
