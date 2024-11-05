import HTTPUtils._
import sttp.client4.UriContext
import sttp.model.Uri
import ujson.Obj

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.util.control.Breaks.break

object AemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

  def saveAllStationsMeteorologicalDataBetweenDates(): Unit = {
    FileUtils.getContentFromPath(Constants.aemetAllStationsMeteorologicalMetadataBetweenDatesEndpointLastSavedDatesJSON) match {
      case Left(exception: Exception) =>
        println(exception)

        val startDate: ZonedDateTime = ZonedDateTime
          .parse(Constants.startDate)

        val endDate: ZonedDateTime = ZonedDateTime
          .parse(Constants.startDate)
          .plusDays(14)
          .plusHours(23)
          .plusMinutes(59)
          .plusSeconds(59)

        saveAllStationsMeteorologicalDataBetweenDatesAction(startDate, endDate, !FileUtils.fileExists(Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates + "metadata.json"), List())

      case Right(json) =>
        val lastSavedDatesJSON = ujson.read(json)

        val startDate: ZonedDateTime = ZonedDateTime
          .parse(lastSavedDatesJSON("last_end_date").str)
          .plusSeconds(1)

        val temporalEndDate: ZonedDateTime = startDate
          .plusDays(14)
          .plusHours(23)
          .plusMinutes(59)
          .plusSeconds(59)

        val endDate = if (temporalEndDate.isAfter(ZonedDateTime.parse(Constants.endDate)))
          ZonedDateTime.parse(Constants.endDate)
        else temporalEndDate

        saveAllStationsMeteorologicalDataBetweenDatesAction(startDate, endDate, !FileUtils.fileExists(Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates + "metadata.json"), List())

    }
  }

  @scala.annotation.tailrec
  private def saveAllStationsMeteorologicalDataBetweenDatesAction(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean, fails: List[(ZonedDateTime, ZonedDateTime, Boolean)]): List[(ZonedDateTime, ZonedDateTime, Boolean)] = {
    if (!startDate.isBefore(ZonedDateTime.parse(Constants.endDate))) return fails

    val startTime = System.nanoTime()

    val getAndSave: (ZonedDateTime, ZonedDateTime, Boolean) => Boolean = (startDate, endDate, getMetadata) => {
      val result = getAemetAPIResource(
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
          val resultMetadata = {
            if (!getMetadata) true
            else metadata match {
              case Left(exception: Exception) =>
                println(exception)
                false
              case Right(Some(metadata)) =>
                FileUtils.saveContentToPath(
                  Constants.aemetJSONAllStationsMeteorologicalMetadataBetweenDates,
                  "metadata.json",
                  metadata,
                  appendContent = false,
                  JSONUtils.writeJSON
                )
                true
            }
          }

          FileUtils.saveContentToPath(
            Constants.aemetJSONAllStationsMeteorologicalDataBetweenDates,
            s"${startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}_${endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}_data.json",
            json,
            appendContent = false,
            JSONUtils.writeJSON
          )

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

          resultMetadata
      }

      result
    }

    @tailrec
    def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean): Unit = {
      if (!getAndSave(startDate, endDate, getMetadata)) {
        Thread.sleep(Constants.minimumMillisBetweenRequestMetadata.toLong)
        doWhileWithGetAndSave(startDate, endDate, getMetadata)
      }
    }

    doWhileWithGetAndSave(startDate, endDate, getMetadata)

    val newStartDate: ZonedDateTime = endDate
      .plusSeconds(1)

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
    val minimumMillis = if (getMetadata) Constants.minimumMillisBetweenRequestMetadata else Constants.minimumMillisBetweenRequest

    if (lapse < minimumMillis)
      Thread.sleep((minimumMillis - lapse).toLong)

    saveAllStationsMeteorologicalDataBetweenDatesAction(
      newStartDate,
      newEndDate,
      getMetadata = false,
      fails
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
