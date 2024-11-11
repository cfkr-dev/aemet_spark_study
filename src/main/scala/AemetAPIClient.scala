import HTTPUtils._
import sttp.client4.UriContext
import sttp.model.Uri
import ujson.Obj

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec

object AemetAPIClient {
  private val constantsAemetPathsGlobal = Constants.AemetPaths.Global
  private val constantsParamsGlobal = Constants.Params.Global
  private val constantsAemetAPIGlobal = Constants.AemetAPI.Global
  
  private val APIKey = FileUtils.getContentFromPath(constantsAemetPathsGlobal.apiKeyFile)

  object AllStationsMeteorologicalDataBetweenDates {
    private val constantsAemetPaths = Constants.AemetPaths.AllStationsMeteorologicalDataBetweenDates
    private val constantsParams = Constants.Params.AllStationsMeteorologicalDataBetweenDates
    private val constantsAemetAPI = Constants.AemetAPI.AllStationsMeteorologicalDataBetweenDates
    
    def saveAllStationsMeteorologicalDataBetweenDates(): Unit = {
      val dates: (ZonedDateTime, ZonedDateTime) = FileUtils.getContentFromPath(
        constantsAemetPaths.jsonLastSavedDatesFile
      ) match {
        case Left(exception: Exception) =>
          println(exception)

          (
            DateUtils.getDate(constantsParams.startDate),
            DateUtils.addTime(
              DateUtils.getDate(constantsParams.startDate), 14, 23, 59, 59
            )
          )

        case Right(json) =>
          val endDate = DateUtils.getDate(
            ujson.read(json)(
              constantsAemetAPI.lastSavedDatesLastEndDateParamName
            ).str
          )

          (
            DateUtils.addTime(endDate, 1),
            DateUtils.capDate(DateUtils.addTime(DateUtils.addTime(endDate, 1), 14, 23, 59, 59), DateUtils.getDate(
              constantsParams.endDate)
            )
          )
      }

      saveAllStationsMeteorologicalDataBetweenDatesAction(
        dates._1,
        dates._2,
        !FileUtils.fileExists(constantsAemetPaths.jsonMetadataFile)
      )
    }

    private def getAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean): Boolean = {
      getAemetAPIResource(
        buildUrl(
          constantsAemetAPI.dataEndpoint,
          List(
            startDate.format(
              DateTimeFormatter.ofPattern(constantsParams.dateHourFormat)
            ),
            endDate.format(
              DateTimeFormatter.ofPattern(constantsParams.dateHourFormat)
            )
          ),
          List(
            (constantsAemetAPIGlobal.apiKeyQueryParamName, APIKey match {
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
                  constantsAemetPaths.jsonMetadata,
                  constantsAemetPaths.jsonMetadataFilename,
                  metadata,
                  appendContent = false,
                  JSONUtils.writeJSON
                )
            }
          }

          FileUtils.saveContentToPath(
            constantsAemetPaths.jsonData,
            constantsAemetPaths.jsonDataFilename.format(
              startDate.format(
                DateTimeFormatter.ofPattern(constantsParams.dateFormat)
              ),
              endDate.format(
                DateTimeFormatter.ofPattern(constantsParams.dateFormat)
              )
            ),
            json,
            appendContent = false,
            JSONUtils.writeJSON
          )

          FileUtils.saveContentToPath(
            constantsAemetPaths.jsonMetadata,
            constantsAemetPaths.jsonLastSavedDatesFilename,
            Obj(
              constantsAemetAPI.lastSavedDatesLastEndDateParamName ->
                endDate.format(
                  DateTimeFormatter.ofPattern(constantsParams.dateHourFormat)
                )
            ),
            appendContent = false,
            JSONUtils.writeJSON
          )

          true
      }
    }

    @tailrec
    private def doWhileWithGetAndSave(startDate: ZonedDateTime, endDate: ZonedDateTime, getMetadata: Boolean): Unit = {
      if (!getAndSave(startDate, endDate, getMetadata)) {
        ChronoUtils.await(constantsParamsGlobal.minimumMillisBetweenRequestMetadata)
        doWhileWithGetAndSave(startDate, endDate, getMetadata)
      }
    }

    @tailrec
    private def saveAllStationsMeteorologicalDataBetweenDatesAction(startDate: ZonedDateTime,
                                                                    endDate: ZonedDateTime,
                                                                    getMetadata: Boolean
                                                                   ): Unit = {
      if (!startDate.isBefore(DateUtils.getDate(constantsParams.endDate))) return

      ChronoUtils.executeAndAwaitIfTimeNotExceedMinimum(
        if (getMetadata) constantsParamsGlobal.minimumMillisBetweenRequestMetadata
        else constantsParamsGlobal.minimumMillisBetweenRequest
      ) {
        doWhileWithGetAndSave(startDate, endDate, getMetadata)
      }

      saveAllStationsMeteorologicalDataBetweenDatesAction(
        DateUtils.addTime(endDate, 1),
        DateUtils.capDate(
          DateUtils.addTime(DateUtils.addTime(endDate, 1), 14, 23, 59, 59),
          DateUtils.getDate(constantsParams.endDate)
        ),
        getMetadata = false
      )
    }
  }

  object AllStationsData {
    private val constantsAemetPaths = Constants.AemetPaths.AllStationsData
    private val constantsParams = Constants.Params.AllStationsData
    private val constantsAemetAPI = Constants.AemetAPI.AllStationsData

    def saveAllStations(): Unit = {
      getAemetAPIResource(
        buildUrl(
          constantsAemetAPI.dataEndpoint,
          List(),
          List(
            (constantsAemetAPIGlobal.apiKeyQueryParamName, APIKey match {
              case Right(apiKey) => apiKey
              case Left(exception) => throw exception
            })
          )
        ),
        getMetadata = true
      ) match {
        case Left(exception: Exception) => println(exception)
        case Right((json, metadata)) => metadata match {
          case Left(exception: Exception) =>
            println(exception)
            return
          case Right(Some(metadata)) => FileUtils.saveContentToPath(
            constantsAemetPaths.jsonMetadata,
            constantsAemetPaths.jsonMetadataFilename,
            metadata,
            appendContent = false,
            JSONUtils.writeJSON
          )
        }
          
        FileUtils.saveContentToPath(
          constantsAemetPaths.jsonData,
          constantsAemetPaths.jsonDataFilename,
          json,
          appendContent = false,
          JSONUtils.writeJSON
        )
      }
    }
  }

  private def getAemetAPIResource(uri: Uri,
                                  getMetadata: Boolean = false
                                 ): Either[Exception, (ujson.Value, Either[Exception, Option[ujson.Value]])] = {
    sendRequest(uri) match {
      case Right(response) =>
        val dataParsedToJSON = ujson.read(response.body)
        dataParsedToJSON(constantsAemetAPIGlobal.responseStateNumberKey).num.toInt match {
          case 200 =>
            val metadata = if (getMetadata) sendRequest(uri"${dataParsedToJSON(constantsAemetAPIGlobal.responseMetadataKey).str}") match {
              case Right(response) =>
                Right(Some(ujson.read(response.body)))
              case Left(exception) =>
                Left(exception)
            } else Right(None)

            sendRequest(uri"${dataParsedToJSON(constantsAemetAPIGlobal.responseDataKey).str}") match {
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
