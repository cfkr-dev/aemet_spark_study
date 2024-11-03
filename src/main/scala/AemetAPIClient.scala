import HTTPUtils._
import EnhancedConsoleLog._
import sttp.client4.UriContext
import sttp.model.Uri

import java.io.File

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

  def getAllStationsMeteorologicalDataBetweenDates(): Either[Exception, ujson.Value] = {

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
