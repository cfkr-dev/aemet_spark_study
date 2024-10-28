import HTTPUtils._
import EnhancedConsoleLog._
import sttp.client4.UriContext

object AemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

  def getAllStationsMeteorologicalDataBetweenDates(startDate: String, endDate: String): Either[Exception, ujson.Value] = {
    sendRequest(
      buildUrl(
        Constants.aemetURL,
        List(
          "valores",
          "climatologicos",
          "diarios",
          "datos",
          "fechaini",
          startDate,
          "fechafin",
          endDate,
          "todasestaciones",
          ""
        ),
        List(
          ("api_key", APIKey match {
            case Right(apiKey) => apiKey
            case Left(exception) => return Left(exception)
          })
        )
      )
    ) match {
      case Right(response) =>
        val dataParsedToJSON = ujson.read(response.body)
        dataParsedToJSON("estado").num.toInt match {
          case 200 =>
            sendRequest(uri"${dataParsedToJSON("datos").str}") match {
              case Right(response) =>
                Right(ujson.read(response.body))
              case Left(exception) =>
                Left(exception)
            }
          case _ => Left(new Exception("Fail on getting data JSON"))
        }
      case Left(exception) => Left(exception)
    }
  }
}
