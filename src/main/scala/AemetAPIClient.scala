import HTTPUtils._
import EnhancedConsoleLog._
import sttp.client4.UriContext

object AemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

  def getAllStationsMeteorologicalDataBetweenDates(startDate: String, endDate: String): Either[Exception, Option[ujson.Value]] = {
    val uri = buildUrl(
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
    EnhancedConsoleLog.Method.printlnGet(uri)

    makeRequest(uri).body match {
      case Right(data) =>
        val dataParsedToJSON = ujson.read(data)

        dataParsedToJSON("estado").num.toInt match {
          case 200 =>
            EnhancedConsoleLog.Response.println200OK(uri)

            val subUri = uri"${dataParsedToJSON("datos").str}"
            EnhancedConsoleLog.Method.printlnGet(subUri)

            makeRequest(subUri).body match {
              case Right(data) =>
                EnhancedConsoleLog.Response.println200OK(subUri)
                Right(Some(ujson.read(data)))

              case Left(_) =>
                EnhancedConsoleLog.Response.println500ServerError()
                Right(None)
            }
          case _ => None
        }
      case Left(_) => None
    }
  }
}
