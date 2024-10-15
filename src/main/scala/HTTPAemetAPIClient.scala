import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, UriContext, basicRequest}
import sttp.model.Uri

object HTTPAemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

  private def buildUrl(baseUri: Uri, uriPaths: List[String], uriParams: List[(String, String)]): Uri = {
    baseUri
      .addPath(uriPaths)
      .addParams(uriParams: _*)
  }

  private def makeRequest(url: Uri): Response[Either[String, String]] = {
    val backend = HttpURLConnectionBackend()

    basicRequest
      .get(url)
      .send(backend)
  }

  def getAllStationsMeteorologicalDataBetweenDates(startDate: String, endDate: String): Option[ujson.Value] = {
    makeRequest(buildUrl(
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
          case Some(apiKey) => apiKey
          case None => return None
        })
      )
    )).body match {
      case Right(data) =>
        val dataParsedToJSON = ujson.read(data)

        dataParsedToJSON("estado").num.toInt match {
          case 200 =>
            makeRequest(uri"${dataParsedToJSON("datos").str}").body match {
              case Right(data) => Some(ujson.read(data))
              case Left(_) => None
            }
          case _ => None
        }
      case Left(_) => None
    }
  }
}
