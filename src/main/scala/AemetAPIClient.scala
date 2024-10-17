import HTTPUtils._
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

    println(s"GET -> $uri")

    makeRequest(uri).body match {
      case Right(data) =>
        val dataParsedToJSON = ujson.read(data)

        dataParsedToJSON("estado").num.toInt match {
          case 200 =>

            println(s"200 OK -> $uri")

            val subUri = uri"${dataParsedToJSON("datos").str}"
            println(s"GET -> $subUri")

            makeRequest(subUri).body match {
              case Right(data) =>
                println(s"200 OK -> $uri")

                Right(Some(ujson.read(data)))
              case Left(_) => Right(None)
            }
          case _ => None
        }
      case Left(_) => None
    }
  }
}
