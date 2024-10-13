import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, UriContext, basicRequest}

object HTTPAemetAPIClient {
  private val APIKey = FileUtils.getContentFromPath(Constants.aemetAPIKeyPath)

  def makeRequest: Response[Either[String, String]] = {
    val backend = HttpURLConnectionBackend()
    val request = basicRequest.get(uri"https://catfact.ninja/fact")

    request.send(backend)
  }
}
