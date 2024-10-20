import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, basicRequest, quickRequest}
import sttp.model.Uri

object HTTPUtils {
  def buildUrl(baseUri: Uri, uriPaths: List[String], uriParams: List[(String, String)]): Uri = {
    baseUri
      .addPath(uriPaths)
      .addParams(uriParams: _*)
  }

  def makeRequest(uri: Uri): Response[String] = {
    val backend = HttpURLConnectionBackend()

    quickRequest
      .get(uri)
      .send()
  }



}
