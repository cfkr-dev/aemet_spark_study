import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, basicRequest}
import sttp.model.Uri
import ConsoleUtils._

object HTTPUtils {
  val getMessage = colorString(ConsoleColor.Green, "GET")
  val arrowMessage =

  def buildUrl(baseUri: Uri, uriPaths: List[String], uriParams: List[(String, String)]): Uri = {
    baseUri
      .addPath(uriPaths)
      .addParams(uriParams: _*)
  }

  def makeRequest(url: Uri): Response[Either[String, String]] = {
    val backend = HttpURLConnectionBackend()

    basicRequest
      .get(url)
      .send(backend)
  }

  def enhancedMessageGet(uri: Uri): Unit = {

  }

}
