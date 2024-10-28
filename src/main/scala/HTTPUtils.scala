import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, basicRequest, quickRequest}
import sttp.model.Uri

object HTTPUtils {
  def buildUrl(baseUri: Uri, uriPaths: List[String], uriParams: List[(String, String)]): Uri = {
    baseUri
      .addPath(uriPaths)
      .addParams(uriParams: _*)
  }

  def sendRequest(uri: Uri): Either[Exception, Response[String]] = {
    val backend = HttpURLConnectionBackend()

    EnhancedConsoleLog.Method.printlnGet(uri)

    val response = quickRequest
      .get(uri)
      .send(backend)

    EnhancedConsoleLog.Response.printlnResponse(response)

    if (response.code.isClientError || response.code.isServerError)
      Left(new Exception(response.code.toString() + response.statusText))
    else
      Right(response)
  }



}
