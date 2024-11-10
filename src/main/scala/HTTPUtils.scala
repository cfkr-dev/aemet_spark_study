import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, UriContext, quickRequest}
import sttp.model.Uri

object HTTPUtils {
  def buildUrl(baseUrl: String, urlSegments: List[String], urlQueryParams: List[(String, String)]): Uri = {
    uri"${baseUrl.format(urlSegments: _*)}".addParams(urlQueryParams: _*)
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
