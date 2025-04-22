package Utils

import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.{Response, UriContext, quickRequest}
import sttp.model.Uri

object HTTPUtils {
  def buildUrl(baseUrl: String, urlSegments: List[String], urlQueryParams: List[(String, String)]): Uri = {
    uri"${baseUrl.format(urlSegments: _*)}".addParams(urlQueryParams: _*)
  }

  def buildUrl(baseUrl: String, urlSegments: List[String]): Uri = {
    uri"${baseUrl.format(urlSegments: _*)}"
  }

  def buildUrl(baseUrl: String): Uri = {
    uri"$baseUrl"
  }

  def sendRequest(uri: Uri): Either[Exception, Response[String]] = {

    val backend = HttpURLConnectionBackend()

    ConsoleLogUtils.Method.printlnGet(uri)

    val HTTPHeaderAccept = (
      "Accept",
      "application/json"
    )
    val HTTPHeaderUserAgent = (
      "User-Agent",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    )

    try {
      val response = quickRequest
        .get(uri)
        .header(HTTPHeaderAccept._1, HTTPHeaderAccept._2)
        .header(HTTPHeaderUserAgent._1, HTTPHeaderUserAgent._2)
        .send(backend)

      ConsoleLogUtils.Response.printlnResponse(response)

      if (response.code.isClientError || response.code.isServerError)
        Left(new Exception(response.code.toString() + response.statusText))
      else
        Right(response)
    } catch {
      case exception: Exception => Left(exception)
    }

  }

  //  def sendAsyncRequest(uri: Uri, executionContext: ExecutionContextExecutorService): Either[Exception, Response[String]] = {
  //    implicit val exCtx = executionContext
  //    implicit val backend = HttpClientFutureBackend()
  //
  //    val HTTPHeaderAccept = Config.Params.Global.HTTPHeaderAccept
  //    val HTTPHeaderUserAgent = Config.Params.Global.HTTPHeaderUserAgent
  //
  //    try {
  //      val response = quickRequest
  //        .get(uri)
  //        .header(HTTPHeaderAccept._1, HTTPHeaderAccept._2)
  //        .header(HTTPHeaderUserAgent._1, HTTPHeaderUserAgent._2)
  //        .send(backend)
  //        .onComplete {
  //          case Success(response: Response[String]) =>
  //            if (response.code.isClientError || response.code.isServerError)
  //              Left(new Exception(response.code.toString() + response.statusText))
  //            else
  //              Right(response)
  //          case Failure(exception: Exception) =>
  //            Left(exception)
  //        }
  //    }
  //
  //  }

}
