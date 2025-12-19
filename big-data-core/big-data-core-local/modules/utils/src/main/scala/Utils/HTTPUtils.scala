package Utils

import sttp.client4.httpurlconnection.HttpURLConnectionBackend
import sttp.client4.okhttp.OkHttpSyncBackend
import sttp.client4.{Response, UriContext, quickRequest}
import sttp.model.Uri
import ujson.{Value, write}

import java.util.concurrent.TimeUnit

/**
 * Small HTTP helpers built on `sttp` to `GET` and `POST` JSON payloads.
 *
 * Provides `buildUrl` overloads to construct a `Uri` from a base string and
 * optional segments or query parameters, and `sendGetRequest` / `sendPostRequest`
 * which print logs and return `Either[Exception, Response[String]]`.
 */
object HTTPUtils {
  /**
   * Build a `Uri` from a base URL and formatted segments and query parameters.
   *
   * @param baseUrl base URL with formatting placeholders
   * @param urlSegments list of `String` segments to format into `baseUrl`
   * @param urlQueryParams list of `(name, value)` pairs to add as query params
   * @return constructed `Uri`
   */
  def buildUrl(baseUrl: String, urlSegments: List[String], urlQueryParams: List[(String, String)]): Uri = {
    uri"${baseUrl.format(urlSegments: _*)}".addParams(urlQueryParams: _*)
  }

  /**
   * Build a `Uri` from a base URL and formatted segments.
   *
   * @param baseUrl base URL with formatting placeholders
   * @param urlSegments list of `String` segments to format into `baseUrl`
   * @return constructed `Uri`
   */
  def buildUrl(baseUrl: String, urlSegments: List[String]): Uri = {
    uri"${baseUrl.format(urlSegments: _*)}"
  }

  /**
   * Build a `Uri` directly from a base string.
   *
   * @param baseUrl the base URL string
   * @return constructed `Uri`
   */
  def buildUrl(baseUrl: String): Uri = {
    uri"$baseUrl"
  }

  /**
   * Send a `GET` request to the provided `Uri` and return the `Response` or an `Exception`.
   * The function logs the request and the response; client/server error codes are
   * returned as `Left(Exception)`.
   *
   * @param uri the target `Uri`
   * @return `Right(Response[String])` on success or `Left(Exception)` on error
   */
  def sendGetRequest(uri: Uri): Either[Exception, Response[String]] = {

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

  /**
   * Send a `POST` request with a JSON `body` to the provided `Uri` and return
   * the `Response` or an `Exception`. Uses an OkHttp client with extended
   * timeouts suitable for long-running plot generation requests.
   *
   * @param uri the target `Uri`
   * @param body `ujson.Value` payload to send as pretty-printed JSON
   * @return `Right(Response[String])` on success or `Left(Exception)` on error
   */
  def sendPostRequest(uri: Uri, body: Value): Either[Exception, Response[String]] = {
    val backend = OkHttpSyncBackend.usingClient(
      new okhttp3.OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.MINUTES)
        .readTimeout(10, TimeUnit.MINUTES)
        .writeTimeout(10, TimeUnit.MINUTES)
        .build()
    )

    ConsoleLogUtils.Method.printlnPost(uri)

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
        .post(uri)
        .body(write(body, 2))
        .header(HTTPHeaderAccept._1, HTTPHeaderAccept._2)
        .header(HTTPHeaderUserAgent._1, HTTPHeaderUserAgent._2)
        .contentType("application/json")
        .send(backend)

      ConsoleLogUtils.Response.printlnResponse(response)

      if (response.code.isClientError || response.code.isServerError)
        Left(new Exception(response.code.toString() + response.statusText + "\n" + response.body))
      else
        Right(response)
    } catch {
      case exception: Exception => Left(exception)
    }
  }

}
