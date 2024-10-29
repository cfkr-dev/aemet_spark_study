import sttp.client4.UriContext
import sttp.model.Uri

import java.nio.file.Paths

object Constants {
  val aemetURL: Uri = uri"https://opendata.aemet.es/opendata/api"
  val aemetAPIKeyPath: String = Paths.get("aemet_api.key").toString
  val startDate: String = "1973-01-01T00:00:00Z"
  val endDate: String = "2023-12-31T23:59:59Z"

  val aemetTypesToJSONCasting: Map[String, ujson.Value => ujson.Value] = Map(
    "string" -> ((value: ujson.Value) => ujson.Str(value.str)),
    "float" -> ((value: ujson.Value) => ujson.Num(value.num))
  )
}
