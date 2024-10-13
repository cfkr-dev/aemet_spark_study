import sttp.client4.UriContext
import sttp.model.Uri

import java.nio.file.Paths

object Constants {
  val aemetURL: Uri = uri"https://opendata.aemet.es/opendata/api/"
  val aemetAPIKeyPath: String = Paths.get("aemet_api.key").toString
}
