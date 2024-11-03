import java.nio.file.Paths

object Constants {
  val aemetAPIURL: String = "https://opendata.aemet.es/opendata/api"
  val aemetAllStationsMeteorologicalDataBetweenDatesEndpoint: String = aemetAPIURL + "/valores/climatologicos/diarios/datos/fechaini/%s/fechafin/%s/todasestaciones/"

  val aemetAPIKeyQueryParamName: String = "api_key"
  val aemetAPIKeyPath: String = Paths.get("aemet_api.key").toString

  val aemetJSONBasePath: String = "./aemet_json/"
  val aemetJSONAllStationsMeteorologicalDataBetweenDates: String = aemetJSONBasePath + "all_stations_meteorological_data_between_dates/data/"
  val aemetJSONAllStationsMeteorologicalMetadataBetweenDates: String = aemetJSONBasePath + "all_stations_meteorological_data_between_dates/metadata/"

  val startDate: String = "1973-01-01T00:00:00Z"
  val endDate: String = "2023-12-31T23:59:59Z"

  val aemetTypesToJSONCasting: Map[String, ujson.Value => ujson.Value] = Map(
    "string" -> ((value: ujson.Value) => ujson.Str(value.str)),
    "float" -> ((value: ujson.Value) => ujson.Num(value.num))
  )
  val minimumMillisBetweenRequest: Double = 1200
}
