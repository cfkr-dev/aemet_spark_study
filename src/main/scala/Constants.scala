import java.nio.file.Paths

object Constants {

  object AemetAPI {
    object AllStationsMeteorologicalDataBetweenDates {
      val dataEndpoint: String = Global.baseURL + "/valores/climatologicos/diarios/datos/fechaini/%s/fechafin/%s/todasestaciones/"
      val lastSavedDatesLastEndDateParamName: String = "last_end_date"
    }

    object AllStationsData {

    }

    object Global {
      val baseURL: String = "https://opendata.aemet.es/opendata/api"
      val apiKeyQueryParamName: String = "api_key"
    }
  }

  object AemetPaths {
    object AllStationsMeteorologicalDataBetweenDates {
      val jsonData: String = Global.jsonBase + "all_stations_meteorological_data_between_dates/data/"
      val jsonDataFilename: String = "%s_%s_data.json"

      val jsonMetadata: String = Global.jsonBase + "all_stations_meteorological_data_between_dates/metadata/"
      val jsonMetadataFilename: String = "metadata.json"
      val jsonMetadataFile: String = jsonMetadata + jsonMetadataFilename

      val jsonLastSavedDatesFilename: String = "last_saved_dates.json"
      val jsonLastSavedDatesFile: String = jsonMetadata + jsonLastSavedDatesFilename

    }

    object AllStationsData {

    }

    object Global {
      val jsonBase: String = "./aemet_json/"
      val secrets: String = "./secrets/"
      val apiKeyFile: String = secrets + "aemet_api.key"
    }
  }

  object Params {
    object AllStationsMeteorologicalDataBetweenDates {
      val dateHourFormat: String = "yyyy-MM-dd'T'HH:mm:ss'UTC'"
      val dateFormat: String = "yyyy-MM-dd"

      val startDate: String = "1973-01-01T00:00:00Z"
      val endDate: String = "2023-12-31T23:59:59Z"
    }

    object AllStationsData {

    }

    object Global {
      val minimumMillisBetweenRequest: Long = 2600
      val minimumMillisBetweenRequestMetadata: Long = 3800
    }
  }


}
