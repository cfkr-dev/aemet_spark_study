package Config

object Constants {

  object AemetAPI {
    object AllStationsMeteorologicalDataBetweenDates {
      val dataEndpoint: String = Global.baseURL + "/valores/climatologicos/diarios/datos/fechaini/%s/fechafin/%s/todasestaciones/"
      val lastSavedDatesLastEndDateParamName: String = "last_end_date"
    }

    object AllStationsData {
      val dataEndpoint: String = Global.baseURL + "/valores/climatologicos/inventarioestaciones/todasestaciones/"
    }

    object Global {
      val baseURL: String = "https://opendata.aemet.es/opendata/api"
      val apiKeyQueryParamName: String = "api_key"

      val responseStateNumberKey: String = "estado"
      val responseMetadataKey: String = "metadatos"
      val responseDataKey: String = "datos"

      val metadataFields: String = "campos"
      val metadataFieldsID: String = "id"
      val metadataFieldsRequired: String = "requerido"
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
      val jsonData: String = Global.jsonBase + "all_stations/data/"
      val jsonDataFilename: String = "all_stations_data.json"

      val jsonMetadata: String = Global.jsonBase + "all_stations/metadata/"
      val jsonMetadataFilename: String = "metadata.json"
      val jsonMetadataFile: String = jsonMetadata + jsonMetadataFilename
    }

    object Global {
      val jsonBase: String = "./aemet_json/"
      val secrets: String = "./secrets/"
      val apiKeyFile: String = secrets + "aemet_api.key"
    }
  }

  object IfapaAPI {
    object StationMeteorologicalDataBetweenDates {
      val dataEndpoint: String = Global.baseURL + "/datosdiarios/%s/%s/%s/%s/%s"
    }

    object StationData {
      val dataEndpoint: String = Global.baseURL + "/estaciones/%s/%s"
    }

    object Global {
      val baseURL: String = "https://www.juntadeandalucia.es/agriculturaypesca/ifapa/riaws"
      val dataEndpointMetadata: String = Global.baseURL + "/v2/api-docs"

      val definitionsField = "definitions"
      val definitionsFieldSingleStationOneDate = "DatoDiario"
      val definitionsFieldStationInfo = "Estacion"
      val definitionsFieldProperties = "properties"
    }
  }

  object IfapaPaths {
    object StationMeteorologicalDataBetweenDates {
      val jsonData: String = Global.jsonBase + "station_meteorological_data_between_dates/data/"
      val jsonDataFilename: String = "%s_%s_data.json"

      val jsonMetadata: String = Global.jsonBase + "station_meteorological_data_between_dates/metadata/"
      val jsonMetadataFilename: String = "metadata.json"
      val jsonMetadataFile: String = jsonMetadata + jsonMetadataFilename
    }

    object StationData {
      val jsonData: String = Global.jsonBase + "station_data/data/"
      val jsonDataFilename: String = "%s_%s_data.json"

      val jsonMetadata: String = Global.jsonBase + "station_data/metadata/"
      val jsonMetadataFilename: String = "metadata.json"
      val jsonMetadataFile: String = jsonMetadata + jsonMetadataFilename
    }

    object Global {
      val jsonBase: String = "./ifapa_json/"
    }
  }

  object Params {
    object AllStationsMeteorologicalDataBetweenDates {
      val dateHourFormat: String = "yyyy-MM-dd'T'HH:mm:ss'UTC'"
      val dateHourFormatRaw: String = "yyyy-MM-dd'T'HH:mm:ss'Z'"
      val dateFormat: String = "yyyy-MM-dd"

      val startDate: String = "1973-01-01T00:00:00Z"
      val endDate: String = "2024-12-31T23:59:59Z"
    }

    object AllStationsData {

    }

    object StationMeteorologicalDataBetweenDates {
      val startDateIfapa: String = "2024-01-01"
      val endDateIfapa: String = "2024-12-31"
    }

    object Ifapa {
      val state_code_almeria: Int = 4
      val station_code_tabernas: Int = 4
    }

    object Global {
      val minimumMillisBetweenRequest: Long = 2600
      val minimumMillisBetweenRequestMetadata: Long = 3800
      val HTTPHeaderAccept: (String, String) = ("Accept", "application/json")
      val HTTPHeaderUserAgent: (String, String) = ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
    }
  }
}
