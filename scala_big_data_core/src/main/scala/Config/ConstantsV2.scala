package Config

object ConstantsV2 {
  object RemoteRequest {
    object AemetAPI {
      object URI {
        object AllMeteoInfo {
          val dataEndpoint: String = Global.baseURL + "/valores/climatologicos/diarios/datos/fechaini/%s/fechafin/%s/todasestaciones/"
        }

        object AllStationInfo {
          val dataEndpoint: String = Global.baseURL + "/valores/climatologicos/inventarioestaciones/todasestaciones/"
        }

        object Global {
          val baseURL: String = "https://opendata.aemet.es/opendata/api"
        }
      }

      object Params {
        object AllMeteoInfo {
          object LastSavedDatesJSONKeys {
            val lastEndDate: String = "last_end_date"
          }

          object Metadata {
            object DataFieldsJSONKeys {
              val fechaJKey: String = "fecha"
              val indicativoJKey: String = "indicativo"
              val nombreJKey: String = "nombre"
              val provinciaJKey: String = "provincia"
              val altitudJKey: String = "altitud"
              val tmedJKey: String = "tmed"
              val precJKey: String = "prec"
              val tminJKey: String = "tmin"
              val horatminJKey: String = "horatmin"
              val tmaxJKey: String = "tmax"
              val horatmaxJKey: String = "horatmax"
              val dirJKey: String = "dir"
              val velmediaJKey: String = "velmedia"
              val rachaJKey: String = "racha"
              val horarachaJKey: String = "horaracha"
              val solJKey: String = "sol"
              val presmaxJKey: String = "presmax"
              val horapresmaxJKey: String = "horapresmax"
              val presminJKey: String = "presmin"
              val horapresminJKey: String = "horapresmin"
              val hrmediaJKey: String = "hrmedia"
              val hrmaxJKey: String = "hrmax"
              val horahrmaxJKey: String = "horahrmax"
              val hrminJKey: String = "hrmin"
              val horahrminJKey: String = "horahrmin"
            }

            val fields: List[String] = List(
              this.DataFieldsJSONKeys.fechaJKey,
              this.DataFieldsJSONKeys.indicativoJKey,
              this.DataFieldsJSONKeys.nombreJKey,
              this.DataFieldsJSONKeys.provinciaJKey,
              this.DataFieldsJSONKeys.altitudJKey,
              this.DataFieldsJSONKeys.tmedJKey,
              this.DataFieldsJSONKeys.precJKey,
              this.DataFieldsJSONKeys.tminJKey,
              this.DataFieldsJSONKeys.horatminJKey,
              this.DataFieldsJSONKeys.tmaxJKey,
              this.DataFieldsJSONKeys.horatmaxJKey,
              this.DataFieldsJSONKeys.dirJKey,
              this.DataFieldsJSONKeys.velmediaJKey,
              this.DataFieldsJSONKeys.rachaJKey,
              this.DataFieldsJSONKeys.horarachaJKey,
              this.DataFieldsJSONKeys.solJKey,
              this.DataFieldsJSONKeys.presmaxJKey,
              this.DataFieldsJSONKeys.horapresmaxJKey,
              this.DataFieldsJSONKeys.presminJKey,
              this.DataFieldsJSONKeys.horapresminJKey,
              this.DataFieldsJSONKeys.hrmediaJKey,
              this.DataFieldsJSONKeys.hrmaxJKey,
              this.DataFieldsJSONKeys.horahrmaxJKey,
              this.DataFieldsJSONKeys.hrminJKey,
              this.DataFieldsJSONKeys.horahrminJKey
            )
          }

          object Execution {
            object Args {
              val startDate: String = "1973-01-01T00:00:00Z"
              val endDate: String = "2024-12-31T23:59:59Z"
            }

            object Format {
              val dateHour: String = "yyyy-MM-dd'T'HH:mm:ss'UTC'"
              val dateHourRaw: String = "yyyy-MM-dd'T'HH:mm:ss'Z'"
              val dateFormat: String = "yyyy-MM-dd"
              val dateFormatFile: String = "yyyy_MM_dd"
              val hourMinuteFormat: String = "HH:mm"
            }
          }
        }

        object AllStationInfo {
          object Metadata {
            object DataFieldsJSONKeys {
              val latitudJKey: String = "latitud"
              val provinciaJKey: String = "provincia"
              val indicativoJKey: String = "indicativo"
              val altitudJKey: String = "altitud"
              val nombreJKey: String = "nombre"
              val indsinopJKey: String = "indsinop"
              val longitudJKey: String = "longitud"
            }

            val fields: List[String] = List(
              DataFieldsJSONKeys.latitudJKey,
              DataFieldsJSONKeys.provinciaJKey,
              DataFieldsJSONKeys.indicativoJKey,
              DataFieldsJSONKeys.altitudJKey,
              DataFieldsJSONKeys.nombreJKey,
              DataFieldsJSONKeys.indsinopJKey,
              DataFieldsJSONKeys.longitudJKey
            )
          }
        }

        object Global {
          object RequestJSONKeys {
            val apiKey: String = "api_key"
          }

          object ResponseJSONKeys {
            val stateNumber: String = "estado"
            val metadata: String = "metadatos"
            val data: String = "datos"
          }

          object Metadata {
            object SchemaJSONKeys {
              val fieldDefJKey: String = "campos"

              object DataFieldsJSONKeys {
                val idJKey: String = "id"
                val requiredJKey: String = "requerido"
              }
            }
          }

          object Execution {
            object Time {
              val minimumMillisBetweenRequest: Long = 2600
              val minimumMillisBetweenMetadataRequest: Long = 3800
            }
          }
        }
      }
    }

    object IfapaAPI {
      object URI {
        object SingleStationMeteoInfo {
          val dataEndpoint: String = Global.baseURL + "/datosdiarios/%s/%s/%s/%s/%s"
        }

        object SingleStationInfo {
          val dataEndpoint: String = Global.baseURL + "/estaciones/%s/%s"
        }

        object SingleStateInfo {
          val dataEndpoint: String = Global.baseURL + "/provincias/%s"
        }

        object AllMetadata {
          val dataEndpoint: String = Global.baseURL + "/v2/api-docs"
        }

        object Global {
          val baseURL: String = "https://www.juntadeandalucia.es/agriculturaypesca/ifapa/riaws"
        }
      }

      object Params {
        object SingleStationMeteoInfo {
          object Metadata {
            object DataFieldsJSONKeys {
              val bateriaJKey: String = "bateria"
              val diaJKey: String = "dia"
              val dirvientoJKey: String = "dirviento"
              val dirvientovelmaxJKey: String = "dirvientovelmax"
              val et0JKey: String = "et0"
              val fechaJKey: String = "fecha"
              val fechautlmodJKey: String = "fechautlmod"
              val horminhummaxJKey: String = "horminhummax"
              val horminhumminJKey: String = "horminhummin"
              val hormintempmaxJKey: String = "hormintempmax"
              val hormintempminJKey: String = "hormintempmin"
              val horminvelmaxJKey: String = "horminvelmax"
              val humedadmaxJKey: String = "humedadmax"
              val humedadmediaJKey: String = "humedadmedia"
              val humedadminJKey: String = "humedadmin"
              val precipitacionJKey: String = "precipitacion"
              val radiacionJKey: String = "radiacion"
              val tempmaxJKey: String = "tempmax"
              val tempmediaJKey: String = "tempmedia"
              val tempminJKey: String = "tempmin"
              val velvientoJKey: String = "velviento"
              val velvientomaxJKey: String = "velvientomax"
            }

            val fields: List[String] = List(
              this.DataFieldsJSONKeys.bateriaJKey,
              this.DataFieldsJSONKeys.diaJKey,
              this.DataFieldsJSONKeys.dirvientoJKey,
              this.DataFieldsJSONKeys.dirvientovelmaxJKey,
              this.DataFieldsJSONKeys.et0JKey,
              this.DataFieldsJSONKeys.fechaJKey,
              this.DataFieldsJSONKeys.fechautlmodJKey,
              this.DataFieldsJSONKeys.horminhummaxJKey,
              this.DataFieldsJSONKeys.horminhumminJKey,
              this.DataFieldsJSONKeys.hormintempmaxJKey,
              this.DataFieldsJSONKeys.hormintempminJKey,
              this.DataFieldsJSONKeys.horminvelmaxJKey,
              this.DataFieldsJSONKeys.humedadmaxJKey,
              this.DataFieldsJSONKeys.humedadmediaJKey,
              this.DataFieldsJSONKeys.humedadminJKey,
              this.DataFieldsJSONKeys.precipitacionJKey,
              this.DataFieldsJSONKeys.radiacionJKey,
              this.DataFieldsJSONKeys.tempmaxJKey,
              this.DataFieldsJSONKeys.tempmediaJKey,
              this.DataFieldsJSONKeys.tempminJKey,
              this.DataFieldsJSONKeys.velvientoJKey,
              this.DataFieldsJSONKeys.velvientomaxJKey
            )
          }

          object Execution {
            object Args {
              val startDate: String = "2024-01-01"
              val endDate: String = "2024-12-31"
              val stateAlmeriaCode: String = "4"
              val stationTabernasCode: String = "4"
            }
          }
        }

        object SingleStationInfo {
          object Metadata {
            object DataFieldsJSONKeys {
              val activaJKey: String = "activa"
              val altitudJKey: String = "altitud"
              val bajoplasticoJKey: String = "bajoplastico"
              val codigoestacionJKey: String = "codigoestacion"
              val husoJKey: String = "huso"
              val latitudJKey: String = "latitud"
              val longitudJKey: String = "longitud"
              val nombreJKey: String = "nombre"
              val provinciaJKey: String = "provincia"
              val visibleJKey: String = "visible"
              val xutmJKey: String = "xutm"
              val yutmJKey: String = "yutm"
            }

            val fields: List[String] = List(
              this.DataFieldsJSONKeys.activaJKey,
              this.DataFieldsJSONKeys.altitudJKey,
              this.DataFieldsJSONKeys.bajoplasticoJKey,
              this.DataFieldsJSONKeys.codigoestacionJKey,
              this.DataFieldsJSONKeys.husoJKey,
              this.DataFieldsJSONKeys.latitudJKey,
              this.DataFieldsJSONKeys.longitudJKey,
              this.DataFieldsJSONKeys.nombreJKey,
              this.DataFieldsJSONKeys.provinciaJKey,
              this.DataFieldsJSONKeys.visibleJKey,
              this.DataFieldsJSONKeys.xutmJKey,
              this.DataFieldsJSONKeys.yutmJKey
            )
          }

          object Execution {
            object Args {
              val stateAlmeriaCode: String = "4"
              val stationTabernasCode: String = "4"
            }
          }
        }

        object SingleStateInfo {
          object Metadata {
            object DataFieldsJSONKeys {
              val idJKey: String = "id"
              val nombreJKey: String = "nombre"
            }

            val fields: List[String] = List(
              this.DataFieldsJSONKeys.idJKey,
              this.DataFieldsJSONKeys.nombreJKey
            )
          }

          object Execution {
            object Args {
              val stateAlmeriaCode: String = "4"
            }
          }
        }

        object Global {
          object ResponseJSONKeys {
            val definitions: String = "definitions"
          }

          object Metadata {
            object SchemaJSONKeys {
              val singleStationMeteoInfo: String = "DatoDiario"
              val singleStationInfo: String = "Estacion"
              val DataField: String = "properties"
            }
          }

          object Execution {
            object Time {
              val minimumMillisBetweenRequest: Long = 2600
              val minimumMillisBetweenMetadataRequest: Long = 3800
            }
          }
        }
      }
    }

    object Global {
      object Params {
        object Global {
          object Execution {
            object HTTPHeaders {
              val HTTPHeaderAccept: (String, String) = (
                "Accept",
                "application/json"
              )
              val HTTPHeaderUserAgent: (String, String) = (
                "User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
              )
            }
          }
        }
      }
    }
  }

  object Storage {
    object DataAemet {
      object AllMeteoInfo {
        object Dirs {
          val dataRegistry: String = Global.Dirs.base + "all_stations_meteo_info/data/"
          val metadata: String = Global.Dirs.base + "all_stations_meteo_info/metadata/"
          val lastSavedDate: String = metadata
        }

        object FilePaths {
          val dataRegistry: String = Dirs.dataRegistry + FileNames.dataRegistry
          val metadata: String = Dirs.metadata + FileNames.metadata
          val lastSavedDate: String = Dirs.lastSavedDate + FileNames.lastSavedDate
        }

        object FileNames {
          val dataRegistry: String = "%s_%s.json"
          val metadata: String = "metadata.json"
          val lastSavedDate: String = "last_saved_date.json"
        }
      }

      object AllStationInfo {
        object Dirs {
          val dataRegistry: String = Global.Dirs.base + "all_stations_info/data/"
          val metadata: String = Global.Dirs.base + "all_stations_info/metadata/"
        }

        object FilePaths {
          val dataRegistry: String = Dirs.dataRegistry + FileNames.dataRegistry
          val metadata: String = Dirs.metadata + FileNames.metadata
        }

        object FileNames {
          val dataRegistry: String = "all_station.json"
          val metadata: String = "metadata.json"
        }
      }

      object Global {
        object Dirs {
          val base: String = Storage.Global.Global.Dirs.base + "source/aemet/"
        }

        object FilePaths {
          val apiKey: String = Storage.Global.Global.Dirs.secrets + "aemet_api.key"
        }

      }
    }

    object DataIfapa {
      object SingleStationMeteoInfo {
        object Dirs {
          val dataRegistry: String = Global.Dirs.base + "single_station_meteo_info/data/"
          val metadata: String = Global.Dirs.base + "single_station_meteo_info/metadata/"
        }

        object FilePaths {
          val dataRegistry: String = Dirs.dataRegistry + FileNames.dataRegistry
          val metadata: String = Dirs.metadata + FileNames.metadata
        }

        object FileNames {
          val dataRegistry: String = "%s_%s.json"
          val metadata: String = "metadata.json"
        }
      }

      object SingleStationInfo {
        object Dirs {
          val dataRegistry: String = Global.Dirs.base + "single_station_info/data/"
          val metadata: String = Global.Dirs.base + "single_station_info/metadata/"
        }

        object FilePaths {
          val dataRegistry: String = Dirs.dataRegistry + FileNames.dataRegistry
          val metadata: String = Dirs.metadata + FileNames.metadata
        }

        object FileNames {
          val dataRegistry: String = "%s_%s.json"
          val metadata: String = "metadata.json"
        }
      }

      object Global {
        object Dirs {
          val base: String = Storage.Global.Global.Dirs.base + "source/ifapa/"
        }
      }
    }

    object DataIfapaAemetFormat {
      object SingleStationMeteoInfo {
        object Dirs {
          val dataRegistry: String = Global.Dirs.base + "single_station_meteo_info/data/"
          val metadata: String = Global.Dirs.base + "single_station_meteo_info/metadata/"
        }

        object FilePaths {
          val dataRegistry: String = Dirs.dataRegistry + FileNames.dataRegistry
          val metadata: String = Dirs.metadata + FileNames.metadata
        }

        object FileNames {
          val dataRegistry: String = "%s_%s.json"
          val metadata: String = "metadata.json"
        }
      }

      object SingleStationInfo {
        object Dirs {
          val dataRegistry: String = Global.Dirs.base + "single_station_info/data/"
          val metadata: String = Global.Dirs.base + "single_station_info/metadata/"
        }

        object FilePaths {
          val dataRegistry: String = Dirs.dataRegistry + FileNames.dataRegistry
          val metadata: String = Dirs.metadata + FileNames.metadata
        }

        object FileNames {
          val dataRegistry: String = "%s_%s.json"
          val metadata: String = "metadata.json"
        }
      }

      object Global {
        object Dirs {
          val base: String = Storage.Global.Global.Dirs.base + "source/ifapa_aemet_format/"
        }
      }
    }

    object DataSpark {
      object Stations {
        object StationCountEvolFromStart {
          object Dirs {
            val result: String = Stations.Global.Dirs.base + "count_evol/"
          }
        }
        object StationCountByState2024 {
          object Dirs {
            val result: String = Stations.Global.Dirs.base + "count_by_state_2024/"
          }
        }
        object StationCountByAltitude2024 {
          object Dirs {
            val result: String = Stations.Global.Dirs.base + "count_by_altitude_2024/"
          }
        }
        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "stations/"
          }
        }
      }
      object Climograph {
        object Dirs {
          val resultStation: String = Climograph.Global.Dirs.base + "%s/%s/%s/station"
          val resultTempPrec: String = Climograph.Global.Dirs.base + "%s/%s/%s/temp_and_prec"
        }

        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "climograph/"
          }
        }
      }
      object SingleParamStudies {
        object Top10 {
          object Dirs {
            val resultHighest2024: String = Global.Dirs.base + "top_10/highest/2024/"
            val resultHighestDecade: String = Global.Dirs.base + "top_10/highest/decade/"
            val resultHighestGlobal: String = Global.Dirs.base + "top_10/highest/global/"
            val resultLowest2024: String = Global.Dirs.base + "top_10/lowest/2024/"
            val resultLowestDecade: String = Global.Dirs.base + "top_10/lowest/decade/"
            val resultLowestGlobal: String = Global.Dirs.base + "top_10/lowest/global/"
          }
        }

        object EvolFromStartForEachState {
          object Dirs {
            val resultStation: String = Global.Dirs.base + "evol/%s/station/"
            val resultEvol: String = Global.Dirs.base + "evol/%s/evol/"
            val resultEvolRegression: String = Global.Dirs.base + "evol/%s/regression/"
          }
        }

        object Top5Inc {
          object Dirs {
            val resultHighest: String = Global.Dirs.base + "top_5_inc/highest/"
            val resultLowest: String = Global.Dirs.base + "top_5_inc/lowest/"
          }
        }

        object Avg2024AllStationSpain {
          object Dirs {
            val resultContinental: String = Global.Dirs.base + "avg_2024_spain/continental/"
            val resultCanary: String = Global.Dirs.base + "avg_2024_spain/canary_islands/"
          }
        }

        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "%s/"
          }
        }
      }
      object InterestingStudies {
        object Top10InterestingStudies {
          object Dirs {
            val resultBetterWindPower: String = InterestingStudies.Global.Dirs.base + "top_10_better_wind_power/"
            val resultBetterSunPower: String = InterestingStudies.Global.Dirs.base + "top_10_better_sun_power/"
            val resultTorrentialRains: String = InterestingStudies.Global.Dirs.base + "top_10_torrential_rains/"
            val resultStorms: String = InterestingStudies.Global.Dirs.base + "top_10_storms/"
            val resultAgriculture: String = InterestingStudies.Global.Dirs.base + "top_10_agriculture/"
            val resultDroughts: String = InterestingStudies.Global.Dirs.base + "top_10_droughts/"
            val resultFires: String = InterestingStudies.Global.Dirs.base + "top_10_fires/"
            val resultHeatWaves: String = InterestingStudies.Global.Dirs.base + "top_10_heat_waves/"
            val resultFrosts: String = InterestingStudies.Global.Dirs.base + "top_10_frosts/"
          }
        }

        object PrecAndPressionEvolFromStartForEachState {
          object Dirs {
            val resultStation: String = InterestingStudies.Global.Dirs.base + "prec_and_pression_evol/%s/station/"
            val resultEvol: String = InterestingStudies.Global.Dirs.base + "prec_and_pression_evol/%s/evol/"
          }
        }

        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "interesting_studies/"
          }
        }
      }
      object Global {
        object Dirs {
          val base: String = Storage.Global.Global.Dirs.base + "results/spark/"
        }
      }
    }

    object Global {
      object Global {
        object Dirs {
          val base: String = "../data/"
          val secrets: String = "./secrets/"
        }
      }
    }
  }

  object Logs {
    object Aemet {
      object AllMeteoInfo {

      }

      object AllStationInfo {

      }

      object Global {
        object GetAemetResource {
          val failOnGettingJSON: String = "Fail on getting JSON (%s)"
        }

        object AemetDataExtraction {
          val allStationInfoStartFetchingMetadata: String = "Fetching metadata from all Aemet stations"
          val allStationInfoEndFetchingMetadata: String = "Completed fetching metadata from all Aemet station"
          val allStationInfoStartFetchingData: String = "Fetching data from all Aemet stations"
          val allStationInfoEndFetchingData: String = "Completed fetching data from all Aemet station"
          val allMeteoInfoStartFetchingMetadata: String = "Fetching metadata from all Aemet meteorological registers"
          val allMeteoInfoEndFetchingMetadata: String = "Completed fetching metadata from all Aemet meteorological registers"
          val allMeteoInfoStartFetchingData: String = "Fetching data from all Aemet meteorological registers"
          val allMeteoInfoEndFetchingData: String = "Completed fetching data from all Aemet meteorological registers"
        }
      }
    }

    object Ifapa {
      object SingleStationMeteoInfo {

      }

      object SingleStationInfo {

      }

      object Global {
        object IfapaDataExtraction {
          val singleStationInfoStartFetchingMetadata: String = "Fetching metadata from Ifapa Tabernas (Almería) station"
          val singleStationInfoEndFetchingMetadata: String = "Completed fetching metadata from Ifapa Tabernas (Almería) station"
          val singleStationInfoStartFetchingData: String = "Fetching data from Ifapa Tabernas (Almería) station"
          val singleStationInfoEndFetchingData: String = "Completed fetching data from Ifapa Tabernas (Almería) station"
          val singleStationMeteoInfoStartFetchingMetadata: String = "Fetching metadata from Ifapa Tabernas (Almería) meteorological registers"
          val singleStationMeteoInfoEndFetchingMetadata: String = "Completed fetching metadata from Ifapa Tabernas (Almería) meteorological registers"
          val singleStationMeteoInfoStartFetchingData: String = "Fetching data from Ifapa Tabernas (Almería) meteorological registers"
          val singleStationMeteoInfoEndFetchingData: String = "Completed fetching data from Ifapa Tabernas (Almería) meteorological registers"
        }
      }
    }

    object IfapaAemetFormat {
      object SingleStationMeteoInfo {

      }

      object SingleStationInfo {

      }

      object Global {
        object IfapaToAemet {
          val singleStationInfoStartConverting: String = "Converting data from Ifapa Tabernas (Almería) station to Aemet format"
          val singleStationInfoEndConverting: String = "Completed converting data from Ifapa Tabernas (Almería) station to Aemet format"
          val singleStationMeteoInfoStartConverting: String = "Converting data from Ifapa Tabernas (Almería) meteorological registers to Aemet format"
          val singleStationMeteoInfoEndConverting: String = "Completed converting data from Ifapa Tabernas (Almería) meteorological registers to Aemet format"
        }
      }
    }

    object SparkQueries {
      object Studies {
        object Stations {
          val studyName: String = "stations"

          object Execution {
            val stationCountEvolFromStart: String = "Station count evolution from the start of registers"
            val stationCountByState2024: String = "Station count by state in 2024"
            val stationCountByAltitude2024: String = "Station count by altitude in 2024"

          }
        }

        object Climograph {
          val studyName: String = "climograph"

          object Execution {
            val startFetchingClimateGroup: String = "Fetching %s climates information"
            val fetchingClimate: String = "%s climate information"
            val fetchingClimateLocationStation = "%s station information"
            val fetchingClimateLocationTempPrec = "%s temperature and precipitation information"
          }
        }

        object SingleParamStudies {
          object Execution {
            val top10Highest2024: String = "Top 10 places with the highest %s in 2024"
            val top10HighestDecade: String = "Top 10 places with the highest %s in the last decade"
            val top10HighestGlobal: String = "Top 10 places with the highest %s from the start of registers"
            val top10Lowest2024: String = "Top 10 places with the lowest %s in 2024"
            val top10LowestDecade: String = "Top 10 places with the lowest %s in the last decade"
            val top10LowestGlobal: String = "Top 10 places with the lowest %s from the start of registers"
            val evolFromStartForEachState: String = "%s evolution from the start of registries for each state"
            val evolFromStartForEachStateStartStation: String = "Fetching data from representative %s state station"
            val evolFromStartForEachStateStart: String = "Fetching data from %s state %s evolution"
            val evolFromStartForEachStateStartRegression: String = "%s regression model for %s evolution from the start of registers"
            val top5HighestInc: String = "Top 5 places with the highest increment of %s from the start of registers"
            val top5LowestInc: String = "Top 5 places with the lowest increment of %s from the start of registers"
            val avg2024AllStationSpainContinental: String = "Average %s in 2024 for all station in the spanish continental territory"
            val avg2024AllStationSpainCanary: String = "Average %s in 2024 for all station in Canary islands"
          }
        }

        object InterestingStudies {
          val studyName: String = "interesting studies"

          object Execution {
            val precAndPressureEvolFromStartForEachState: String = "Precipitation and pressure evolution from the start of registries for each state"
            val precAndPressureEvolFromStartForEachStateStartStation: String = "Fetching data from representative %s state station"
            val precAndPressureEvolFromStartForEachStateStartEvol: String = "Fetching data from %s state precipitation and pressure evolution"
            val top10BetterWindPower: String = "Top 10 better places for wind power generation in the last decade"
            val top10BetterSunPower: String = "Top 10 better places for sun power generation in the last decade"
            val top10TorrentialRains: String = "Top 10 places with the highest incidence of torrential rains in the last decade"
            val top10Storms: String = "Top 10 the highest incidence of storms in the last decade"
            val top10Agriculture: String = "Top 10 better places for agriculture in the last decade"
            val top10Droughts: String = "Top 10 the highest incidence of droughts in the last decade"
            val top10Fires: String = "Top 10 the highest incidence of fires in the last decade"
            val top10HeatWaves: String = "Top 10 the highest incidence of heat waves in the last decade"
            val top10Frosts: String = "Top 10 the highest incidence of frosts in the last decade"
          }
        }

        object Global {
          val startStudy: String = "Starting %s study"
          val endStudy: String = "Completed %s study"
          val startQuery: String = "Stating query (%s)"
          val endQuery: String = "Completed query (%s)"
          val showInfo: String = "Showing a part of collected information"
          val saveInfo: String = "Saving collected information (%s)"
          val startSubQuery: String = "Stating subquery (%s)"
          val endSubQuery: String = "Completed subquery (%s)"
        }
      }
    }

    object Global {
      object EnhancedConsoleLog {
        object format {
          val dateHour: String = "dd-MM-yyyy HH:mm:ss"
        }

        object Method {
          val methodGet: String = "GET"
        }

        object Message {
          val notificationError: String = "ERR"
          val notificationWarning: String = "WARN"
          val notificationInformation: String = "INFO"
        }

        object Decorators {
          val spaceVerticalDividerSpace: String = " | "
          val spaceBigArrowSpace: String = " => "
          val colonSpace: String = ": "
          val horizontalCenterLine: String = "-"
        }
      }

      object FileUtils {
        val errorInReadingFile: String = "Error in finding file (%s)"
        val errorInDirectoryCreation: String = "Error in directory creation (%s)"
      }

      object Global {}
    }
  }

  object Spark {
    object Queries {
      object Stations {
        object Execution {
          object CountEvolFromStart {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
            val param: String = "fecha"
            val paramGroupName: String = "year"
          }
          object StationCountByState2024 {
            val startDate: String = "2024"
            val param: String = "provincia"
            val paramGroupName: String = "state"
          }
          object StationCountByAltitude2024 {
            val startDate: String = "2024"
            val intervals: List[(String, Double, Double)] = List(
              ("altitud", 0, 500),
              ("altitud", 500, 1000),
              ("altitud", 1000, 1500),
              ("altitud", 1500, 2000),
              ("altitud", 2000, 2500),
              ("altitud", 2500, Double.PositiveInfinity)
            )
          }
        }
      }
      object Climograph {
        val observationYear = 2024
        object Location extends Enumeration {
          type Location = Value
          val Peninsula: Value = Value("peninsula")
          val CanaryIslands: Value = Value("canary islands")
          val BalearIslands: Value = Value("balear islands")
        }
        case class RepresentativeStationRegistry(location: Location.Location, stationId: String)
        case class ClimateRegistry(climateName: String, registries: List[RepresentativeStationRegistry])
        case class ClimateGroup(climateGroupName: String, climates: List[ClimateRegistry])
        val stationsRegistries: List[ClimateGroup] = List(
          ClimateGroup(
            climateGroupName = "arid",
            climates = List(
              ClimateRegistry(
                climateName = "BWh",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "7002Y"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.CanaryIslands,
                    stationId = "C249I"
                  ),
                )
              ),
              ClimateRegistry(
                climateName = "BWk",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "4"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "BSh",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "7012C"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.CanaryIslands,
                    stationId = "C459Z"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.BalearIslands,
                    stationId = "B954"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "BSk",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "9434"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.CanaryIslands,
                    stationId = "C426I"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.BalearIslands,
                    stationId = "B228"
                  )
                )
              ),
            )
          ),
          ClimateGroup(
            climateGroupName = "warm",
            climates = List(
              ClimateRegistry(
                climateName = "Csa",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "5783"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.CanaryIslands,
                    stationId = "C426E"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.BalearIslands,
                    stationId = "B051A"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "Csb",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "1518A"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.CanaryIslands,
                    stationId = "C611E"
                  ),
                  RepresentativeStationRegistry(
                    location = Location.BalearIslands,
                    stationId = "B684A"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "Cfa",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "9901X"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "Cfb",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "1082"
                  )
                )
              ),
            )
          ),
          ClimateGroup(
            climateGroupName = "cold",
            climates = List(
              ClimateRegistry(
                climateName = "Dsb",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "2462"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "Dfb",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "9814I"
                  )
                )
              ),
              ClimateRegistry(
                climateName = "Dfc",
                registries = List(
                  RepresentativeStationRegistry(
                    location = Location.Peninsula,
                    stationId = "9839V"
                  )
                )
              )
            )
          )
        )
      }
      object SingleParamStudies {
        case class RepresentativeStationRegistry(
          stateName: String,
          stateNameNoSC: String,
          stationId: String,
          startDate: String,
          endDate: String
        )

        case class SingleParamStudyValues(
          studyParam: String,
          studyParamAbbrev: String,
          dataframeColName: String,
          reprStationRegs: List[RepresentativeStationRegistry]
        )

        private object StudyParamNames {
          val temperature: String = "temperature"
          val precipitation: String = "precipitation"
          val windVelocity: String = "wind velocity"
          val pressure: String = "pressure"
          val sunRadiation: String = "sun radiation"
          val relativeHumidity: String = "relative humidity"
        }

        private object StudyParamAbbrev {
          val temperature: String = "temp"
          val precipitation: String = "prec"
          val windVelocity: String = "wind_vel"
          val pressure: String = "press"
          val sunRadiation: String = "sun_rad"
          val relativeHumidity: String = "rel_hum"
        }

        val singleParamStudiesValues: List[SingleParamStudyValues] = List(
          SingleParamStudyValues(
            studyParam = StudyParamNames.temperature,
            studyParamAbbrev = StudyParamAbbrev.temperature,
            dataframeColName = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys.tmedJKey,
            reprStationRegs = List(
              RepresentativeStationRegistry(
                stateName = "A CORUÑA",
                stateNameNoSC = "a coruna",
                stationId = "1387",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALBACETE",
                stateNameNoSC = "albacete",
                stationId = "8175",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALICANTE",
                stateNameNoSC = "alicante",
                stationId = "8025",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALMERIA",
                stateNameNoSC = "almeria",
                stationId = "6325O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ARABA/ALAVA",
                stateNameNoSC = "araba",
                stationId = "9091O",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ASTURIAS",
                stateNameNoSC = "asturias",
                stationId = "1212E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "AVILA",
                stateNameNoSC = "avila",
                stationId = "2444",
                startDate = "1983-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BADAJOZ",
                stateNameNoSC = "badajoz",
                stationId = "4452",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BARCELONA",
                stateNameNoSC = "barcelona",
                stationId = "0076",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BIZKAIA",
                stateNameNoSC = "bizkaia",
                stationId = "1082",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BURGOS",
                stateNameNoSC = "burgos",
                stationId = "2331",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CACERES",
                stateNameNoSC = "caceres",
                stationId = "3469A",
                startDate = "1982-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CADIZ",
                stateNameNoSC = "cadiz",
                stationId = "5960",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CANTABRIA",
                stateNameNoSC = "cantabria",
                stationId = "1174I",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CASTELLON",
                stateNameNoSC = "castellon",
                stationId = "8500A",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CEUTA",
                stateNameNoSC = "ceuta",
                stationId = "5000C",
                startDate = "2003-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CIUDAD REAL",
                stateNameNoSC = "cuidad real",
                stationId = "4121",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CORDOBA",
                stateNameNoSC = "cordoba",
                stationId = "5402",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CUENCA",
                stateNameNoSC = "cuenca",
                stationId = "8096",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIPUZKOA",
                stateNameNoSC = "gipuzkoa",
                stationId = "1014",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIRONA",
                stateNameNoSC = "girona",
                stationId = "0367",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GRANADA",
                stateNameNoSC = "granada",
                stationId = "5530E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GUADALAJARA",
                stateNameNoSC = "guadalajara",
                stationId = "3013",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUELVA",
                stateNameNoSC = "huelva",
                stationId = "4642E",
                startDate = "1984-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUESCA",
                stateNameNoSC = "huesca",
                stationId = "9898",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ILLES BALEARS",
                stateNameNoSC = "illes balears",
                stationId = "B893",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "JAEN",
                stateNameNoSC = "jaen",
                stationId = "5270B",
                startDate = "1988-09-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LA RIOJA",
                stateNameNoSC = "la rioja",
                stationId = "9170",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LAS PALMAS",
                stateNameNoSC = "las palmas",
                stationId = "C029O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LEON",
                stateNameNoSC = "leon",
                stationId = "2661",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LLEIDA",
                stateNameNoSC = "lleida",
                stationId = "9771C",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LUGO",
                stateNameNoSC = "lugo",
                stationId = "1518A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MADRID",
                stateNameNoSC = "madrid",
                stationId = "3200",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MALAGA",
                stateNameNoSC = "malaga",
                stationId = "6155A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MELILLA",
                stateNameNoSC = "melilla",
                stationId = "6000A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MURCIA",
                stateNameNoSC = "murcia",
                stationId = "7031",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "NAVARRA",
                stateNameNoSC = "navarra",
                stationId = "9262",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "OURENSE",
                stateNameNoSC = "ourense",
                stationId = "1690A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PALENCIA",
                stateNameNoSC = "palencia",
                stationId = "2400E",
                startDate = "1989-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PONTEVEDRA",
                stateNameNoSC = "pontevedra",
                stationId = "1495",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SALAMANCA",
                stateNameNoSC = "salamanca",
                stationId = "2867",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEGOVIA",
                stateNameNoSC = "segovia",
                stationId = "2465",
                startDate = "1988-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEVILLA",
                stateNameNoSC = "sevilla",
                stationId = "5783",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SORIA",
                stateNameNoSC = "soria",
                stationId = "2030",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SANTA CRUZ DE TENERIFE",
                stateNameNoSC = "santa cruz de tenerife",
                stationId = "C447A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TARRAGONA",
                stateNameNoSC = "tarragona",
                stationId = "9981A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TERUEL",
                stateNameNoSC = "teruel",
                stationId = "8368U",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TOLEDO",
                stateNameNoSC = "toledo",
                stationId = "4067",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALENCIA",
                stateNameNoSC = "valencia",
                stationId = "8414A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALLADOLID",
                stateNameNoSC = "valladolid",
                stationId = "2539",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZAMORA",
                stateNameNoSC = "zamora",
                stationId = "2614",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZARAGOZA",
                stateNameNoSC = "zaragoza",
                stationId = "9390",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              )
            )
          ),
          SingleParamStudyValues(
            studyParam = StudyParamNames.precipitation,
            studyParamAbbrev = StudyParamAbbrev.precipitation,
            dataframeColName = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys.precJKey,
            reprStationRegs = List(
              RepresentativeStationRegistry(
                stateName = "A CORUÑA",
                stateNameNoSC = "a coruna",
                stationId = "1387",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALBACETE",
                stateNameNoSC = "albacete",
                stationId = "8175",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALICANTE",
                stateNameNoSC = "alicante",
                stationId = "8025",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALMERIA",
                stateNameNoSC = "almeria",
                stationId = "6325O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ARABA/ALAVA",
                stateNameNoSC = "araba",
                stationId = "9091O",
                startDate = "1977-07-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ASTURIAS",
                stateNameNoSC = "asturias",
                stationId = "1212E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "AVILA",
                stateNameNoSC = "avila",
                stationId = "2444",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BADAJOZ",
                stateNameNoSC = "badajoz",
                stationId = "4452",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BARCELONA",
                stateNameNoSC = "barcelona",
                stationId = "0200E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BIZKAIA",
                stateNameNoSC = "bizkaia",
                stationId = "1082",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BURGOS",
                stateNameNoSC = "burgos",
                stationId = "2331",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CACERES",
                stateNameNoSC = "caceres",
                stationId = "3469A",
                startDate = "1982-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CADIZ",
                stateNameNoSC = "cadiz",
                stationId = "5960",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CANTABRIA",
                stateNameNoSC = "cantabria",
                stationId = "1109",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CASTELLON",
                stateNameNoSC = "castellon",
                stationId = "8500A",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CEUTA",
                stateNameNoSC = "ceuta",
                stationId = "5000C",
                startDate = "2003-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CIUDAD REAL",
                stateNameNoSC = "cuidad real",
                stationId = "4121",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CORDOBA",
                stateNameNoSC = "cordoba",
                stationId = "5402",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CUENCA",
                stateNameNoSC = "cuenca",
                stationId = "8096",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIPUZKOA",
                stateNameNoSC = "gipuzkoa",
                stationId = "1014",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIRONA",
                stateNameNoSC = "girona",
                stationId = "0367",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GRANADA",
                stateNameNoSC = "granada",
                stationId = "5514",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GUADALAJARA",
                stateNameNoSC = "guadalajara",
                stationId = "3013",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUELVA",
                stateNameNoSC = "huelva",
                stationId = "4642E",
                startDate = "1984-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUESCA",
                stateNameNoSC = "huesca",
                stationId = "9898",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ILLES BALEARS",
                stateNameNoSC = "illes balears",
                stationId = "B278",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "JAEN",
                stateNameNoSC = "jaen",
                stationId = "5270B",
                startDate = "1983-08-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LA RIOJA",
                stateNameNoSC = "la rioja",
                stationId = "9170",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LAS PALMAS",
                stateNameNoSC = "las palmas",
                stationId = "C029O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LEON",
                stateNameNoSC = "leon",
                stationId = "2661",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LLEIDA",
                stateNameNoSC = "lleida",
                stationId = "9771C",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LUGO",
                stateNameNoSC = "lugo",
                stationId = "1658",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MADRID",
                stateNameNoSC = "madrid",
                stationId = "3200",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MALAGA",
                stateNameNoSC = "malaga",
                stationId = "6155A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MELILLA",
                stateNameNoSC = "melilla",
                stationId = "6000A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MURCIA",
                stateNameNoSC = "murcia",
                stationId = "7031",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "NAVARRA",
                stateNameNoSC = "navarra",
                stationId = "9263D",
                startDate = "1975-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "OURENSE",
                stateNameNoSC = "ourense",
                stationId = "1690A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PALENCIA",
                stateNameNoSC = "palencia",
                stationId = "2374X",
                startDate = "1988-12-03",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PONTEVEDRA",
                stateNameNoSC = "pontevedra",
                stationId = "1495",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SALAMANCA",
                stateNameNoSC = "salamanca",
                stationId = "2867",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEGOVIA",
                stateNameNoSC = "segovia",
                stationId = "2465",
                startDate = "1988-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEVILLA",
                stateNameNoSC = "sevilla",
                stationId = "5783",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SORIA",
                stateNameNoSC = "soria",
                stationId = "2030",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SANTA CRUZ DE TENERIFE",
                stateNameNoSC = "santa cruz de tenerife",
                stationId = "C447A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TARRAGONA",
                stateNameNoSC = "tarragona",
                stationId = "9981A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TERUEL",
                stateNameNoSC = "teruel",
                stationId = "8368U",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TOLEDO",
                stateNameNoSC = "toledo",
                stationId = "3260B",
                startDate = "1982-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALENCIA",
                stateNameNoSC = "valencia",
                stationId = "8414A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALLADOLID",
                stateNameNoSC = "valladolid",
                stationId = "2539",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZAMORA",
                stateNameNoSC = "zamora",
                stationId = "2614",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZARAGOZA",
                stateNameNoSC = "zaragoza",
                stationId = "9390",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              )
            )
          ),
          SingleParamStudyValues(
            studyParam = StudyParamNames.windVelocity,
            studyParamAbbrev = StudyParamAbbrev.windVelocity,
            dataframeColName = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys.velmediaJKey,
            reprStationRegs = List(
              RepresentativeStationRegistry(
                stateName = "A CORUÑA",
                stateNameNoSC = "a coruna",
                stationId = "1387",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALBACETE",
                stateNameNoSC = "albacete",
                stationId = "8175",
                startDate = "1999-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALICANTE",
                stateNameNoSC = "alicante",
                stationId = "8025",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALMERIA",
                stateNameNoSC = "almeria",
                stationId = "6325O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ARABA/ALAVA",
                stateNameNoSC = "araba",
                stationId = "9091O",
                startDate = "1977-07-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ASTURIAS",
                stateNameNoSC = "asturias",
                stationId = "1212E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "AVILA",
                stateNameNoSC = "avila",
                stationId = "2444",
                startDate = "1988-08-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BADAJOZ",
                stateNameNoSC = "badajoz",
                stationId = "4452",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BARCELONA",
                stateNameNoSC = "barcelona",
                stationId = "0076",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BIZKAIA",
                stateNameNoSC = "bizkaia",
                stationId = "1082",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BURGOS",
                stateNameNoSC = "burgos",
                stationId = "2331",
                startDate = "1986-07-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CACERES",
                stateNameNoSC = "caceres",
                stationId = "3469A",
                startDate = "1982-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CADIZ",
                stateNameNoSC = "cadiz",
                stationId = "5960",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CANTABRIA",
                stateNameNoSC = "cantabria",
                stationId = "1109",
                startDate = "1977-09-01",
                endDate = "2006-04-30"
              ),
              RepresentativeStationRegistry(
                stateName = "CASTELLON",
                stateNameNoSC = "castellon",
                stationId = "8500A",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CEUTA",
                stateNameNoSC = "ceuta",
                stationId = "5000C",
                startDate = "2003-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CIUDAD REAL",
                stateNameNoSC = "cuidad real",
                stationId = "4121",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CORDOBA",
                stateNameNoSC = "cordoba",
                stationId = "5402",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CUENCA",
                stateNameNoSC = "cuenca",
                stationId = "8096",
                startDate = "1995-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIPUZKOA",
                stateNameNoSC = "gipuzkoa",
                stationId = "1014",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIRONA",
                stateNameNoSC = "girona",
                stationId = "0367",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GRANADA",
                stateNameNoSC = "granada",
                stationId = "5530E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GUADALAJARA",
                stateNameNoSC = "guadalajara",
                stationId = "3013",
                startDate = "1973-01-01",
                endDate = "1997-01-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUELVA",
                stateNameNoSC = "huelva",
                stationId = "4642E",
                startDate = "1984-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUESCA",
                stateNameNoSC = "huesca",
                stationId = "9898",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ILLES BALEARS",
                stateNameNoSC = "illes balears",
                stationId = "B893",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "JAEN",
                stateNameNoSC = "jaen",
                stationId = "5270B",
                startDate = "1988-07-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LA RIOJA",
                stateNameNoSC = "la rioja",
                stationId = "9170",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LAS PALMAS",
                stateNameNoSC = "las palmas",
                stationId = "C029O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LEON",
                stateNameNoSC = "leon",
                stationId = "2661",
                startDate = "1973-01-01",
                endDate = "2014-04-30"
              ),
              RepresentativeStationRegistry(
                stateName = "LLEIDA",
                stateNameNoSC = "lleida",
                stationId = "9771C",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LUGO",
                stateNameNoSC = "lugo",
                stationId = "1505",
                startDate = "1985-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MADRID",
                stateNameNoSC = "madrid",
                stationId = "3129",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MALAGA",
                stateNameNoSC = "malaga",
                stationId = "6155A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MELILLA",
                stateNameNoSC = "melilla",
                stationId = "6000A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MURCIA",
                stateNameNoSC = "murcia",
                stationId = "7228",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "NAVARRA",
                stateNameNoSC = "navarra",
                stationId = "9263D",
                startDate = "1988-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "OURENSE",
                stateNameNoSC = "ourense",
                stationId = "1700X",
                startDate = "1994-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PALENCIA",
                stateNameNoSC = "palencia",
                stationId = "2400E",
                startDate = "1988-12-03",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PONTEVEDRA",
                stateNameNoSC = "pontevedra",
                stationId = "1495",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SALAMANCA",
                stateNameNoSC = "salamanca",
                stationId = "2867",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEGOVIA",
                stateNameNoSC = "segovia",
                stationId = "2465",
                startDate = "1988-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEVILLA",
                stateNameNoSC = "sevilla",
                stationId = "5783",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SORIA",
                stateNameNoSC = "soria",
                stationId = "2030",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SANTA CRUZ DE TENERIFE",
                stateNameNoSC = "santa cruz de tenerife",
                stationId = "C447A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TARRAGONA",
                stateNameNoSC = "tarragona",
                stationId = "9981A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TERUEL",
                stateNameNoSC = "teruel",
                stationId = "8368U",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TOLEDO",
                stateNameNoSC = "toledo",
                stationId = "3260B",
                startDate = "1982-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALENCIA",
                stateNameNoSC = "valencia",
                stationId = "8414A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALLADOLID",
                stateNameNoSC = "valladolid",
                stationId = "2422",
                startDate = "1973-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZAMORA",
                stateNameNoSC = "zamora",
                stationId = "2614",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZARAGOZA",
                stateNameNoSC = "zaragoza",
                stationId = "9434",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
            )
          ),
          SingleParamStudyValues(
            studyParam = StudyParamNames.pressure,
            studyParamAbbrev = StudyParamAbbrev.pressure,
            dataframeColName = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys.presmaxJKey,
            reprStationRegs = List(
              RepresentativeStationRegistry(
                stateName = "A CORUÑA",
                stateNameNoSC = "a coruna",
                stationId = "1387",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALBACETE",
                stateNameNoSC = "albacete",
                stationId = "8175",
                startDate = "1988-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALICANTE",
                stateNameNoSC = "alicante",
                stationId = "8025",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALMERIA",
                stateNameNoSC = "almeria",
                stationId = "6325O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ARABA/ALAVA",
                stateNameNoSC = "araba",
                stationId = "9091O",
                startDate = "1980-02-16",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ASTURIAS",
                stateNameNoSC = "asturias",
                stationId = "1212E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "AVILA",
                stateNameNoSC = "avila",
                stationId = "2444",
                startDate = "1983-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BADAJOZ",
                stateNameNoSC = "badajoz",
                stationId = "4452",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BARCELONA",
                stateNameNoSC = "barcelona",
                stationId = "0076",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BIZKAIA",
                stateNameNoSC = "bizkaia",
                stationId = "1082",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BURGOS",
                stateNameNoSC = "burgos",
                stationId = "2331",
                startDate = "1978-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CACERES",
                stateNameNoSC = "caceres",
                stationId = "3469A",
                startDate = "1982-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CADIZ",
                stateNameNoSC = "cadiz",
                stationId = "5960",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CANTABRIA",
                stateNameNoSC = "cantabria",
                stationId = "1109",
                startDate = "1973-01-01",
                endDate = "2024-05-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CASTELLON",
                stateNameNoSC = "castellon",
                stationId = "8500A",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CEUTA",
                stateNameNoSC = "ceuta",
                stationId = "5000C",
                startDate = "2005-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CIUDAD REAL",
                stateNameNoSC = "cuidad real",
                stationId = "4121",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CORDOBA",
                stateNameNoSC = "cordoba",
                stationId = "5402",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CUENCA",
                stateNameNoSC = "cuenca",
                stationId = "8096",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIPUZKOA",
                stateNameNoSC = "gipuzkoa",
                stationId = "1024E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIRONA",
                stateNameNoSC = "girona",
                stationId = "0367",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GRANADA",
                stateNameNoSC = "granada",
                stationId = "5530E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GUADALAJARA",
                stateNameNoSC = "guadalajara",
                stationId = "3013",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUELVA",
                stateNameNoSC = "huelva",
                stationId = "4642E",
                startDate = "1984-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUESCA",
                stateNameNoSC = "huesca",
                stationId = "9898",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ILLES BALEARS",
                stateNameNoSC = "illes balears",
                stationId = "B893",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "JAEN",
                stateNameNoSC = "jaen",
                stationId = "5270B",
                startDate = "1994-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LA RIOJA",
                stateNameNoSC = "la rioja",
                stationId = "9170",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LAS PALMAS",
                stateNameNoSC = "las palmas",
                stationId = "C029O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LEON",
                stateNameNoSC = "leon",
                stationId = "1549",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LLEIDA",
                stateNameNoSC = "lleida",
                stationId = "9771C",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LUGO",
                stateNameNoSC = "lugo",
                stationId = "1505",
                startDate = "1985-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MADRID",
                stateNameNoSC = "madrid",
                stationId = "3200",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MALAGA",
                stateNameNoSC = "malaga",
                stationId = "6155A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MELILLA",
                stateNameNoSC = "melilla",
                stationId = "6000A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MURCIA",
                stateNameNoSC = "murcia",
                stationId = "7228",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "NAVARRA",
                stateNameNoSC = "navarra",
                stationId = "9263D",
                startDate = "1975-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "OURENSE",
                stateNameNoSC = "ourense",
                stationId = "1690A",
                startDate = "1974-01-01",
                endDate = "2022-09-08"
              ),
              RepresentativeStationRegistry(
                stateName = "PALENCIA",
                stateNameNoSC = "palencia",
                stationId = "2400E",
                startDate = "1989-06-01",
                endDate = "2008-03-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PONTEVEDRA",
                stateNameNoSC = "pontevedra",
                stationId = "1495",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SALAMANCA",
                stateNameNoSC = "salamanca",
                stationId = "2867",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEGOVIA",
                stateNameNoSC = "segovia",
                stationId = "2465",
                startDate = "1988-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEVILLA",
                stateNameNoSC = "sevilla",
                stationId = "5783",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SORIA",
                stateNameNoSC = "soria",
                stationId = "2030",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SANTA CRUZ DE TENERIFE",
                stateNameNoSC = "santa cruz de tenerife",
                stationId = "C449C",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TARRAGONA",
                stateNameNoSC = "tarragona",
                stationId = "9981A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TERUEL",
                stateNameNoSC = "teruel",
                stationId = "8368U",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TOLEDO",
                stateNameNoSC = "toledo",
                stationId = "3260B",
                startDate = "1982-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALENCIA",
                stateNameNoSC = "valencia",
                stationId = "8414A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALLADOLID",
                stateNameNoSC = "valladolid",
                stationId = "2539",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZAMORA",
                stateNameNoSC = "zamora",
                stationId = "2614",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZARAGOZA",
                stateNameNoSC = "zaragoza",
                stationId = "9434",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
            )
          ),
          SingleParamStudyValues(
            studyParam = StudyParamNames.sunRadiation,
            studyParamAbbrev = StudyParamAbbrev.sunRadiation,
            dataframeColName = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys.solJKey,
            reprStationRegs = List(
              RepresentativeStationRegistry(
                stateName = "A CORUÑA",
                stateNameNoSC = "a coruna",
                stationId = "1387",
                startDate = "1974-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALBACETE",
                stateNameNoSC = "albacete",
                stationId = "8175",
                startDate = "1998-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALICANTE",
                stateNameNoSC = "alicante",
                stationId = "8025",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALMERIA",
                stateNameNoSC = "almeria",
                stationId = "6325O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ARABA/ALAVA",
                stateNameNoSC = "araba",
                stationId = "9091O",
                startDate = "1977-08-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ASTURIAS",
                stateNameNoSC = "asturias",
                stationId = "1249I",
                startDate = "1973-01-01",
                endDate = "2023-01-31"
              ),
              RepresentativeStationRegistry(
                stateName = "AVILA",
                stateNameNoSC = "avila",
                stationId = "2444",
                startDate = "1983-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BADAJOZ",
                stateNameNoSC = "badajoz",
                stationId = "4452",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BARCELONA",
                stateNameNoSC = "barcelona",
                stationId = "0076",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BIZKAIA",
                stateNameNoSC = "bizkaia",
                stationId = "1082",
                startDate = "1973-01-01",
                endDate = "2002-02-28"
              ),
              RepresentativeStationRegistry(
                stateName = "BURGOS",
                stateNameNoSC = "burgos",
                stationId = "2331",
                startDate = "1985-11-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CACERES",
                stateNameNoSC = "caceres",
                stationId = "3469A",
                startDate = "1983-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CADIZ",
                stateNameNoSC = "cadiz",
                stationId = "5960",
                startDate = "1973-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CANTABRIA",
                stateNameNoSC = "cantabria",
                stationId = "1109",
                startDate = "1977-09-01",
                endDate = "2007-07-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CASTELLON",
                stateNameNoSC = "castellon",
                stationId = "8500A",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CEUTA",
                stateNameNoSC = "ceuta",
                stationId = "5000A",
                startDate = "1973-01-01",
                endDate = "1986-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CIUDAD REAL",
                stateNameNoSC = "cuidad real",
                stationId = "4121",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CORDOBA",
                stateNameNoSC = "cordoba",
                stationId = "5402",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CUENCA",
                stateNameNoSC = "cuenca",
                stationId = "8096",
                startDate = "1973-01-01",
                endDate = "2012-03-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIPUZKOA",
                stateNameNoSC = "gipuzkoa",
                stationId = "1014",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIRONA",
                stateNameNoSC = "girona",
                stationId = "0367",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GRANADA",
                stateNameNoSC = "granada",
                stationId = "5530E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GUADALAJARA",
                stateNameNoSC = "guadalajara",
                stationId = "3013",
                startDate = "1973-01-01",
                endDate = "2007-03-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUELVA",
                stateNameNoSC = "huelva",
                stationId = "4642E",
                startDate = "1984-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUESCA",
                stateNameNoSC = "huesca",
                stationId = "9898",
                startDate = "1973-01-01",
                endDate = "1998-03-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ILLES BALEARS",
                stateNameNoSC = "illes balears",
                stationId = "B278",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "JAEN",
                stateNameNoSC = "jaen",
                stationId = "5270B",
                startDate = "1994-06-01",
                endDate = "2009-11-30"
              ),
              RepresentativeStationRegistry(
                stateName = "LA RIOJA",
                stateNameNoSC = "la rioja",
                stationId = "9170",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LAS PALMAS",
                stateNameNoSC = "las palmas",
                stationId = "C029O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LEON",
                stateNameNoSC = "leon",
                stationId = "2661",
                startDate = "1973-01-01",
                endDate = "2014-04-30"
              ),
              RepresentativeStationRegistry(
                stateName = "LLEIDA",
                stateNameNoSC = "lleida",
                stationId = "9771C",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LUGO",
                stateNameNoSC = "lugo",
                stationId = "1505",
                startDate = "1990-08-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MADRID",
                stateNameNoSC = "madrid",
                stationId = "3200",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MALAGA",
                stateNameNoSC = "malaga",
                stationId = "6155A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MELILLA",
                stateNameNoSC = "melilla",
                stationId = "6000A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MURCIA",
                stateNameNoSC = "murcia",
                stationId = "7228",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "NAVARRA",
                stateNameNoSC = "navarra",
                stationId = "9263D",
                startDate = "1975-02-01",
                endDate = "2016-03-31"
              ),
              RepresentativeStationRegistry(
                stateName = "OURENSE",
                stateNameNoSC = "ourense",
                stationId = "1690A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PALENCIA",
                stateNameNoSC = "palencia",
                stationId = "2235U",
                startDate = "2012-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PONTEVEDRA",
                stateNameNoSC = "pontevedra",
                stationId = "1484C",
                startDate = "1985-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SALAMANCA",
                stateNameNoSC = "salamanca",
                stationId = "2867",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEGOVIA",
                stateNameNoSC = "segovia",
                stationId = "2465",
                startDate = "1988-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEVILLA",
                stateNameNoSC = "sevilla",
                stationId = "5783",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SORIA",
                stateNameNoSC = "soria",
                stationId = "2030",
                startDate = "1973-01-01",
                endDate = "2010-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SANTA CRUZ DE TENERIFE",
                stateNameNoSC = "santa cruz de tenerife",
                stationId = "C447A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TARRAGONA",
                stateNameNoSC = "tarragona",
                stationId = "9981A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TERUEL",
                stateNameNoSC = "teruel",
                stationId = "8368U",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TOLEDO",
                stateNameNoSC = "toledo",
                stationId = "3260B",
                startDate = "1982-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALENCIA",
                stateNameNoSC = "valencia",
                stationId = "8414A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALLADOLID",
                stateNameNoSC = "valladolid",
                stationId = "2422",
                startDate = "1973-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZAMORA",
                stateNameNoSC = "zamora",
                stationId = "2614",
                startDate = "1973-01-01",
                endDate = "2011-06-30"
              ),
              RepresentativeStationRegistry(
                stateName = "ZARAGOZA",
                stateNameNoSC = "zaragoza",
                stationId = "9390",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              )
            )
          ),
          SingleParamStudyValues(
            studyParam = StudyParamNames.relativeHumidity,
            studyParamAbbrev = StudyParamAbbrev.relativeHumidity,
            dataframeColName = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys.hrmediaJKey,
            reprStationRegs = List(
              RepresentativeStationRegistry(
                stateName = "A CORUÑA",
                stateNameNoSC = "a coruna",
                stationId = "1387",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALBACETE",
                stateNameNoSC = "albacete",
                stationId = "8175",
                startDate = "1998-09-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALICANTE",
                stateNameNoSC = "alicante",
                stationId = "8025",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ALMERIA",
                stateNameNoSC = "almeria",
                stationId = "6325O",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ARABA/ALAVA",
                stateNameNoSC = "araba",
                stationId = "9091O",
                startDate = "1977-07-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ASTURIAS",
                stateNameNoSC = "asturias",
                stationId = "1212E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "AVILA",
                stateNameNoSC = "avila",
                stationId = "2444",
                startDate = "1983-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BADAJOZ",
                stateNameNoSC = "badajoz",
                stationId = "4452",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BARCELONA",
                stateNameNoSC = "barcelona",
                stationId = "0200E",
                startDate = "1983-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BIZKAIA",
                stateNameNoSC = "bizkaia",
                stationId = "1082",
                startDate = "1985-09-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "BURGOS",
                stateNameNoSC = "burgos",
                stationId = "2331",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CACERES",
                stateNameNoSC = "caceres",
                stationId = "3469A",
                startDate = "1982-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CADIZ",
                stateNameNoSC = "cadiz",
                stationId = "5960",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CANTABRIA",
                stateNameNoSC = "cantabria",
                stationId = "1109",
                startDate = "1977-09-01",
                endDate = "2024-05-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CASTELLON",
                stateNameNoSC = "castellon",
                stationId = "8500A",
                startDate = "1976-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CEUTA",
                stateNameNoSC = "ceuta",
                stationId = "5000C",
                startDate = "2005-12-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CIUDAD REAL",
                stateNameNoSC = "cuidad real",
                stationId = "4121",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CORDOBA",
                stateNameNoSC = "cordoba",
                stationId = "5402",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "CUENCA",
                stateNameNoSC = "cuenca",
                stationId = "8096",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIPUZKOA",
                stateNameNoSC = "gipuzkoa",
                stationId = "1014",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GIRONA",
                stateNameNoSC = "girona",
                stationId = "0367",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GRANADA",
                stateNameNoSC = "granada",
                stationId = "5530E",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "GUADALAJARA",
                stateNameNoSC = "guadalajara",
                stationId = "3013",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUELVA",
                stateNameNoSC = "huelva",
                stationId = "4642E",
                startDate = "1984-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "HUESCA",
                stateNameNoSC = "huesca",
                stationId = "9898",
                startDate = "1980-09-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ILLES BALEARS",
                stateNameNoSC = "illes balears",
                stationId = "B893",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "JAEN",
                stateNameNoSC = "jaen",
                stationId = "5270B",
                startDate = "1994-06-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LA RIOJA",
                stateNameNoSC = "la rioja",
                stationId = "9170",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LAS PALMAS",
                stateNameNoSC = "las palmas",
                stationId = "C649I",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LEON",
                stateNameNoSC = "leon",
                stationId = "1549",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LLEIDA",
                stateNameNoSC = "lleida",
                stationId = "9771C",
                startDate = "1983-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "LUGO",
                stateNameNoSC = "lugo",
                stationId = "1505",
                startDate = "1985-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MADRID",
                stateNameNoSC = "madrid",
                stationId = "3200",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MALAGA",
                stateNameNoSC = "malaga",
                stationId = "6155A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MELILLA",
                stateNameNoSC = "melilla",
                stationId = "6000A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "MURCIA",
                stateNameNoSC = "murcia",
                stationId = "7228",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "NAVARRA",
                stateNameNoSC = "navarra",
                stationId = "9263D",
                startDate = "1975-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "OURENSE",
                stateNameNoSC = "ourense",
                stationId = "1690A",
                startDate = "1983-05-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PALENCIA",
                stateNameNoSC = "palencia",
                stationId = "2400E",
                startDate = "1989-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "PONTEVEDRA",
                stateNameNoSC = "pontevedra",
                stationId = "1495",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SALAMANCA",
                stateNameNoSC = "salamanca",
                stationId = "2867",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEGOVIA",
                stateNameNoSC = "segovia",
                stationId = "2465",
                startDate = "1988-10-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SEVILLA",
                stateNameNoSC = "sevilla",
                stationId = "5783",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SORIA",
                stateNameNoSC = "soria",
                stationId = "2030",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "SANTA CRUZ DE TENERIFE",
                stateNameNoSC = "santa cruz de tenerife",
                stationId = "C447A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TARRAGONA",
                stateNameNoSC = "tarragona",
                stationId = "9981A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TERUEL",
                stateNameNoSC = "teruel",
                stationId = "8368U",
                startDate = "1986-04-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "TOLEDO",
                stateNameNoSC = "toledo",
                stationId = "3260B",
                startDate = "1982-02-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALENCIA",
                stateNameNoSC = "valencia",
                stationId = "8414A",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "VALLADOLID",
                stateNameNoSC = "valladolid",
                stationId = "2539",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZAMORA",
                stateNameNoSC = "zamora",
                stationId = "2614",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              ),
              RepresentativeStationRegistry(
                stateName = "ZARAGOZA",
                stateNameNoSC = "zaragoza",
                stationId = "9434",
                startDate = "1973-01-01",
                endDate = "2024-12-31"
              )
            )
          ),
        )

        object Execution {
          object Top10Highest2024 {
            val startDate: String = "2024"
          }

          object Top10HighestDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10Lowest2024 {
            val startDate: String = "2024"
          }

          object Top10LowestDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestInc {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestInc {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Avg2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object InterestingStudies {
        case class RepresentativeStationRegistry(stateName: String, stateNameNoSC: String, stationId: String, startDate: String, endDate: String)
        val stationRegistries: List[RepresentativeStationRegistry] = List(
          RepresentativeStationRegistry(
            stateName = "A CORUÑA",
            stateNameNoSC = "a coruna",
            stationId = "1387",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ALBACETE",
            stateNameNoSC = "albacete",
            stationId = "8175",
            startDate = "1988-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ALICANTE",
            stateNameNoSC = "alicante",
            stationId = "8025",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ALMERIA",
            stateNameNoSC = "almeria",
            stationId = "6325O",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ARABA/ALAVA",
            stateNameNoSC = "araba",
            stationId = "9091O",
            startDate = "1980-02-16",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ASTURIAS",
            stateNameNoSC = "asturias",
            stationId = "1212E",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "AVILA",
            stateNameNoSC = "avila",
            stationId = "2444",
            startDate = "1983-02-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "BADAJOZ",
            stateNameNoSC = "badajoz",
            stationId = "4452",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "BARCELONA",
            stateNameNoSC = "barcelona",
            stationId = "0076",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "BIZKAIA",
            stateNameNoSC = "bizkaia",
            stationId = "1082",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "BURGOS",
            stateNameNoSC = "burgos",
            stationId = "2331",
            startDate = "1978-05-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CACERES",
            stateNameNoSC = "caceres",
            stationId = "3469A",
            startDate = "1982-12-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CADIZ",
            stateNameNoSC = "cadiz",
            stationId = "5960",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CANTABRIA",
            stateNameNoSC = "cantabria",
            stationId = "1109",
            startDate = "1973-01-01",
            endDate = "2024-05-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CASTELLON",
            stateNameNoSC = "castellon",
            stationId = "8500A",
            startDate = "1976-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CEUTA",
            stateNameNoSC = "ceuta",
            stationId = "5000C",
            startDate = "2005-12-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CIUDAD REAL",
            stateNameNoSC = "cuidad real",
            stationId = "4121",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CORDOBA",
            stateNameNoSC = "cordoba",
            stationId = "5402",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "CUENCA",
            stateNameNoSC = "cuenca",
            stationId = "8096",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "GIPUZKOA",
            stateNameNoSC = "gipuzkoa",
            stationId = "1024E",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "GIRONA",
            stateNameNoSC = "girona",
            stationId = "0367",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "GRANADA",
            stateNameNoSC = "granada",
            stationId = "5530E",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "GUADALAJARA",
            stateNameNoSC = "guadalajara",
            stationId = "3013",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "HUELVA",
            stateNameNoSC = "huelva",
            stationId = "4642E",
            startDate = "1984-06-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "HUESCA",
            stateNameNoSC = "huesca",
            stationId = "9898",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ILLES BALEARS",
            stateNameNoSC = "illes balears",
            stationId = "B893",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "JAEN",
            stateNameNoSC = "jaen",
            stationId = "5270B",
            startDate = "1994-06-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "LA RIOJA",
            stateNameNoSC = "la rioja",
            stationId = "9170",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "LAS PALMAS",
            stateNameNoSC = "las palmas",
            stationId = "C029O",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "LEON",
            stateNameNoSC = "leon",
            stationId = "1549",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "LLEIDA",
            stateNameNoSC = "lleida",
            stationId = "9771C",
            startDate = "1983-02-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "LUGO",
            stateNameNoSC = "lugo",
            stationId = "1505",
            startDate = "1985-05-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "MADRID",
            stateNameNoSC = "madrid",
            stationId = "3200",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "MALAGA",
            stateNameNoSC = "malaga",
            stationId = "6155A",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "MELILLA",
            stateNameNoSC = "melilla",
            stationId = "6000A",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "MURCIA",
            stateNameNoSC = "murcia",
            stationId = "7228",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "NAVARRA",
            stateNameNoSC = "navarra",
            stationId = "9263D",
            startDate = "1975-02-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "OURENSE",
            stateNameNoSC = "ourense",
            stationId = "1690A",
            startDate = "1974-01-01",
            endDate = "2022-09-08"
          ),
          RepresentativeStationRegistry(
            stateName = "PALENCIA",
            stateNameNoSC = "palencia",
            stationId = "2400E",
            startDate = "1989-06-01",
            endDate = "2008-03-31"
          ),
          RepresentativeStationRegistry(
            stateName = "PONTEVEDRA",
            stateNameNoSC = "pontevedra",
            stationId = "1495",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "SALAMANCA",
            stateNameNoSC = "salamanca",
            stationId = "2867",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "SEGOVIA",
            stateNameNoSC = "segovia",
            stationId = "2465",
            startDate = "1988-10-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "SEVILLA",
            stateNameNoSC = "sevilla",
            stationId = "5783",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "SORIA",
            stateNameNoSC = "soria",
            stationId = "2030",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "SANTA CRUZ DE TENERIFE",
            stateNameNoSC = "santa cruz de tenerife",
            stationId = "C449C",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "TARRAGONA",
            stateNameNoSC = "tarragona",
            stationId = "9981A",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "TERUEL",
            stateNameNoSC = "teruel",
            stationId = "8368U",
            startDate = "1986-04-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "TOLEDO",
            stateNameNoSC = "toledo",
            stationId = "3260B",
            startDate = "1982-02-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "VALENCIA",
            stateNameNoSC = "valencia",
            stationId = "8414A",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "VALLADOLID",
            stateNameNoSC = "valladolid",
            stationId = "2539",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ZAMORA",
            stateNameNoSC = "zamora",
            stationId = "2614",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          ),
          RepresentativeStationRegistry(
            stateName = "ZARAGOZA",
            stateNameNoSC = "zaragoza",
            stationId = "9434",
            startDate = "1973-01-01",
            endDate = "2024-12-31"
          )
        )
        object Execution {
          object PrecAndPressEvolFromStartForEachState {
            val climateParams: List[(String, String)] = List(
              ("prec", "prec"),
              ("presmax", "press")
            )
          }
          object Top10BetterWindPower {
            val climateParams: List[(String, Double, Double)] = List(
              ("velmedia", 6, 9),
              ("hrmedia", 0, 70),
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10BetterSunPower {
            val climateParams: List[(String, Double, Double)] = List(
              ("sol", 6, Double.PositiveInfinity),
              ("prec", 0, 1.5)
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10TorrentialRains {
            val climateParams: List[(String, Double, Double)] = List(
              ("prec", 100, Double.PositiveInfinity),
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10Storms {
            val climateParams: List[(String, Double, Double)] = List(
              ("tmax", 20, Double.PositiveInfinity),
              ("hrmax", 60, 100),
              ("presmin", Float.NegativeInfinity, 1010),
              ("prec", 1, Float.PositiveInfinity)
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10Agriculture {
            val climateParams: List[(String, Double, Double)] = List(
              ("tmed", 15, 25),
              ("prec", 5, 10)
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10Droughts {
            val climateParams: List[(String, Double, Double)] = List(
              ("tmax", 28, Double.PositiveInfinity),
              ("prec", 0, 0),
              ("sol", 10, Double.PositiveInfinity)
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10Fires {
            val climateParams: List[(String, Double, Double)] = List(
              ("tmax", 30, Double.PositiveInfinity),
              ("hrmedia", 0, 30),
              ("velmedia", 5.5, Double.PositiveInfinity),
              ("prec", 0, 0),
              ("sol", 10, Double.PositiveInfinity)
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10HeatWaves {
            val climateParams: List[(String, Double, Double)] = List(
              ("tmax", 35, Double.PositiveInfinity),
              ("hrmedia", 0, 40),
              ("prec", 0, 0),
              ("presmax", 1015, Double.PositiveInfinity),
              ("sol", 10, Double.PositiveInfinity),
              ("velmedia", Double.NegativeInfinity, 3)
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
          object Top10Frosts {
            val climateParams: List[(String, Double, Double)] = List(
              ("tmax", Double.NegativeInfinity, 0),
            )
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }
        }
      }
      object Global {

      }
    }
  }
}
