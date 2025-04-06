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
              val horahrmaxJKey: String = "horaharmax"
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
      object Climograph {
        object Arid {
          object BWh {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BWh.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Canary {
              object Dirs {
                val stationResult: String = Canary.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Canary.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BWh.Global.Dirs.base + "canary_islands/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Arid.Global.Dirs.base + "BWh/"
              }
            }
          }
          object BWk {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BWk.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Arid.Global.Dirs.base + "BWk/"
              }
            }
          }
          object BSh {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BSh.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Canary {
              object Dirs {
                val stationResult: String = Canary.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Canary.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BSh.Global.Dirs.base + "canary_islands/"
                }
              }
            }
            object Balear {
              object Dirs {
                val stationResult: String = Balear.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Balear.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BSh.Global.Dirs.base + "balear_islands/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Arid.Global.Dirs.base + "BSh/"
              }
            }
          }
          object BSk {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BSk.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Canary {
              object Dirs {
                val stationResult: String = Canary.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Canary.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BSk.Global.Dirs.base + "canary_islands/"
                }
              }
            }
            object Balear {
              object Dirs {
                val stationResult: String = Balear.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Balear.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = BSk.Global.Dirs.base + "balear_islands/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Arid.Global.Dirs.base + "BSk/"
              }
            }
          }
          object Global {
            object Dirs {
              val base: String = Climograph.Global.Dirs.base + "arid_climates/"
            }
          }
        }
        object Warm {
          object Csa {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Csa.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Canary {
              object Dirs {
                val stationResult: String = Canary.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Canary.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Csa.Global.Dirs.base + "canary_islands/"
                }
              }
            }
            object Balear {
              object Dirs {
                val stationResult: String = Balear.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Balear.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Csa.Global.Dirs.base + "balear_islands/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Warm.Global.Dirs.base + "Csa/"
              }
            }
          }
          object Csb {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Csb.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Canary {
              object Dirs {
                val stationResult: String = Canary.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Canary.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Csb.Global.Dirs.base + "canary_islands/"
                }
              }
            }
            object Balear {
              object Dirs {
                val stationResult: String = Balear.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Balear.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Csb.Global.Dirs.base + "balear_islands/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Warm.Global.Dirs.base + "Csb/"
              }
            }
          }
          object Cfa {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Cfa.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Warm.Global.Dirs.base + "Cfa/"
              }
            }
          }
          object Cfb {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Cfb.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Warm.Global.Dirs.base + "Cfb/"
              }
            }
          }
          object Global {
            object Dirs {
              val base: String = Climograph.Global.Dirs.base + "warm_climates/"
            }
          }
        }
        object Cold {
          object Dsb {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Dsb.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Cold.Global.Dirs.base + "Dsb/"
              }
            }
          }
          object Dfb {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Dfb.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Cold.Global.Dirs.base + "Dfb/"
              }
            }
          }
          object Dfc {
            object Peninsula {
              object Dirs {
                val stationResult: String = Peninsula.Global.Dirs.base + "station/"
                val tempAndPrecResult: String = Peninsula.Global.Dirs.base + "temp_and_prec/"
              }
              object Global {
                object Dirs {
                  val base: String = Dfc.Global.Dirs.base + "peninsula/"
                }
              }
            }
            object Global {
              object Dirs {
                val base: String = Cold.Global.Dirs.base + "Dfc/"
              }
            }
          }
          object Global {
            object Dirs {
              val base: String = Climograph.Global.Dirs.base + "cold_climates/"
            }
          }
        }
        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "climograph/"
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
        object Climograph {
          val studyName: String = "climograph"
          object Execution {
            val startFetchingClimateGroup: String = "Fetching %s climates information"
            val endFetchingClimateGroup: String = "Completed fetching %s climates information"
            val startFetchingClimate: String = "Fetching %s climate information"
            val endFetchingClimate: String = "Completed fetching %s climate information"
            val infoShowDataframeTempAndPrecInfo = "Showing a part of monthly average temperature and precipitation sum of %s registers"
            val infoSaveDataframeTempAndPrecInfo = "Saving %s registers of monthly average temperature and precipitation sum (%s)"
            val infoShowDataframeStationInfo = "Showing a part of station information of %s registers"
            val infoSaveDataframeStationInfo = "Saving %s registers of station information (%s)"
          }
        }
        object Global {
          val startStudy: String = "Starting %s study"
          val endStudy: String = "Completed %s study"
        }
      }
      object Methods {
        object GetStationInfoById {
          val startFetching: String = "Fetching station (ID: %s) information"
          val endFetching: String = "Completed fetching station (ID: %s) information"
        }

        object GetStationMonthlyAvgTempAndPrecInAYear {
          val startFetching: String = "Fetching station (ID: %s) monthly average temperature and precipitation sum in %s"
          val endFetching: String = "Completed fetching station (ID: %s) monthly average temperature and precipitation sum in %s"
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
      object Climograph {
        val observationYear = 2024
        object Locations {
          val peninsula: String = "peninsular land"
          val canaryIslands: String = "canary islands"
          val balearIslands: String = "balear islands"
        }
        object Climates {
          object Arid {
            object BWh {
              val peninsula: String = "7002Y"
              val canaryIslands: String = "C249I"
            }
            object BWk {
              val peninsula: String = "4"
            }
            object BSh {
              val peninsula: String = "7012C"
              val canaryIslands: String = "C459Z"
              val balearIslands: String = "B954"
            }
            object BSk {
              val peninsula: String = "9434"
              val canaryIslands: String = "C426I"
              val balearIslands: String = "B228"
            }
          }
          object Warm {
            object Csa {
              val peninsula: String = "5783"
              val canaryIslands: String = "C426E"
              val balearIslands: String = "B051A"
            }
            object Csb {
              val peninsula: String = "1518A"
              val canaryIslands: String = "C611E"
              val balearIslands: String = "B684A"
            }
            object Cfa {
              val peninsula: String = "9901X"
            }
            object Cfb {
              val peninsula: String = "1082"
            }
          }
          object Cold {
            object Dsb {
              val peninsula: String = "2462"
            }
            object Dfb {
              val peninsula: String = "9814I"
            }
            object Dfc {
              val peninsula: String = "9839V"
            }
          }
        }
      }
      object Global {
        object Climates {
          val ClimateGroupNames: List[String] = List(
            Arid.climateGroupName,
            Warm.climateGroupName,
            Cold.climateGroupName
          )
          object Arid {
            val climateGroupName: String = "arid"
            val climateNames: List[String] = List(
              Climates.BWh.climateName,
              Climates.BWk.climateName,
              Climates.BSh.climateName,
              Climates.BSk.climateName
            )
            object Climates {
              object BWh {
                val climateName: String = "BWh"
              }
              object BWk {
                val climateName: String = "BWk"
              }
              object BSh {
                val climateName: String = "BSh"
              }
              object BSk {
                val climateName: String = "BSk"
              }
            }
          }
          object Warm {
            val climateGroupName: String = "warm"
            val climateNames: List[String] = List(
              Climates.Csa.climateName,
              Climates.Csb.climateName,
              Climates.Cfa.climateName,
              Climates.Cfb.climateName
            )
            object Climates {
              object Csa {
                val climateName: String = "Csa"
              }
              object Csb {
                val climateName: String = "Csb"
              }
              object Cfa {
                val climateName: String = "Cfa"
              }
              object Cfb {
                val climateName: String = "Cfb"
              }
            }
          }
          object Cold {
            val climateGroupName: String = "cold"
            val climateNames: List[String] = List(
              Climates.Dsb.climateName,
              Climates.Dfb.climateName,
              Climates.Dfc.climateName
            )
            object Climates {
              object Dsb {
                val climateName: String = "Dsb"
              }
              object Dfb {
                val climateName: String = "Dfb"
              }
              object Dfc {
                val climateName: String = "Dfb"
              }
            }
          }
        }
        object Methods {
          object GetStationInfoById {
            val id: String = "getStationInfoById"
          }

          object GetStationMonthlyAvgTempAndPrecInAYear {
            val id: String = "getStationMonthlyAvgTempAndPrecInAYear"
          }
        }
      }
    }
  }
}
