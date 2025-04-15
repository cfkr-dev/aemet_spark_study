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

      object Temperature {
        object Top10Temp {
          object Dirs {
            val resultHighest2024: String = Temperature.Global.Dirs.base + "top_10_temp/highest/2024/"
            val resultHighestDecade: String = Temperature.Global.Dirs.base + "top_10_temp/highest/decade/"
            val resultHighestGlobal: String = Temperature.Global.Dirs.base + "top_10_temp/highest/global/"
            val resultLowest2024: String = Temperature.Global.Dirs.base + "top_10_temp/lowest/2024/"
            val resultLowestDecade: String = Temperature.Global.Dirs.base + "top_10_temp/lowest/decade/"
            val resultLowestGlobal: String = Temperature.Global.Dirs.base + "top_10_temp/lowest/global/"
          }
        }

        object TempEvolFromStartForEachState {
          object Dirs {
            val resultStation: String = Temperature.Global.Dirs.base + "temp_evol/%s/station/"
            val resultEvol: String = Temperature.Global.Dirs.base + "temp_evol/%s/evolution/"
          }
        }

        object Top5TempInc {
          object Dirs {
            val resultHighest: String = Temperature.Global.Dirs.base + "top_5_temp_inc/highest/"
            val resultLowest: String = Temperature.Global.Dirs.base + "top_5_temp_inc/lowest/"
          }
        }

        object AvgTemp2024AllStationSpain {
          object Dirs {
            val resultContinental: String = Temperature.Global.Dirs.base + "avg_temp_2024_spain/continental/"
            val resultCanary: String = Temperature.Global.Dirs.base + "avg_temp_2024_spain/canary_islands/"
          }
        }

        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "temperature/"
          }
        }
      }

      object Precipitation {
        object Top10Prec {
          object Dirs {
            val resultHighest2024: String = Precipitation.Global.Dirs.base + "top_10_prec/highest/2024/"
            val resultHighestDecade: String = Precipitation.Global.Dirs.base + "top_10_prec/highest/decade/"
            val resultHighestGlobal: String = Precipitation.Global.Dirs.base + "top_10_prec/highest/global/"
            val resultLowest2024: String = Precipitation.Global.Dirs.base + "top_10_prec/lowest/2024/"
            val resultLowestDecade: String = Precipitation.Global.Dirs.base + "top_10_prec/lowest/decade/"
            val resultLowestGlobal: String = Precipitation.Global.Dirs.base + "top_10_prec/lowest/global/"
          }
        }

        object PrecEvolFromStartForEachState {
          object Dirs {
            val resultStation: String = Precipitation.Global.Dirs.base + "prec_evol/%s/station/"
            val resultEvol: String = Precipitation.Global.Dirs.base + "prec_evol/%s/evolution/"
          }
        }

        object Top5PrecInc {
          object Dirs {
            val resultHighest: String = Precipitation.Global.Dirs.base + "top_5_prec_inc/highest/"
            val resultLowest: String = Precipitation.Global.Dirs.base + "top_5_prec_inc/lowest/"
          }
        }

        object AvgPrec2024AllStationSpain {
          object Dirs {
            val resultContinental: String = Precipitation.Global.Dirs.base + "avg_prec_2024_spain/continental/"
            val resultCanary: String = Precipitation.Global.Dirs.base + "avg_prec_2024_spain/canary_islands/"
          }
        }

        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "precipitation/"
          }
        }
      }

      object WindVelocity {
        object Top10WindVelocity {
          object Dirs {
            val resultHighest2024: String = WindVelocity.Global.Dirs.base + "top_10_wind_velocity/highest/2024/"
            val resultHighestDecade: String = WindVelocity.Global.Dirs.base + "top_10_wind_velocity/highest/decade/"
            val resultHighestGlobal: String = WindVelocity.Global.Dirs.base + "top_10_wind_velocity/highest/global/"
            val resultLowest2024: String = WindVelocity.Global.Dirs.base + "top_10_wind_velocity/lowest/2024/"
            val resultLowestDecade: String = WindVelocity.Global.Dirs.base + "top_10_wind_velocity/lowest/decade/"
            val resultLowestGlobal: String = WindVelocity.Global.Dirs.base + "top_10_wind_velocity/lowest/global/"
          }
        }

        object WindVelocityEvolFromStartForEachState {
          object Dirs {
            val resultStation: String = WindVelocity.Global.Dirs.base + "wind_velocity_evol/%s/station/"
            val resultEvol: String = WindVelocity.Global.Dirs.base + "wind_velocity_evol/%s/evolution/"
          }
        }

        object Top5WindVelocityInc {
          object Dirs {
            val resultHighest: String = WindVelocity.Global.Dirs.base + "top_5_wind_velocity_inc/highest/"
            val resultLowest: String = WindVelocity.Global.Dirs.base + "top_5_wind_velocity_inc/lowest/"
          }
        }

        object AvgWindVelocity2024AllStationSpain {
          object Dirs {
            val resultContinental: String = WindVelocity.Global.Dirs.base + "avg_wind_velocity_2024_spain/continental/"
            val resultCanary: String = WindVelocity.Global.Dirs.base + "avg_wind_velocity_2024_spain/canary_islands/"
          }
        }

        object Global {
          object Dirs {
            val base: String = DataSpark.Global.Dirs.base + "wind_velocity/"
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
            val fetchingClimate: String = "%s climate information"
            val startFetchingClimate: String = "Fetching %s climate information"
            val endFetchingClimate: String = "Completed fetching %s climate information"
            val infoShowDataframeTempAndPrecInfo = "Showing a part of monthly average temperature and precipitation sum of %s registers"
            val infoSaveDataframeTempAndPrecInfo = "Saving %s registers of monthly average temperature and precipitation sum (%s)"
            val infoShowDataframeStationInfo = "Showing a part of station information of %s registers"
            val infoSaveDataframeStationInfo = "Saving %s registers of station information (%s)"
            val fetchingClimateLocationStation = "%s station information"
            val fetchingClimateLocationTempPrec = "%s temperature and precipitation information"
          }
        }

        object Temperature {
          val studyName: String = "temperature"

          object Execution {
            val top10HighestTemp2024: String = "Top 10 places with the highest temperatures in 2024"
            val top10HighestTempDecade: String = "Top 10 places with the highest temperatures in the last decade"
            val top10HighestTempGlobal: String = "Top 10 places with the highest temperatures from the start of registers"
            val top10LowestTemp2024: String = "Top 10 places with the lowest temperatures in 2024"
            val top10LowestTempDecade: String = "Top 10 places with the lowest temperatures in the last decade"
            val top10LowestTempGlobal: String = "Top 10 places with the lowest temperatures from the start of registers"
            val tempEvolFromStartForEachState: String = "Temperature evolution from the start of registries for each state"
            val tempEvolFromStartForEachStateStartStation: String = "Fetching data from representative %s state station"
            val tempEvolFromStartForEachStateStartEvol: String = "Fetching data from %s state temperature evolution"
            val top5HighestIncTemp: String = "Top 5 places with the highest increment of temperatures from the start of registers"
            val top5LowestIncTemp: String = "Top 5 places with the lowest increment of temperatures from the start of registers"
            val avgTemp2024AllStationSpainContinental: String = "Average temperature in 2024 for all station in the spanish continental territory"
            val avgTemp2024AllStationSpainCanary: String = "Average temperature in 2024 for all station in Canary islands"
          }
        }

        object Precipitation {
          val studyName: String = "precipitation"

          object Execution {
            val top10HighestPrec2024: String = "Top 10 places with the highest precipitations in 2024"
            val top10HighestPrecDecade: String = "Top 10 places with the highest precipitations in the last decade"
            val top10HighestPrecGlobal: String = "Top 10 places with the highest precipitations from the start of registers"
            val top10LowestPrec2024: String = "Top 10 places with the lowest precipitations in 2024"
            val top10LowestPrecDecade: String = "Top 10 places with the lowest precipitations in the last decade"
            val top10LowestPrecGlobal: String = "Top 10 places with the lowest precipitations from the start of registers"
            val precEvolFromStartForEachState: String = "Precipitation evolution from the start of registries for each state"
            val precEvolFromStartForEachStateStartStation: String = "Fetching data from representative %s state station"
            val precEvolFromStartForEachStateEndStation: String = "Completed fetching data from representative %s state station"
            val precEvolFromStartForEachStateStartEvol: String = "Fetching data from %s state precipitation evolution"
            val precEvolFromStartForEachStateEndEvol: String = "Completed fetching data from %s state precipitation evolution"
            val top5HighestIncPrec: String = "Top 5 places with the highest increment of precipitations from the start of registers"
            val top5LowestIncPrec: String = "Top 5 places with the lowest increment of precipitations from the start of registers"
            val avgPrec2024AllStationSpainContinental: String = "Average precipitation in 2024 for all station in the spanish continental territory"
            val avgPrec2024AllStationSpainCanary: String = "Average precipitation in 2024 for all station in Canary islands"
          }
        }

        object WindVelocity {
          val studyName: String = "wind velocity"

          object Execution {
            val top10HighestWindVelocity2024: String = "Top 10 places with the highest wind velocity in 2024"
            val top10HighestWindVelocityDecade: String = "Top 10 places with the highest wind velocity in the last decade"
            val top10HighestWindVelocityGlobal: String = "Top 10 places with the highest wind velocity from the start of registers"
            val top10LowestWindVelocity2024: String = "Top 10 places with the lowest wind velocity in 2024"
            val top10LowestWindVelocityDecade: String = "Top 10 places with the lowest wind velocity in the last decade"
            val top10LowestWindVelocityGlobal: String = "Top 10 places with the lowest wind velocity from the start of registers"
            val windVelocityEvolFromStartForEachState: String = "Wind Velocity evolution from the start of registries for each state"
            val windVelocityEvolFromStartForEachStateStartStation: String = "Fetching data from representative %s state station"
            val windVelocityEvolFromStartForEachStateEndStation: String = "Completed fetching data from representative %s state station"
            val windVelocityEvolFromStartForEachStateStartEvol: String = "Fetching data from %s state wind velocity evolution"
            val windVelocityEvolFromStartForEachStateEndEvol: String = "Completed fetching data from %s state wind velocity evolution"
            val top5HighestIncWindVelocity: String = "Top 5 places with the highest increment of wind velocity from the start of registers"
            val top5LowestIncWindVelocity: String = "Top 5 places with the lowest increment of wind velocity from the start of registers"
            val avgWindVelocity2024AllStationSpainContinental: String = "Average wind velocity in 2024 for all station in the spanish continental territory"
            val avgWindVelocity2024AllStationSpainCanary: String = "Average wind velocity in 2024 for all station in Canary islands"
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
      object Temperature {
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

        object Execution {
          object Top10HighestTemp2024 {
            val startDate: String = "2024"
          }

          object Top10HighestTempDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestTempGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestTemp2024 {
            val startDate: String = "2024"
          }

          object Top10LowestTempDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestTempGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestIncTemp {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestIncTemp {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object AvgTemp2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object Precipitation {
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
        object Execution {
          object Top10HighestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10HighestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10LowestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object AvgPrec2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object WindVelocity {
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
        object Execution {
          object Top10HighestWindVelocity2024 {
            val startDate: String = "2024"
          }

          object Top10HighestWindVelocityDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestWindVelocityGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestWindVelocity2024 {
            val startDate: String = "2024"
          }

          object Top10LowestWindVelocityDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestWindVelocityGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestIncWindVelocity {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestIncWindVelocity {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object AvgWindVelocity2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object Pressure {
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
        object Execution {
          object Top10HighestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10HighestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10LowestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object AvgPrec2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object SunRadiation {
        case class RepresentativeStationRegistry(stateName: String, stateNameNoSC: String, stationId: String, startDate: String, endDate: String)
        val stationRegistries: List[RepresentativeStationRegistry] = List(
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
        object Execution {
          object Top10HighestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10HighestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10LowestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object AvgPrec2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object RelativeHumidity {
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
        object Execution {
          object Top10HighestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10HighestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10HighestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrec2024 {
            val startDate: String = "2024"
          }

          object Top10LowestPrecDecade {
            val startDate: String = "2014-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top10LowestPrecGlobal {
            val startDate: String = "1973-01-01"
            val endDate: String = "2024-12-31"
          }

          object Top5HighestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object Top5LowestIncPrec {
            val startYear: Int = 1973
            val endYear: Int = 2024
          }

          object AvgPrec2024AllStationSpain {
            val startDate: String = "2024"
            val canaryIslandStates: List[String] = List(
              "SANTA CRUZ DE TENERIFE",
              "LAS PALMAS"
            )
          }
        }
      }
      object Global {

      }
    }
  }
}
