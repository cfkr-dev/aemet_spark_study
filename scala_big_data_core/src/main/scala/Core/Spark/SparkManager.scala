package Core.Spark

import Utils.JSONUtils
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import Config.ConstantsV2._
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object SparkManager {
  private val ctsStorageDataAemetAllStationInfo = Storage.DataAemet.AllStationInfo
  private val ctsStorageDataAemetAllMeteoInfo = Storage.DataAemet.AllMeteoInfo
  private val ctsStorageDataIfapaAemetFormatSingleStationInfo = Storage.DataIfapaAemetFormat.SingleStationInfo
  private val ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo = Storage.DataIfapaAemetFormat.SingleStationMeteoInfo

  private object SparkCore {
    val sparkSession: SparkSession = createSparkSession("AEMET Spark Study", "local[*]", "ERROR")

    // TODO antes de realizar cualquier query mostrar info de spark.
    private def createSparkSession(name: String, master: String, logLevel: String): SparkSession = {
      val spark = SparkSession.builder()
        .appName(name)
        .master(master)
        .getOrCreate()

      spark.sparkContext
        .setLogLevel(logLevel)

      spark.catalog.clearCache()

      spark
    }

    private def createDataframeFromJSONAndAemetMetadata(
      session: SparkSession,
      sourcePath: String,
      metadataPath: String
    ): Either[Exception, DataFrame] = {
      def createDataframeSchemaAemet(metadataJSON: ujson.Value): StructType = {
        val constantsAemetAPIGlobal = Config.Constants.AemetAPI.Global
        StructType(
          metadataJSON(constantsAemetAPIGlobal.metadataFields).arr.map(field => {
            StructField(
              field(constantsAemetAPIGlobal.metadataFieldsID).str,
              StringType,
              !field(constantsAemetAPIGlobal.metadataFieldsRequired).bool
            )
          }).toArray
        )
      }

      Right(session
        .read.format("json")
        .option("multiline", value = true)
        .schema(
          createDataframeSchemaAemet(
            JSONUtils.readJSON(
              metadataPath
            ) match {
              case Right(json) => json
              case Left(exception) => return Left(exception)
            }
          )
        )
        .json(sourcePath))
    }

    def saveDataframeAsParquet(dataframe: DataFrame, path: String): Either[Exception, String] = {
      try {
        dataframe.write
          .mode("overwrite")
          .parquet(path)

        Right(path)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    object dataframes {
      private def formatAllMeteoInfoDataframe(dataframe: DataFrame): DataFrame = {
        val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys
        val ctsRemoteReqAemetParamsAllMeteoInfoExecFormat = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Execution.Format

        val formatters: Map[String, String => Column] = Map(
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.fechaJKey ->
            (column => to_date(col(column), ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.provinciaJKey ->
            (column => udf((value: String) => Map(
              "STA. CRUZ DE TENERIFE" -> "SANTA CRUZ DE TENERIFE",
              "BALEARES" -> "ILLES BALEARS",
              "ALMERÍA" -> "ALMERIA"
            ).getOrElse(value, value)).apply(col(column))),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.altitudJKey ->
            (column => col(column).cast("int")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey ->
            (column => {
              when(col(column) === "Acum", lit(null).cast("double"))
                .otherwise(
                  when(col(column) === "Ip", lit(0.0).cast("double"))
                    .otherwise(bround(regexp_replace(col(column), ",", ".").cast("double"), 1))
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tminJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatminJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmaxJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatmaxJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.dirJKey ->
            (column => {
              when(col(column) === "99" || col(column) === "88", lit(null).cast("int"))
                .otherwise(regexp_replace(col(column), ",", "").cast("int"))
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.rachaJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horarachaJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horapresmaxJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presminJKey ->
            (column => bround(regexp_replace(col(column), ",", ".").cast("double"), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horapresminJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey ->
            (column => col(column).cast("int")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmaxJKey ->
            (column => col(column).cast("int")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrmaxJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrminJKey ->
            (column => col(column).cast("int")),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrminJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast("timestamp"))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
        )

        formatters.foldLeft(dataframe) {
          case (accumulatedDf, (colName, transformationFunc)) =>
            accumulatedDf.withColumn(colName, transformationFunc(colName))
        }
      }

      private def formatAllStationsDataframe(dataframe: DataFrame): DataFrame = {
        val ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllStationInfo.Metadata.DataFieldsJSONKeys

        val formatters: Map[String, String => Column] = Map(
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.provinciaJKey ->
            (column => udf((value: String) => Map(
              "SANTA CRUZ DE TENERIFE" -> "STA. CRUZ DE TENERIFE",
              "BALEARES" -> "ILLES BALEARS",
              "ALMERÍA" -> "ALMERIA"
            ).getOrElse(value, value)).apply(col(column))),
          ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.altitudJKey ->
            (column => col(column).cast("int")),
          "latitud_dec" ->
            (_ => round(udf((dms: String) => {
              val degrees = dms.substring(0, 2).toInt
              val minutes = dms.substring(2, 4).toInt
              val seconds = dms.substring(4, 6).toInt
              val direction = dms.last
              val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
              if (direction == 'S' || direction == 'W') -decimal else decimal
            }).apply(col(ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.latitudJKey)), 6)),
          "longitud_dec" ->
            (_ => round(udf((dms: String) => {
              val degrees = dms.substring(0, 2).toInt
              val minutes = dms.substring(2, 4).toInt
              val seconds = dms.substring(4, 6).toInt
              val direction = dms.last
              val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
              if (direction == 'S' || direction == 'W') -decimal else decimal
            }).apply(col(ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.longitudJKey)), 6)),
        )

        formatters.foldLeft(dataframe) {
          case (accumulatedDf, (colName, transformationFunc)) =>
            accumulatedDf.withColumn(colName, transformationFunc(colName))
        }
      }

      val allMeteoInfo: DataFrame =
        formatAllMeteoInfoDataframe(
          createDataframeFromJSONAndAemetMetadata(
            sparkSession,
            ctsStorageDataAemetAllMeteoInfo.Dirs.dataRegistry,
            ctsStorageDataAemetAllMeteoInfo.FilePaths.metadata
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df.union(createDataframeFromJSONAndAemetMetadata(
              sparkSession,
              ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.Dirs.dataRegistry,
              ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.FilePaths.metadata
            ) match {
              case Left(exception) => throw exception
              case Right(df) => df
            })
          }
        ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val allStations: DataFrame =
        formatAllStationsDataframe(
          createDataframeFromJSONAndAemetMetadata(
            sparkSession,
            ctsStorageDataAemetAllStationInfo.Dirs.dataRegistry,
            ctsStorageDataAemetAllStationInfo.FilePaths.metadata
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df.union(createDataframeFromJSONAndAemetMetadata(
              sparkSession,
              ctsStorageDataIfapaAemetFormatSingleStationInfo.Dirs.dataRegistry,
              ctsStorageDataIfapaAemetFormatSingleStationInfo.FilePaths.metadata
            ) match {
              case Left(exception) => throw exception
              case Right(df) => df
            })
          }
        ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }

  object SparkQueries {

    import SparkCore.sparkSession.implicits._

    private val ctsSparkQueriesGlobal = Spark.Queries.Global
    private val ctsLogsSparkQueriesStudiesGlobal = Logs.SparkQueries.Studies.Global

    private case class FetchAndSaveInfo(
      dataframe: DataFrame,
      pathToSave: String,
      title: String = "",
      showInfoMessage: String = ctsLogsSparkQueriesStudiesGlobal.showInfo,
      saveInfoMessage: String = ctsLogsSparkQueriesStudiesGlobal.saveInfo
    )

    private def simpleFetchAndSave(
      queryTitle: String = "",
      queries: Seq[FetchAndSaveInfo],
      encloseHalfLengthStart: Int = 35
    ): Unit = {
      if (queryTitle != "")
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startQuery.format(
          queryTitle
        ), encloseHalfLength = encloseHalfLengthStart)

      queries.foreach(subQuery => {
        if (subQuery.title != "")
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startSubQuery.format(
            subQuery.title
          ), encloseHalfLength = encloseHalfLengthStart + 5)

        printlnConsoleMessage(NotificationType.Information, subQuery.showInfoMessage)
        subQuery.dataframe.show()

        printlnConsoleMessage(NotificationType.Information, subQuery.saveInfoMessage.format(
          subQuery.pathToSave
        ))
        SparkCore.saveDataframeAsParquet(subQuery.dataframe, subQuery.pathToSave) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }

        if (subQuery.title != "")
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endSubQuery.format(
            subQuery.title
          ), encloseHalfLength = encloseHalfLengthStart + 5)
      })

      if (queryTitle != "")
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endQuery.format(
          queryTitle
        ), encloseHalfLength = encloseHalfLengthStart)
    }

    object Climograph {
      private val ctsSparkQueriesClimograph = Spark.Queries.Climograph
      private val ctsLogsSparkQueriesStudiesClimograph = Logs.SparkQueries.Studies.Climograph
      private val ctsStorageDataSparkClimograph = Storage.DataSpark.Climograph

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesClimograph.studyName
        ))

        ctsSparkQueriesClimograph.stationsRegistries.foreach(climateGroup => {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.startFetchingClimateGroup.format(
            climateGroup.climateGroupName
          ), encloseHalfLength = 35)

          climateGroup.climates.foreach(climateRegistry => {
            simpleFetchAndSave(
              ctsLogsSparkQueriesStudiesClimograph.Execution.fetchingClimate.format(
                climateRegistry.climateName
              ),
              climateRegistry.registries.flatMap(registry => {
                List(
                  FetchAndSaveInfo(
                    getStationInfoById(registry.stationId) match {
                      case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                        return
                      case Right(dataFrame: DataFrame) => dataFrame
                    },
                    ctsStorageDataSparkClimograph.Dirs.resultStation.format(
                      climateGroup.climateGroupName,
                      climateRegistry.climateName,
                      registry.location.toString.replace(" ", "_")
                    ),
                    ctsLogsSparkQueriesStudiesClimograph.Execution.fetchingClimateLocationStation.format(
                      registry.location.toString.capitalize,
                    )
                  ),
                  FetchAndSaveInfo(
                    getStationMonthlyAvgTempAndSumPrecInAYear(
                      registry.stationId,
                      ctsSparkQueriesClimograph.observationYear
                    ) match {
                      case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                        return
                      case Right(dataFrame: DataFrame) => dataFrame
                    },
                    ctsStorageDataSparkClimograph.Dirs.resultTempPrec.format(
                      climateGroup.climateGroupName,
                      climateRegistry.climateName,
                      registry.location.toString.replace(" ", "_")
                    ),
                    ctsLogsSparkQueriesStudiesClimograph.Execution.fetchingClimateLocationTempPrec.format(
                      registry.location.toString.capitalize,
                    )
                  )
                )
              }),
              encloseHalfLengthStart = 40
            )
          })

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.startFetchingClimateGroup.format(
            climateGroup.climateGroupName
          ), encloseHalfLength = 35)
        })

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesClimograph.studyName
        ))
      }
    }

    object Temperature {
      private val ctsSparkQueriesTemperature = Spark.Queries.Temperature
      private val ctsLogsSparkQueriesStudiesTemperature = Logs.SparkQueries.Studies.Temperature
      private val ctsStorageDataSparkTemperature = Storage.DataSpark.Temperature
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesTemperature.studyName
        ))

        // Top 10 places with the highest temperatures in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top10HighestTemp2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.Top10HighestTemp2024.startDate,
                endDate = None) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top10Temp.Dirs.resultHighest2024
            )
          )
        )

        // Top 10 places with the highest temperatures in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top10HighestTempDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.Top10HighestTempDecade.startDate,
                endDate = Some(ctsSparkQueriesTemperature.Execution.Top10HighestTempDecade.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top10Temp.Dirs.resultHighestDecade
            )
          )
        )

        // Top 10 places with the highest temperatures from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top10HighestTempGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.Top10HighestTempGlobal.startDate,
                endDate = Some(ctsSparkQueriesTemperature.Execution.Top10HighestTempGlobal.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top10Temp.Dirs.resultHighestGlobal
            )
          )
        )

        // Top 10 places with the lowest temperatures in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top10LowestTemp2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.Top10LowestTemp2024.startDate,
                endDate = None,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top10Temp.Dirs.resultLowest2024
            )
          )
        )

        // Top 10 places with the lowest temperatures in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top10LowestTempDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.Top10LowestTempDecade.startDate,
                endDate = Some(ctsSparkQueriesTemperature.Execution.Top10LowestTempDecade.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top10Temp.Dirs.resultLowestDecade
            )
          )
        )

        // Top 10 places with the lowest temperatures from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top10LowestTempGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.Top10LowestTempGlobal.startDate,
                endDate = Some(ctsSparkQueriesTemperature.Execution.Top10LowestTempGlobal.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top10Temp.Dirs.resultLowestGlobal
            )
          )
        )

        // Temperature evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.tempEvolFromStartForEachState,
          ctsSparkQueriesTemperature.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkTemperature.TempEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesTemperature.Execution.tempEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkTemperature.TempEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesTemperature.Execution.tempEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 5 highest increment of temperature
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top5HighestIncTemp,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesTemperature.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startYear = ctsSparkQueriesTemperature.Execution.Top5HighestIncTemp.startYear,
                endYear = ctsSparkQueriesTemperature.Execution.Top5HighestIncTemp.endYear
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top5TempInc.Dirs.resultHighest
            )
          )
        )

        // Top 5 lowest increment of temperature
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.top5LowestIncTemp,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesTemperature.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startYear = ctsSparkQueriesTemperature.Execution.Top5LowestIncTemp.startYear,
                endYear = ctsSparkQueriesTemperature.Execution.Top5LowestIncTemp.endYear,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.Top5TempInc.Dirs.resultLowest
            )
          )
        )

        // Get average temperature in 2024 for all station in the spanish continental territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.avgTemp2024AllStationSpainContinental,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.AvgTemp2024AllStationSpain.startDate
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.AvgTemp2024AllStationSpain.Dirs.resultContinental
            )
          )
        )

        // Get average temperature in 2024 for all station in the canary islands territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesTemperature.Execution.avgTemp2024AllStationSpainCanary,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey,
                startDate = ctsSparkQueriesTemperature.Execution.AvgTemp2024AllStationSpain.startDate,
                states = Some(ctsSparkQueriesTemperature.Execution.AvgTemp2024AllStationSpain.canaryIslandStates),
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkTemperature.AvgTemp2024AllStationSpain.Dirs.resultCanary
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesTemperature.studyName
        ))
      }
    }

    object Precipitation {
      private val ctsSparkQueriesPrecipitation = Spark.Queries.Precipitation
      private val ctsLogsSparkQueriesStudiesPrecipitation = Logs.SparkQueries.Studies.Precipitation
      private val ctsStorageDataSparkPrecipitation = Storage.DataSpark.Precipitation
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesPrecipitation.studyName
        ))

        // Top 10 places with the highest precipitations in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top10HighestPrec2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.Top10HighestPrec2024.startDate,
                endDate = None) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top10Prec.Dirs.resultHighest2024
            )
          )
        )

        // Top 10 places with the highest precipitations in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top10HighestPrecDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.Top10HighestPrecDecade.startDate,
                endDate = Some(ctsSparkQueriesPrecipitation.Execution.Top10HighestPrecDecade.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top10Prec.Dirs.resultHighestDecade
            )
          )
        )

        // Top 10 places with the highest precipitations from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top10HighestPrecGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.Top10HighestPrecGlobal.startDate,
                endDate = Some(ctsSparkQueriesPrecipitation.Execution.Top10HighestPrecGlobal.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top10Prec.Dirs.resultHighestGlobal
            )
          )
        )

        // Top 10 places with the lowest precipitations in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top10LowestPrec2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.Top10LowestPrec2024.startDate,
                endDate = None,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top10Prec.Dirs.resultLowest2024
            )
          )
        )

        // Top 10 places with the lowest precipitations in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top10LowestPrecDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.Top10LowestPrecDecade.startDate,
                endDate = Some(ctsSparkQueriesPrecipitation.Execution.Top10LowestPrecDecade.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top10Prec.Dirs.resultLowestDecade
            )
          )
        )

        // Top 10 places with the lowest precipitations from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top10LowestPrecGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.Top10LowestPrecGlobal.startDate,
                endDate = Some(ctsSparkQueriesPrecipitation.Execution.Top10LowestPrecGlobal.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top10Prec.Dirs.resultLowestGlobal
            )
          )
        )

        // Precipitation evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.precEvolFromStartForEachState,
          ctsSparkQueriesPrecipitation.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkPrecipitation.PrecEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesPrecipitation.Execution.precEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkPrecipitation.PrecEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesPrecipitation.Execution.precEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 5 highest increment of precipitation
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top5HighestIncPrec,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesPrecipitation.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startYear = ctsSparkQueriesPrecipitation.Execution.Top5HighestIncPrec.startYear,
                endYear = ctsSparkQueriesPrecipitation.Execution.Top5HighestIncPrec.endYear
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top5PrecInc.Dirs.resultHighest
            )
          )
        )

        // Top 5 lowest increment of precipitation
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.top5LowestIncPrec,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesPrecipitation.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startYear = ctsSparkQueriesPrecipitation.Execution.Top5LowestIncPrec.startYear,
                endYear = ctsSparkQueriesPrecipitation.Execution.Top5LowestIncPrec.endYear,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.Top5PrecInc.Dirs.resultLowest
            )
          )
        )

        // Get average precipitation in 2024 for all station in the spanish continental territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.avgPrec2024AllStationSpainContinental,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.AvgPrec2024AllStationSpain.startDate
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.AvgPrec2024AllStationSpain.Dirs.resultContinental
            )
          )
        )

        // Get average precipitation in 2024 for all station in the canary islands territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPrecipitation.Execution.avgPrec2024AllStationSpainCanary,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                startDate = ctsSparkQueriesPrecipitation.Execution.AvgPrec2024AllStationSpain.startDate,
                states = Some(ctsSparkQueriesPrecipitation.Execution.AvgPrec2024AllStationSpain.canaryIslandStates),
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPrecipitation.AvgPrec2024AllStationSpain.Dirs.resultCanary
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesPrecipitation.studyName
        ))
      }
    }

    object WindVelocity {
      private val ctsSparkQueriesWindVelocity = Spark.Queries.WindVelocity
      private val ctsLogsSparkQueriesStudiesWindVelocity = Logs.SparkQueries.Studies.WindVelocity
      private val ctsStorageDataSparkWindVelocity = Storage.DataSpark.WindVelocity
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesWindVelocity.studyName
        ))

        // Top 10 places with the highest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top10HighestWindVelocity2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.Top10HighestWindVelocity2024.startDate,
                endDate = None) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top10WindVelocity.Dirs.resultHighest2024
            )
          )
        )

        // Top 10 places with the highest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top10HighestWindVelocityDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.Top10HighestWindVelocityDecade.startDate,
                endDate = Some(ctsSparkQueriesWindVelocity.Execution.Top10HighestWindVelocityDecade.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top10WindVelocity.Dirs.resultHighestDecade
            )
          )
        )

        // Top 10 places with the highest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top10HighestWindVelocityGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.Top10HighestWindVelocityGlobal.startDate,
                endDate = Some(ctsSparkQueriesWindVelocity.Execution.Top10HighestWindVelocityGlobal.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top10WindVelocity.Dirs.resultHighestGlobal
            )
          )
        )

        // Top 10 places with the lowest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top10LowestWindVelocity2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.Top10LowestWindVelocity2024.startDate,
                endDate = None,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top10WindVelocity.Dirs.resultLowest2024
            )
          )
        )

        // Top 10 places with the lowest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top10LowestWindVelocityDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.Top10LowestWindVelocityDecade.startDate,
                endDate = Some(ctsSparkQueriesWindVelocity.Execution.Top10LowestWindVelocityDecade.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top10WindVelocity.Dirs.resultLowestDecade
            )
          )
        )

        // Top 10 places with the lowest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top10LowestWindVelocityGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.Top10LowestWindVelocityGlobal.startDate,
                endDate = Some(ctsSparkQueriesWindVelocity.Execution.Top10LowestWindVelocityGlobal.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top10WindVelocity.Dirs.resultLowestGlobal
            )
          )
        )

        // Wind velocity evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.windVelocityEvolFromStartForEachState,
          ctsSparkQueriesWindVelocity.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkWindVelocity.WindVelocityEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesWindVelocity.Execution.windVelocityEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkWindVelocity.WindVelocityEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesWindVelocity.Execution.windVelocityEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 5 highest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top5HighestIncWindVelocity,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesWindVelocity.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startYear = ctsSparkQueriesWindVelocity.Execution.Top5HighestIncWindVelocity.startYear,
                endYear = ctsSparkQueriesWindVelocity.Execution.Top5HighestIncWindVelocity.endYear
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top5WindVelocityInc.Dirs.resultHighest
            )
          )
        )

        // Top 5 lowest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.top5LowestIncWindVelocity,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesWindVelocity.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startYear = ctsSparkQueriesWindVelocity.Execution.Top5LowestIncWindVelocity.startYear,
                endYear = ctsSparkQueriesWindVelocity.Execution.Top5LowestIncWindVelocity.endYear,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.Top5WindVelocityInc.Dirs.resultLowest
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the spanish continental territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.avgWindVelocity2024AllStationSpainContinental,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.AvgWindVelocity2024AllStationSpain.startDate
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.AvgWindVelocity2024AllStationSpain.Dirs.resultContinental
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the canary islands territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesWindVelocity.Execution.avgWindVelocity2024AllStationSpainCanary,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey,
                startDate = ctsSparkQueriesWindVelocity.Execution.AvgWindVelocity2024AllStationSpain.startDate,
                states = Some(ctsSparkQueriesWindVelocity.Execution.AvgWindVelocity2024AllStationSpain.canaryIslandStates),
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkWindVelocity.AvgWindVelocity2024AllStationSpain.Dirs.resultCanary
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesWindVelocity.studyName
        ))
      }
    }

    object Pressure {
      private val ctsSparkQueriesPressure = Spark.Queries.Pressure
      private val ctsLogsSparkQueriesStudiesPressure = Logs.SparkQueries.Studies.Pressure
      private val ctsStorageDataSparkPressure = Storage.DataSpark.Pressure
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesPressure.studyName
        ))

        // Top 10 places with the highest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top10HighestPressure2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.Top10HighestPressure2024.startDate,
                endDate = None) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top10Pressure.Dirs.resultHighest2024
            )
          )
        )

        // Top 10 places with the highest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top10HighestPressureDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.Top10HighestPressureDecade.startDate,
                endDate = Some(ctsSparkQueriesPressure.Execution.Top10HighestPressureDecade.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top10Pressure.Dirs.resultHighestDecade
            )
          )
        )

        // Top 10 places with the highest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top10HighestPressureGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.Top10HighestPressureGlobal.startDate,
                endDate = Some(ctsSparkQueriesPressure.Execution.Top10HighestPressureGlobal.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top10Pressure.Dirs.resultHighestGlobal
            )
          )
        )

        // Top 10 places with the lowest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top10LowestPressure2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.Top10LowestPressure2024.startDate,
                endDate = None,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top10Pressure.Dirs.resultLowest2024
            )
          )
        )

        // Top 10 places with the lowest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top10LowestPressureDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.Top10LowestPressureDecade.startDate,
                endDate = Some(ctsSparkQueriesPressure.Execution.Top10LowestPressureDecade.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top10Pressure.Dirs.resultLowestDecade
            )
          )
        )

        // Top 10 places with the lowest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top10LowestPressureGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.Top10LowestPressureGlobal.startDate,
                endDate = Some(ctsSparkQueriesPressure.Execution.Top10LowestPressureGlobal.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top10Pressure.Dirs.resultLowestGlobal
            )
          )
        )

        // Wind velocity evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.pressureEvolFromStartForEachState,
          ctsSparkQueriesPressure.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkPressure.PressureEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesPressure.Execution.pressureEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkPressure.PressureEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesPressure.Execution.pressureEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 5 highest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top5HighestIncPressure,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesPressure.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startYear = ctsSparkQueriesPressure.Execution.Top5HighestIncPressure.startYear,
                endYear = ctsSparkQueriesPressure.Execution.Top5HighestIncPressure.endYear
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top5PressureInc.Dirs.resultHighest
            )
          )
        )

        // Top 5 lowest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.top5LowestIncPressure,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesPressure.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startYear = ctsSparkQueriesPressure.Execution.Top5LowestIncPressure.startYear,
                endYear = ctsSparkQueriesPressure.Execution.Top5LowestIncPressure.endYear,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.Top5PressureInc.Dirs.resultLowest
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the spanish continental territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.avgPressure2024AllStationSpainContinental,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.AvgPressure2024AllStationSpain.startDate
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.AvgPressure2024AllStationSpain.Dirs.resultContinental
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the canary islands territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesPressure.Execution.avgPressure2024AllStationSpainCanary,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                startDate = ctsSparkQueriesPressure.Execution.AvgPressure2024AllStationSpain.startDate,
                states = Some(ctsSparkQueriesPressure.Execution.AvgPressure2024AllStationSpain.canaryIslandStates),
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkPressure.AvgPressure2024AllStationSpain.Dirs.resultCanary
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesPressure.studyName
        ))
      }
    }

    object SunRadiation {
      private val ctsSparkQueriesSunRadiation = Spark.Queries.SunRadiation
      private val ctsLogsSparkQueriesStudiesSunRadiation = Logs.SparkQueries.Studies.SunRadiation
      private val ctsStorageDataSparkSunRadiation = Storage.DataSpark.SunRadiation
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesSunRadiation.studyName
        ))

        // Top 10 places with the highest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top10HighestSunRadiation2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.Top10HighestSunRadiation2024.startDate,
                endDate = None) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top10SunRadiation.Dirs.resultHighest2024
            )
          )
        )

        // Top 10 places with the highest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top10HighestSunRadiationDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.Top10HighestSunRadiationDecade.startDate,
                endDate = Some(ctsSparkQueriesSunRadiation.Execution.Top10HighestSunRadiationDecade.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top10SunRadiation.Dirs.resultHighestDecade
            )
          )
        )

        // Top 10 places with the highest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top10HighestSunRadiationGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.Top10HighestSunRadiationGlobal.startDate,
                endDate = Some(ctsSparkQueriesSunRadiation.Execution.Top10HighestSunRadiationGlobal.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top10SunRadiation.Dirs.resultHighestGlobal
            )
          )
        )

        // Top 10 places with the lowest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top10LowestSunRadiation2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.Top10LowestSunRadiation2024.startDate,
                endDate = None,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top10SunRadiation.Dirs.resultLowest2024
            )
          )
        )

        // Top 10 places with the lowest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top10LowestSunRadiationDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.Top10LowestSunRadiationDecade.startDate,
                endDate = Some(ctsSparkQueriesSunRadiation.Execution.Top10LowestSunRadiationDecade.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top10SunRadiation.Dirs.resultLowestDecade
            )
          )
        )

        // Top 10 places with the lowest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top10LowestSunRadiationGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.Top10LowestSunRadiationGlobal.startDate,
                endDate = Some(ctsSparkQueriesSunRadiation.Execution.Top10LowestSunRadiationGlobal.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top10SunRadiation.Dirs.resultLowestGlobal
            )
          )
        )

        // Wind velocity evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.sunRadiationEvolFromStartForEachState,
          ctsSparkQueriesSunRadiation.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSunRadiation.SunRadiationEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesSunRadiation.Execution.sunRadiationEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSunRadiation.SunRadiationEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesSunRadiation.Execution.sunRadiationEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 5 highest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top5HighestIncSunRadiation,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesSunRadiation.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startYear = ctsSparkQueriesSunRadiation.Execution.Top5HighestIncSunRadiation.startYear,
                endYear = ctsSparkQueriesSunRadiation.Execution.Top5HighestIncSunRadiation.endYear
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top5SunRadiationInc.Dirs.resultHighest
            )
          )
        )

        // Top 5 lowest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.top5LowestIncSunRadiation,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesSunRadiation.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startYear = ctsSparkQueriesSunRadiation.Execution.Top5LowestIncSunRadiation.startYear,
                endYear = ctsSparkQueriesSunRadiation.Execution.Top5LowestIncSunRadiation.endYear,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.Top5SunRadiationInc.Dirs.resultLowest
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the spanish continental territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.avgSunRadiation2024AllStationSpainContinental,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.AvgSunRadiation2024AllStationSpain.startDate
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.AvgSunRadiation2024AllStationSpain.Dirs.resultContinental
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the canary islands territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesSunRadiation.Execution.avgSunRadiation2024AllStationSpainCanary,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey,
                startDate = ctsSparkQueriesSunRadiation.Execution.AvgSunRadiation2024AllStationSpain.startDate,
                states = Some(ctsSparkQueriesSunRadiation.Execution.AvgSunRadiation2024AllStationSpain.canaryIslandStates),
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkSunRadiation.AvgSunRadiation2024AllStationSpain.Dirs.resultCanary
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesSunRadiation.studyName
        ))
      }
    }

    object RelativeHumidity {
      private val ctsSparkQueriesRelativeHumidity = Spark.Queries.RelativeHumidity
      private val ctsLogsSparkQueriesStudiesRelativeHumidity = Logs.SparkQueries.Studies.RelativeHumidity
      private val ctsStorageDataSparkRelativeHumidity = Storage.DataSpark.RelativeHumidity
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesRelativeHumidity.studyName
        ))

        // Top 10 places with the highest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top10HighestRelativeHumidity2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.Top10HighestRelativeHumidity2024.startDate,
                endDate = None) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top10RelativeHumidity.Dirs.resultHighest2024
            )
          )
        )

        // Top 10 places with the highest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top10HighestRelativeHumidityDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.Top10HighestRelativeHumidityDecade.startDate,
                endDate = Some(ctsSparkQueriesRelativeHumidity.Execution.Top10HighestRelativeHumidityDecade.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top10RelativeHumidity.Dirs.resultHighestDecade
            )
          )
        )

        // Top 10 places with the highest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top10HighestRelativeHumidityGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.Top10HighestRelativeHumidityGlobal.startDate,
                endDate = Some(ctsSparkQueriesRelativeHumidity.Execution.Top10HighestRelativeHumidityGlobal.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top10RelativeHumidity.Dirs.resultHighestGlobal
            )
          )
        )

        // Top 10 places with the lowest wind velocity in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top10LowestRelativeHumidity2024,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.Top10LowestRelativeHumidity2024.startDate,
                endDate = None,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top10RelativeHumidity.Dirs.resultLowest2024
            )
          )
        )

        // Top 10 places with the lowest wind velocity in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top10LowestRelativeHumidityDecade,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.Top10LowestRelativeHumidityDecade.startDate,
                endDate = Some(ctsSparkQueriesRelativeHumidity.Execution.Top10LowestRelativeHumidityDecade.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top10RelativeHumidity.Dirs.resultLowestDecade
            )
          )
        )

        // Top 10 places with the lowest wind velocity from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top10LowestRelativeHumidityGlobal,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.Top10LowestRelativeHumidityGlobal.startDate,
                endDate = Some(ctsSparkQueriesRelativeHumidity.Execution.Top10LowestRelativeHumidityGlobal.endDate),
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top10RelativeHumidity.Dirs.resultLowestGlobal
            )
          )
        )

        // Wind velocity evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.relativeHumidityEvolFromStartForEachState,
          ctsSparkQueriesRelativeHumidity.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkRelativeHumidity.RelativeHumidityEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.relativeHumidityEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkRelativeHumidity.RelativeHumidityEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.relativeHumidityEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 5 highest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top5HighestIncRelativeHumidity,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesRelativeHumidity.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startYear = ctsSparkQueriesRelativeHumidity.Execution.Top5HighestIncRelativeHumidity.startYear,
                endYear = ctsSparkQueriesRelativeHumidity.Execution.Top5HighestIncRelativeHumidity.endYear
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top5RelativeHumidityInc.Dirs.resultHighest
            )
          )
        )

        // Top 5 lowest increment of wind velocity
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.top5LowestIncRelativeHumidity,
          List(
            FetchAndSaveInfo(
              getTopNClimateParamIncrementInAYearLapse(
                stationIds = ctsSparkQueriesRelativeHumidity.stationRegistries.map(registry => registry.stationId),
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startYear = ctsSparkQueriesRelativeHumidity.Execution.Top5LowestIncRelativeHumidity.startYear,
                endYear = ctsSparkQueriesRelativeHumidity.Execution.Top5LowestIncRelativeHumidity.endYear,
                highest = false
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.Top5RelativeHumidityInc.Dirs.resultLowest
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the spanish continental territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.avgRelativeHumidity2024AllStationSpainContinental,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.AvgRelativeHumidity2024AllStationSpain.startDate
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.AvgRelativeHumidity2024AllStationSpain.Dirs.resultContinental
            )
          )
        )

        // Get average wind velocity in 2024 for all station in the canary islands territory
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesRelativeHumidity.Execution.avgRelativeHumidity2024AllStationSpainCanary,
          List(
            FetchAndSaveInfo(
              getAllStationsByStatesAvgClimateParamInALapse(
                climateParam = ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey,
                startDate = ctsSparkQueriesRelativeHumidity.Execution.AvgRelativeHumidity2024AllStationSpain.startDate,
                states = Some(ctsSparkQueriesRelativeHumidity.Execution.AvgRelativeHumidity2024AllStationSpain.canaryIslandStates),
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkRelativeHumidity.AvgRelativeHumidity2024AllStationSpain.Dirs.resultCanary
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesRelativeHumidity.studyName
        ))
      }
    }

    object InterestingStudies  {
      private val ctsSparkQueriesInterestingStudies = Spark.Queries.InterestingStudies
      private val ctsLogsSparkQueriesStudiesInterestingStudies = Logs.SparkQueries.Studies.InterestingStudies
      private val ctsStorageDataSparkInterestingStudies = Storage.DataSpark.InterestingStudies
      private val ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllMeteoInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesInterestingStudies.studyName
        ))

        // Precipitation and pressure evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.precAndPressureEvolFromStartForEachState,
          ctsSparkQueriesInterestingStudies.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkInterestingStudies.PrecAndPressionEvolFromStartForEachState.Dirs.resultStation.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesInterestingStudies.Execution.precAndPressureEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                )
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  List(
                    ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey,
                    ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey,
                  ),
                  registry.startDate,
                  Some(registry.endDate)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkInterestingStudies.PrecAndPressionEvolFromStartForEachState.Dirs.resultEvol.format(
                  registry.stateNameNoSC.replace(" ", "_")
                ),
                ctsLogsSparkQueriesStudiesInterestingStudies.Execution.precAndPressureEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              )
            )
          })
        )

        // Top 10 better places for wind power generation in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10BetterWindPower,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10BetterWindPower.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10BetterWindPower.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10BetterWindPower.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultBetterWindPower
            )
          )
        )

        // Top 10 better places for sun power generation in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10BetterSunPower,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10BetterSunPower.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10BetterSunPower.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10BetterSunPower.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultBetterSunPower
            )
          )
        )

        // Top 10 places with the highest incidence of torrential rains in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10TorrentialRains,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10TorrentialRains.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10TorrentialRains.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10TorrentialRains.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultTorrentialRains
            )
          )
        )

        // Top 10 the highest incidence of storms in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10Storms,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10Storms.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10Storms.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10Storms.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultStorms
            )
          )
        )

        // Top 10 better places for agriculture in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10Agriculture,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10Agriculture.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10Agriculture.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10Agriculture.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultAgriculture
            )
          )
        )

        // Top 10 the highest incidence of droughts in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10Droughts,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10Droughts.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10Droughts.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10Droughts.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultDroughts
            )
          )
        )

        // Top 10 the highest incidence of fires in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10Fires,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10Fires.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10Fires.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10Fires.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultFires
            )
          )
        )

        // Top 10 the highest incidence of heat waves in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10HeatWaves,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10HeatWaves.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10HeatWaves.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10HeatWaves.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultHeatWaves
            )
          )
        )

        // Top 10 the highest incidence of frosts in the last decade
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesInterestingStudies.Execution.top10Frosts,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsSparkQueriesInterestingStudies.Execution.Top10Frosts.climateParams,
                startDate = ctsSparkQueriesInterestingStudies.Execution.Top10Frosts.startDate,
                endDate = Some(ctsSparkQueriesInterestingStudies.Execution.Top10Frosts.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkInterestingStudies.Top10InterestingStudies.Dirs.resultFrosts
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesInterestingStudies.studyName
        ))
      }
    }

    private def getAllStationsInfo(): Either[Exception, DataFrame] = {
      try {
        Right(
          SparkCore.dataframes.allStations.select(
            $"indicativo",
            $"nombre",
            $"provincia",
            $"latitud",
            $"longitud",
            $"altitud",
            $"lat_dec",
            $"long_dec"
          )
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationInfoByParam(param: String, value: String): Either[Exception, DataFrame] = {
      try {
        val df: DataFrame = SparkCore.dataframes.allStations
        val dmsToDecimal: String => Double = (dms: String) => {
          val degrees = dms.substring(0, 2).toInt
          val minutes = dms.substring(2, 4).toInt
          val seconds = dms.substring(4, 6).toInt
          val direction = dms.last

          val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)

          if (direction == 'S' || direction == 'W') -decimal else decimal
        }
        val dmsToDecimalUDF: UserDefinedFunction = udf(dmsToDecimal)

        val resultDf = df.filter(col(param) === value)
          .withColumn("lat_dec", round(dmsToDecimalUDF($"latitud"), 6))
          .withColumn("long_dec", round(dmsToDecimalUDF($"longitud"), 6))
          .select(
            $"indicativo",
            $"nombre",
            $"provincia",
            $"latitud".alias("lat_dms"),
            $"longitud".alias("long_dms"),
            $"lat_dec",
            $"long_dec",
            $"altitud"
          )

        Right(resultDf)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getAllStationsByClimateParamInALapse(
      climateParam: String,
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDF = SparkCore.dataframes.allMeteoInfo
        val stationsDF = SparkCore.dataframes.allStations

        // Filtrar por el parámetro climático no nulo
        val filteredDF = meteoDF.filter(col(climateParam).isNotNull)

        // Filtrar por rango de fechas
        val dateFilteredDF = endDate match {
          case Some(end) => filteredDF.filter($"fecha".between(lit(startDate), lit(end)))
          case None => filteredDF.filter(year($"fecha") === startDate.toInt)
        }

        // Obtener indicativos únicos que han operado
        val estacionesOperativas = dateFilteredDF
          .select($"indicativo")
          .distinct()

        // Unir con la info de estaciones
        val resultado = estacionesOperativas
          .join(stationsDF, Seq("indicativo"), "inner")
          .select(
            $"indicativo",
            $"nombre",
            $"provincia",
            $"latitud",
            $"longitud",
            $"altitud",
            $"latitud_dec",
            $"longitud_dec"
          )

        Right(resultado)
      } catch {
        case ex: Exception => Left(ex)
      }
    }

    private def getStationInfoById(stationId: String): Either[Exception, DataFrame] = {
      try {
        val df: DataFrame = SparkCore.dataframes.allStations
        val dmsToDecimal: String => Double = (dms: String) => {
          val degrees = dms.substring(0, 2).toInt
          val minutes = dms.substring(2, 4).toInt
          val seconds = dms.substring(4, 6).toInt
          val direction = dms.last

          val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)

          if (direction == 'S' || direction == 'W') -decimal else decimal
        }
        val dmsToDecimalUDF: UserDefinedFunction = udf(dmsToDecimal)

        val resultDf = df.filter($"indicativo" === stationId)
          .withColumn("lat_dec", round(dmsToDecimalUDF($"latitud"), 6))
          .withColumn("long_dec", round(dmsToDecimalUDF($"longitud"), 6))
          .select(
            $"indicativo",
            $"nombre",
            $"provincia",
            $"latitud".alias("lat_dms"),
            $"longitud".alias("long_dms"),
            $"lat_dec",
            $"long_dec",
            $"altitud"
          )

        Right(resultDf)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationMonthlyAvgTempAndSumPrecInAYear(
      stationId: String,
      observationYear: Int
    ): Either[Exception, DataFrame] = {
      try {
        val df: DataFrame = SparkCore.dataframes.allMeteoInfo

        val filteredDf = df
          .withColumn("tmed", regexp_replace($"tmed", ",", ".").cast("double"))
          .withColumn("prec", regexp_replace($"prec", ",", ".").cast("double"))
          .withColumn("date", to_date($"fecha", "yyyy-MM-dd"))
          .filter(
            $"tmed".isNotNull &&
              $"prec".isNotNull &&
              $"indicativo" === stationId &&
              year($"date") === observationYear
          )

        val resultDf = filteredDf
          .groupBy(
            month($"date").alias("month")
          ).agg(
            round(avg($"tmed"), 2).alias("avg_tmed"),
            round(sum($"prec"), 2).alias("total_prec")
          ).orderBy(
            $"month"
          )

        Right(resultDf)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def test(): Unit = {
      val df: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")

      val dfWithStormConditions = df.filter(
        col("tmed") > 20 &&
        col("hrmax") > 70 &&
        col("presmax") < 1010 &&
        col("prec") > 0.0 &&
        col("sol") > 3
      )

      // Agrupar por estación y contar los días que cumplen con las condiciones
      val dfGroupedByStation = dfWithStormConditions
        .groupBy("indicativo", "nombre", "provincia") // Añadir las columnas de nombre y provincia
        .agg(countDistinct("fecha").alias("dias_con_condiciones"))

      // Agrupar por provincia y sumar los días con condiciones
//      val dfGroupedByProvince = dfGroupedByStation
//        .groupBy("provincia")
//        .agg(sum("dias_con_condiciones").alias("total_dias_con_condiciones"))

      // Ordenar por la suma de los días con condiciones de menor a mayor
//      val dfSortedByProvince = dfGroupedByProvince
//        .orderBy($"total_dias_con_condiciones".desc)

//      // Mostrar el resultado
//      dfSortedByProvince.show()

      val sorted = dfGroupedByStation
        .orderBy($"dias_con_condiciones".desc)

      sorted.show()
    }

    def getTopNClimateParamInALapse(
      climateParam: String,
      startDate: String,
      endDate: Option[String] = None,
      topN: Int = 10,
      highest: Boolean = true
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("station")

        Right(
          meteoDf.filter(endDate match {
              case Some(endDate) => $"fecha".between(lit(startDate), lit(endDate))
              case None => year($"fecha") === startDate.toInt
            }).filter(col(climateParam).isNotNull)
            .groupBy($"indicativo")
            .agg(avg(col(climateParam)).as(s"${climateParam}_avg"))
            .join(stationDf, Seq("indicativo"), "inner")
            .select(
              $"station.indicativo",
              $"station.nombre",
              $"station.provincia",
              col(s"${climateParam}_avg"),
              $"station.latitud",
              $"station.longitud",
              $"station.altitud"
            ).orderBy(if (highest) col(s"${climateParam}_avg").desc else col(s"${climateParam}_avg").asc)
            .limit(topN)
            .withColumn("top", monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getTopNClimateConditionsInALapse(
      climateParams: List[(String, Double, Double)],
      startDate: String,
      endDate: Option[String] = None,
      groupByState: Boolean = false,
      topN: Int = 10,
      highest: Boolean = true
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("station")

        // Filtrado por fechas
        val dateFilteredDf = meteoDf.filter(endDate match {
          case Some(end) => $"fecha".between(lit(startDate), lit(end))
          case None => year($"fecha") === startDate.toInt
        })

        val conditions = climateParams.map { case (paramName, minValue, maxValue) =>
          col(paramName).isNotNull && col(paramName).between(minValue, maxValue)
        }

        // Filtrar utilizando AND lógico para todas las condiciones
        val combinedCondition = conditions.reduce(_ && _)

        val filteredDf = dateFilteredDf.filter(combinedCondition)

        val dfGrouped = if (groupByState) {
          filteredDf
            .groupBy("indicativo", "nombre", "provincia") // Añadir las columnas de nombre y provincia
            .agg(countDistinct("fecha").alias("dias_con_condiciones"))
            .groupBy("provincia")
            .agg(sum("dias_con_condiciones").alias("total_dias_con_condiciones"))
        } else {
          filteredDf
            .groupBy("indicativo") // Añadir las columnas de nombre y provincia
            .agg(countDistinct("fecha").alias("total_dias_con_condiciones"))
        }

        val resultDf = if (groupByState) {
          dfGrouped
            .orderBy(if (highest) col("total_dias_con_condiciones").desc else col("total_dias_con_condiciones").asc)
            .select(
              $"provincia"
            )
        } else {
          // Join con estaciones y selección final
          dfGrouped
            .join(stationDf, Seq("indicativo"), "inner")
            .orderBy(if (highest) col("total_dias_con_condiciones").desc else col("total_dias_con_condiciones").asc)
            .select(
              $"station.indicativo",
              $"station.nombre",
              $"station.provincia",
              $"station.latitud",
              $"station.longitud",
              $"station.altitud"
            )
        }

        Right(
          resultDf
            .limit(topN)
            .withColumn("top", monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getClimateParamInALapseById(
      stationId: String,
      climateParams: Seq[String],
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val df: DataFrame = SparkCore.dataframes.allMeteoInfo

        // Filtrar y transformar cada parámetro climático en la lista
        val filteredDf = climateParams.foldLeft(df) {(acc, climateParam) =>
          acc.filter(col(climateParam).isNotNull)
        }.filter($"indicativo" === stationId)

        // Filtrar por fechas
        val filteredDateDf = endDate match {
          case Some(endDate) => filteredDf.filter($"fecha".between(lit(startDate), lit(endDate)))
          case None => filteredDf.filter(year($"fecha") === startDate.toInt)
        }

        // Seleccionar las columnas para todos los parámetros climáticos
        val orderedDf = filteredDateDf
          .select(Seq(col("fecha")) ++ climateParams.map(param => col(param)): _*)
          .orderBy("fecha")

        Right(orderedDf)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getAllStationsByStatesAvgClimateParamInALapse(
      climateParam: String,
      startDate: String,
      endDate: Option[String] = None,
      states: Option[Seq[String]] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("station")

        Right(
          meteoDf.filter(endDate match {
              case Some(end) => $"fecha".between(lit(startDate), lit(end))
              case None => year($"fecha") === startDate.toInt
            }).filter(states match {
              case Some(stateList) => $"provincia".isin(stateList: _*)
              case None => lit(true)
            }).filter(col(climateParam).isNotNull)
            .groupBy($"indicativo")
            .agg(avg(col(climateParam)).as(s"${climateParam}_avg"))
            .join(stationDf, Seq("indicativo"), "inner")
            .select(
              $"station.nombre",
              col(s"${climateParam}_avg"),
              $"station.latitud_dec",
              $"station.longitud_dec",
              $"station.altitud",
            )
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getAllStationsInfoByAvgClimateParamInALapse(
      climateParam: String,
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo
        val stationsDf: DataFrame = SparkCore.dataframes.allStations

        // Normalización de columnas
        val preparedMeteoDf = meteoDf
          .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))
          .withColumn(climateParam, regexp_replace(col(climateParam), ",", ".").cast("float"))
          .filter(col(climateParam).isNotNull)

        // Filtrado por fechas
        val filteredMeteoDf = endDate match {
          case Some(end) =>
            preparedMeteoDf.filter(col("fecha").between(lit(startDate), lit(end)))
          case None =>
            preparedMeteoDf.filter(year(col("fecha")) === startDate.toInt)
        }

        // Agrupación por estación y cálculo de media
        val avgPerStationDf = filteredMeteoDf
          .groupBy("indicativo")
          .agg(
            avg(col(climateParam)).alias(s"avg_$climateParam"),
            count("*").alias("num_registros")
          )

        // Join solo con estaciones que tengan datos
        val resultDf = avgPerStationDf
          .join(stationsDf, Seq("indicativo"), "inner")
          .filter(col(s"avg_$climateParam") =!= 0.0)
          .select(
            col("indicativo"),
            col("nombre"),
            col("provincia"),
            col("latitud"),
            col("longitud"),
            col("altitud"),
            col(s"avg_$climateParam"),
            col("num_registros")
          )
          .orderBy(desc(s"avg_$climateParam"))

        Right(resultDf)

      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getTopNClimateParamIncrementInAYearLapse(
      stationIds: Seq[String],
      climateParam: String,
      startYear: Int,
      endYear: Int,
      highest: Boolean = true,
      topN: Int = 5
    ): Either[Exception, DataFrame] = {
      try {
        val filteredDf = SparkCore.dataframes.allMeteoInfo
          .filter($"indicativo".isin(stationIds: _*))
          .filter(col(climateParam).isNotNull)
          .withColumn("anio", year($"fecha"))
          .groupBy("indicativo", "anio")
          .agg(avg(col(climateParam)).as(climateParam + "_media"))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val characteristicsVectorAssembler = new VectorAssembler()
          .setInputCols(Array("anio"))
          .setOutputCol("features")

        val result = filteredDf
          .select("indicativo")
          .distinct()
          .as[String]
          .collect()
          .flatMap { indicativo =>
            val fittedModel = new LinearRegression().fit(
              characteristicsVectorAssembler.transform(
                filteredDf.filter($"indicativo" === indicativo)
              ).select(
                $"features", col(climateParam + "_media").as("label")
              )
            )

            val predictions = fittedModel.transform(
                characteristicsVectorAssembler.transform(
                  Seq(startYear, endYear).toDF("anio")
                )
              ).select("anio", "prediction")
              .as[(Int, Double)]
              .collect()
              .toMap

            for {
              predStartYear <- predictions.get(startYear)
              predEndYear <- predictions.get(endYear)
            } yield (indicativo, predEndYear - predStartYear)
          }

        val mediaDiariaPorIndicativo = filteredDf
          .filter($"anio".between(startYear, endYear))
          .groupBy("indicativo")
          .agg(avg(col(climateParam + "_media")).as(climateParam + "_diario_media"))

        filteredDf.unpersist(true)

        Right(
          result
            .toSeq
            .toDF("indicativo", "incremento")
            .join(mediaDiariaPorIndicativo, Seq("indicativo"), "inner")
            .join(SparkCore.dataframes.allStations, Seq("indicativo"), "inner")
            .withColumn(
              "porcentaje_incremento",
              ($"incremento" / col(climateParam + "_diario_media")) * 100
            )
            .select(
              "incremento",
              "porcentaje_incremento",
              climateParam + "_diario_media",
              "indicativo",
              "nombre",
              "provincia",
              "latitud",
              "longitud",
              "altitud",
              "latitud_dec",
              "longitud_dec"
            )
            .orderBy(if (highest) $"incremento".desc else $"incremento".asc)
            .limit(topN)
            .withColumn("top", monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    def getLongestOperativeStationsPerProvince(params: Seq[String], maxNullMonths: Int = 3): DataFrame = {
      val df: DataFrame = SparkCore.dataframes.allMeteoInfo

      val dfParsed = df
        .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))
        .withColumn("year_month", date_format(col("fecha"), "yyyy-MM"))

      val nullCondition = params.map(p => col(p).isNull).reduce(_ || _)

      val monthlyStats = dfParsed
        .groupBy("provincia", "indicativo", "nombre", "year_month")
        .agg(
          count("*").alias("n_registros"),
          count(when(nullCondition, 1)).alias("null_param")
        )

      val inactiveMonths = monthlyStats
        .withColumn("inactive", when(col("n_registros") === 0 || col("null_param") === col("n_registros"), 1).otherwise(0))

      val window = Window.partitionBy("provincia", "indicativo", "nombre").orderBy("year_month")

      val withCutFlag = inactiveMonths
        .withColumn("inactive_seq", sum("inactive").over(window.rowsBetween(-maxNullMonths + 1, 0)))
        .withColumn("cut_flag", when(col("inactive_seq") === maxNullMonths, 1).otherwise(0))

      val withSegment = withCutFlag
        .withColumn("segment_id", sum("cut_flag").over(window.rowsBetween(Window.unboundedPreceding, 0)))

      // Volver a unir con fechas completas
      val fullDates = dfParsed
        .select("provincia", "indicativo", "nombre", "fecha")
        .withColumn("year_month", date_format(col("fecha"), "yyyy-MM"))

      val joinedWithDates = withSegment
        .join(fullDates, Seq("provincia", "indicativo", "nombre", "year_month"))

      val activePeriods = joinedWithDates
        .filter(col("inactive") === 0)
        .groupBy("provincia", "indicativo", "nombre", "segment_id")
        .agg(
          count("*").alias("active_months"),
          min("fecha").alias("start_date"),
          max("fecha").alias("end_date")
        )

      val maxDurations = activePeriods
        .groupBy("provincia")
        .agg(max("active_months").alias("max_months"))

      val longestStations = activePeriods
        .join(maxDurations, Seq("provincia"))
        .filter(col("active_months") === col("max_months"))
        .drop("max_months")

      longestStations.orderBy("provincia", "start_date")
    }
  }
}
