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

    object Stations {
      private val ctsSparkQueriesStations = Spark.Queries.Stations
      private val ctsLogsSparkQueriesStudiesStations = Logs.SparkQueries.Studies.Stations
      private val ctsStorageDataSparkStations = Storage.DataSpark.Stations
      private val ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys = RemoteRequest.AemetAPI.Params.AllStationInfo.Metadata.DataFieldsJSONKeys

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesStations.studyName
        ))

        // Station count evolution from the start of registers
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesStations.Execution.stationCountEvolFromStart,
          List(
            FetchAndSaveInfo(
              getStationCountByColumnInLapse(
                column = (
                  year(col(ctsSparkQueriesStations.Execution.CountEvolFromStart.param)),
                  ctsSparkQueriesStations.Execution.CountEvolFromStart.paramGroupName
                ),
                startDate = ctsSparkQueriesStations.Execution.CountEvolFromStart.startDate,
                endDate = Some(ctsSparkQueriesStations.Execution.CountEvolFromStart.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkStations.StationCountEvolFromStart.Dirs.result
            )
          )
        )

        // Count of stations by state in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesStations.Execution.stationCountByState2024,
          List(
            FetchAndSaveInfo(
              getStationCountByColumnInLapse(
                column = (
                  col(ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.provinciaJKey),
                  ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.provinciaJKey
                ),
                startDate = ctsSparkQueriesStations.Execution.StationCountByState2024.startDate) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkStations.StationCountByState2024.Dirs.result
            )
          )
        )

        // Count of stations by altitude in 2024
        simpleFetchAndSave(
          ctsLogsSparkQueriesStudiesStations.Execution.stationCountByAltitude2024,
          List(
            FetchAndSaveInfo(
              getStationsCountByParamIntervalsInALapse(
                paramIntervals = ctsSparkQueriesStations.Execution.StationCountByAltitude2024.intervals,
                startDate = ctsSparkQueriesStations.Execution.StationCountByAltitude2024.startDate) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorageDataSparkStations.StationCountByAltitude2024.Dirs.result
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesStations.studyName
        ))
      }
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

    object SingleParamStudies {
      private val ctsSparkQueriesSingleParamStudies = Spark.Queries.SingleParamStudies
      private val ctsLogsSparkQueriesStudiesSingleParamStudies = Logs.SparkQueries.Studies.SingleParamStudies
      private val ctsStorageDataSparkSingleParamStudies = Storage.DataSpark.SingleParamStudies

      def execute(): Unit = {
        ctsSparkQueriesSingleParamStudies.singleParamStudiesValues.foreach(study => {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
            study.studyParam.replace("_", " ")
          ))

          // Top 10 places with the highest eratures in 2024
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top10Highest2024.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Top10Highest2024.startDate,
                  endDate = None) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top10.Dirs.resultHighest2024.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the highest eratures in the last decade
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top10HighestDecade.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Top10HighestDecade.startDate,
                  endDate = Some(ctsSparkQueriesSingleParamStudies.Execution.Top10HighestDecade.endDate)) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top10.Dirs.resultHighestDecade.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the highest eratures from the start of registers
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top10HighestGlobal.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Top10HighestGlobal.startDate,
                  endDate = Some(ctsSparkQueriesSingleParamStudies.Execution.Top10HighestGlobal.endDate)) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top10.Dirs.resultHighestGlobal.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the lowest eratures in 2024
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top10Lowest2024.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Top10Lowest2024.startDate,
                  endDate = None,
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top10.Dirs.resultLowest2024.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the lowest eratures in the last decade
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top10LowestDecade.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Top10LowestDecade.startDate,
                  endDate = Some(ctsSparkQueriesSingleParamStudies.Execution.Top10LowestDecade.endDate),
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top10.Dirs.resultLowestDecade.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the lowest eratures from the start of registers
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top10LowestGlobal.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Top10LowestGlobal.startDate,
                  endDate = Some(ctsSparkQueriesSingleParamStudies.Execution.Top10LowestGlobal.endDate),
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top10.Dirs.resultLowestGlobal.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // erature evolution from the start of registers for each state
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.evolFromStartForEachState.format(
              study.studyParam.capitalize
            ),
            study.reprStationRegs.flatMap(registry => {
              List(
                FetchAndSaveInfo(
                  getStationInfoById(registry.stationId) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorageDataSparkSingleParamStudies.EvolFromStartForEachState.Dirs.resultStation.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSC.replace(" ", "_")
                  ),
                  ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.evolFromStartForEachStateStartStation.format(
                    registry.stateName.capitalize
                  )
                ),
                FetchAndSaveInfo(
                  getClimateParamInALapseById(
                    registry.stationId,
                    List(study.dataframeColName),
                    registry.startDate,
                    Some(registry.endDate)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorageDataSparkSingleParamStudies.EvolFromStartForEachState.Dirs.resultEvol.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSC.replace(" ", "_")
                  ),
                  ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.evolFromStartForEachStateStart.format(
                    registry.stateName,
                    study.studyParam.replace("_", " ")
                  )
                )
              )
            })
          )

          // Top 5 highest increment of erature
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top5HighestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(registry => registry.stationId),
                  climateParam = study.dataframeColName,
                  startYear = ctsSparkQueriesSingleParamStudies.Execution.Top5HighestInc.startYear,
                  endYear = ctsSparkQueriesSingleParamStudies.Execution.Top5HighestInc.endYear
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top5Inc.Dirs.resultHighest.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 5 lowest increment of erature
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top5LowestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(registry => registry.stationId),
                  climateParam = study.dataframeColName,
                  startYear = ctsSparkQueriesSingleParamStudies.Execution.Top5LowestInc.startYear,
                  endYear = ctsSparkQueriesSingleParamStudies.Execution.Top5LowestInc.endYear,
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Top5Inc.Dirs.resultLowest.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Get average erature in 2024 for all station in the spanish continental territory
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.avg2024AllStationSpainContinental.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Avg2024AllStationSpain.startDate
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Avg2024AllStationSpain.Dirs.resultContinental.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Get average erature in 2024 for all station in the canary islands territory
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.avg2024AllStationSpainCanary.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  startDate = ctsSparkQueriesSingleParamStudies.Execution.Avg2024AllStationSpain.startDate,
                  states = Some(ctsSparkQueriesSingleParamStudies.Execution.Avg2024AllStationSpain.canaryIslandStates),
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorageDataSparkSingleParamStudies.Avg2024AllStationSpain.Dirs.resultCanary.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
            study.studyParam
          ))
        })
      }
    }

    object InterestingStudies {
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

    private def getStationInfoById(
      stationId: String
    )
    : Either[Exception, DataFrame] = {
      try {
        val allStationsDf: DataFrame = SparkCore.dataframes.allStations.as("stations")

        Right(
          allStationsDf
            .filter($"indicativo" === stationId)
            .select(
              $"indicativo",
              $"nombre",
              $"provincia",
              $"latitud".alias("lat_dms"),
              $"longitud".alias("long_dms"),
              $"altitud"
            )
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationCountByColumnInLapse(
      column: (Column, String),
      startDate: String,
      endDate: Option[String] = None,
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")

        Right(
          meteoDf.filter(
            endDate match {
              case Some(end) => $"fecha".between(lit(startDate), lit(end))
              case None => year($"fecha") === startDate.toInt
            }
          ).groupBy(column._1.alias(column._2))
          .agg(countDistinct($"indicativo").alias("station_count"))
          .orderBy(column._2)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationsCountByParamIntervalsInALapse(
      paramIntervals: List[(String, Double, Double)],
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")

        val param = col(paramIntervals.head._1)

        val filteredDf = meteoDf.filter(
          endDate match {
            case Some(end) => $"fecha".between(lit(startDate), lit(end))
            case None => year($"fecha") === startDate.toInt
          }
        ).select(
          $"indicativo",
          col(paramIntervals.head._1).cast("double")
        )

        Right(
          paramIntervals.zipWithIndex.map {case ((_param, _min, _max), idx) =>
            val actualMin = if (_min.isNegInfinity) filteredDf.agg(min(_param)).collect()(0)(0) else _min
            val actualMax = if (_max.isPosInfinity) filteredDf.agg(max(_param)).collect()(0)(0) else _max

            filteredDf.filter(
              if (idx == 0)
                param >= actualMin && param <= actualMax
              else
                param > actualMin && param <= actualMax
            ).agg(
              lit(actualMin).as("min_value"),
              lit(actualMax).as("max_value"),
              countDistinct("indicativo").as("station_count")
            )
          }.reduce(_.union(_)).orderBy("min_value")
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationMonthlyAvgTempAndSumPrecInAYear(
      stationId: String,
      observationYear: Int
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")

        Right(
          meteoDf
            .filter(
              $"tmed".isNotNull &&
                $"prec".isNotNull &&
                $"indicativo" === stationId &&
                year($"fecha") === observationYear
            ).groupBy(
              month($"fecha").alias("month")
            ).agg(
              round(avg($"tmed"), 2).alias("avg_tmed"),
              round(sum($"prec"), 2).alias("total_prec")
            ).orderBy($"month")
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getTopNClimateParamInALapse(
      climateParam: String,
      startDate: String,
      endDate: Option[String] = None,
      topN: Int = 10,
      highest: Boolean = true
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("stations")

        Right(
          meteoDf.filter(endDate match {
            case Some(endDate) => $"fecha".between(lit(startDate), lit(endDate))
            case None => year($"fecha") === startDate.toInt
          }).filter(col(climateParam).isNotNull)
          .groupBy($"indicativo")
          .agg(avg(col(climateParam)).as(s"${climateParam}_avg"))
          .join(stationDf, Seq("indicativo"), "inner")
          .select(
            $"stations.indicativo",
            $"stations.nombre",
            $"stations.provincia",
            col(s"${climateParam}_avg"),
            $"stations.latitud".alias("lat_dms"),
            $"stations.longitud".alias("long_dms"),
            $"stations.altitud"
          ).orderBy(if (highest) col(s"${climateParam}_avg").desc else col(s"${climateParam}_avg").asc)
          .limit(topN)
          .withColumn("top", monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getTopNClimateConditionsInALapse(
      climateParams: List[(String, Double, Double)],
      startDate: String,
      endDate: Option[String] = None,
      groupByState: Boolean = false,
      topN: Int = 10,
      highest: Boolean = true
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("stations")

        val filteredDf = meteoDf.filter(endDate match {
          case Some(end) => $"fecha".between(lit(startDate), lit(end))
          case None => year($"fecha") === startDate.toInt
        })
        .filter(climateParams.map { case (paramName, minValue, maxValue) =>
          col(paramName).isNotNull && col(paramName).between(minValue, maxValue)
        }.reduce(_ && _))

        Right(
          (if (groupByState) {
            filteredDf
              .groupBy("provincia")
              .agg(count("fecha").alias("days_with_conds"))
              .orderBy(if (highest) col("days_with_conds").desc else col("days_with_conds").asc)
              .withColumn("top", monotonically_increasing_id() + 1)
              .select(
                $"provincia",
                $"top"
              )
          } else {
            filteredDf
              .groupBy("indicativo")
              .agg(countDistinct("fecha").alias("days_with_conds"))
              .join(stationDf, Seq("indicativo"), "inner")
              .orderBy(if (highest) col("days_with_conds").desc else col("days_with_conds").asc)
              .withColumn("top", monotonically_increasing_id() + 1)
              .select(
                $"stations.indicativo",
                $"stations.nombre",
                $"stations.provincia",
                $"stations.latitud".alias("lat_dms"),
                $"stations.longitud".alias("long_dms"),
                $"stations.altitud",
                $"top"
              )
          })
          .orderBy($"top".asc)
          .limit(topN)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getClimateParamInALapseById(
      stationId: String,
      climateParams: Seq[String],
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        
        Right(
          climateParams.foldLeft(
            meteoDf.filter(endDate match {
              case Some(endDate) => $"fecha".between(lit(startDate), lit(endDate))
              case None => year($"fecha") === startDate.toInt
            })
          ) { (acc, climateParam) =>
            acc.filter(col(climateParam).isNotNull)
          }
          .filter($"indicativo" === stationId)
          .select(Seq(col("fecha")) ++ climateParams.map(param => col(param)): _*)
          .orderBy("fecha")
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getAllStationsByStatesAvgClimateParamInALapse(
      climateParam: String,
      startDate: String,
      endDate: Option[String] = None,
      states: Option[Seq[String]] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("stations")

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
            $"stations.nombre",
            col(s"${climateParam}_avg"),
            $"stations.latitud_dec".alias("lat_dec"),
            $"stations.longitud_dec".alias("long_dec"),
            $"stations.altitud",
          )
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getTopNClimateParamIncrementInAYearLapse(
      stationIds: Seq[String],
      climateParam: String,
      startYear: Int,
      endYear: Int,
      highest: Boolean = true,
      topN: Int = 5
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("stations")
        
        val filteredDf = meteoDf
          .filter($"indicativo".isin(stationIds: _*))
          .filter(col(climateParam).isNotNull)
          .withColumn("year", year($"fecha"))
          .groupBy("indicativo", "year")
          .agg(avg(col(climateParam)).as(s"${climateParam}_avg"))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val characteristicsVectorAssembler = new VectorAssembler()
          .setInputCols(Array("year"))
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
                $"features", col(s"${climateParam}_avg").as("label")
              )
            )

            val predictions = fittedModel.transform(
                characteristicsVectorAssembler.transform(
                  Seq(startYear, endYear).toDF("year")
                )
              ).select("year", "prediction")
              .as[(Int, Double)]
              .collect()
              .toMap

            for {
              predStartYear <- predictions.get(startYear)
              predEndYear <- predictions.get(endYear)
            } yield (indicativo, predEndYear - predStartYear)
          }

        val mediaDiariaPorIndicativo = filteredDf
          .filter($"year".between(startYear, endYear))
          .groupBy("indicativo")
          .agg(avg(col(s"${climateParam}_avg")).as(s"${climateParam}_daily_avg"))

        filteredDf.unpersist(true)

        Right(
          result
            .toSeq
            .toDF("indicativo", "incr")
            .join(mediaDiariaPorIndicativo, Seq("indicativo"), "inner")
            .join(stationDf, Seq("indicativo"), "inner")
            .withColumn(
              "incr_percentage",
              ($"incr" / col(s"${climateParam}_daily_avg")) * 100
            )
            .select(
              $"incr",
              $"incr_percentage",
              col(s"${climateParam}_daily_avg"),
              $"stations.indicativo",
              $"stations.nombre",
              $"stations.provincia",
              $"stations.latitud".alias("lat_dms"),
              $"stations.longitud".alias("long_dms"),
              $"stations.altitud"
            )
            .orderBy(if (highest) $"incr".desc else $"incr".asc)
            .limit(topN)
            .withColumn("top", monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getLongestOperativeStationsPerProvince(params: Seq[String], maxNullMonths: Int = 3): DataFrame = {
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
