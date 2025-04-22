package Core.Spark

import Config.ConstantsV2._
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.JSONUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

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
        val ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys = RemoteRequest.AemetAPI.Params.Global.Metadata.SchemaJSONKeys
        StructType(
          metadataJSON(ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys.fieldDefJKey).arr.map(field => {
            StructField(
              field(ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys.DataFieldsJSONKeys.idJKey).str,
              StringType,
              !field(ctsRemoteReqAemetParamsGlobalMetadataSchemaJSONKeys.DataFieldsJSONKeys.requiredJKey).bool
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
          .mode(SaveMode.Overwrite)
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
            (column => col(column).cast(IntegerType)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmedJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.precJKey ->
            (column => {
              when(col(column) === "Acum", lit(null).cast(DoubleType))
                .otherwise(
                  when(col(column) === "Ip", lit(0.0).cast(DoubleType))
                    .otherwise(round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1))
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tminJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatminJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.tmaxJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horatmaxJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.dirJKey ->
            (column => {
              when(col(column) === "99" || col(column) === "88", lit(null).cast(IntegerType))
                .otherwise(regexp_replace(col(column), ",", "").cast(IntegerType))
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.velmediaJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.rachaJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horarachaJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.solJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presmaxJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horapresmaxJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.presminJKey ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horapresminJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmediaJKey ->
            (column => col(column).cast(IntegerType)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrmaxJKey ->
            (column => col(column).cast(IntegerType)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrmaxJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col("fecha"), col(column)),
                    s"${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.dateFormat} ${ctsRemoteReqAemetParamsAllMeteoInfoExecFormat.hourMinuteFormat}"
                  )
                )
            }),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.hrminJKey ->
            (column => col(column).cast(IntegerType)),
          ctsRemoteReqAemetParamsAllMeteoInfoMetadataDataFieldsJSONKeys.horahrminJKey ->
            (column => {
              when(col(column) === "Varias", lit(null).cast(TimestampType))
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
            (column => col(column).cast(IntegerType)),
          "lat_dec" ->
            (_ => round(udf((dms: String) => {
              val degrees = dms.substring(0, 2).toInt
              val minutes = dms.substring(2, 4).toInt
              val seconds = dms.substring(4, 6).toInt
              val direction = dms.last
              val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
              if (direction == 'S' || direction == 'W') -decimal else decimal
            }).apply(col(ctsRemoteReqAemetParamsAllStationInfoMetadataDataFieldsJSONKeys.latitudJKey)), 6)),
          "long_dec" ->
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

    private val ctsLogsSparkQueriesStudiesGlobal = Logs.SparkQueries.Studies.Global

    private case class FetchAndSaveInfo(
      dataframe: DataFrame,
      pathToSave: String,
      title: String = "",
      showInfoMessage: String = ctsLogsSparkQueriesStudiesGlobal.showInfo,
      saveInfoMessage: String = ctsLogsSparkQueriesStudiesGlobal.saveInfo
    )

    def execute(): Unit = {
      SparkCore.dataframes.allStations.as("stations").count()
      SparkCore.dataframes.allMeteoInfo.as("meteo").count()

      Stations.execute()
      Climograph.execute()
      SingleParamStudies.execute()
      InterestingStudies.execute()
    }

    private def simpleFetchAndSave(
      queryTitle: String = "",
      queries: Seq[FetchAndSaveInfo],
      encloseHalfLengthStart: Int = 35
    ): Seq[DataFrame] = {
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

      queries.map(query => query.dataframe)
    }

    object Stations {
      private val ctsSparkQueriesStations = Spark.Queries.Stations
      private val ctsLogsSparkQueriesStudiesStations = Logs.SparkQueries.Studies.Stations
      private val ctsStorageDataSparkStations = Storage.DataSpark.Stations

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
                  col(ctsSparkQueriesStations.Execution.StationCountByState2024.param),
                  ctsSparkQueriesStations.Execution.StationCountByState2024.paramGroupName
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
                  paramNameToShow = study.studyParamAbbrev,
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
                  paramNameToShow = study.studyParamAbbrev,
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
                  paramNameToShow = study.studyParamAbbrev,
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
                  paramNameToShow = study.studyParamAbbrev,
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
                  paramNameToShow = study.studyParamAbbrev,
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
                  paramNameToShow = study.studyParamAbbrev,
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
          val regressionModelDf: DataFrame = simpleFetchAndSave(
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
                    List(
                      (study.dataframeColName, study.studyParamAbbrev)
                    ),
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
                ),
                FetchAndSaveInfo(
                  getStationClimateParamRegressionModelInALapse(
                    registry.stationId,
                    study.dataframeColName,
                    registry.startDate,
                    Some(registry.endDate)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorageDataSparkSingleParamStudies.EvolFromStartForEachState.Dirs.resultEvolRegression.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSC.replace(" ", "_")
                  ),
                  ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.evolFromStartForEachStateStartRegression.format(
                    registry.stateName.capitalize,
                    study.studyParam.replace("_", " ")
                  )
                )
              )
            })
          ).zipWithIndex.filter {
            case (_, idx) => idx % 3 == 2
          }.map(_._1).reduce(_ union _).persist(StorageLevel.MEMORY_AND_DISK_SER)

          regressionModelDf.count()

          // Top 5 highest increment of erature
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.top5HighestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(registry => registry.stationId),
                  regressionModels = regressionModelDf,
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
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
                  regressionModels = regressionModelDf,
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
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

          regressionModelDf.unpersist()

          // Get average erature in 2024 for all station in the spanish continental territory
          simpleFetchAndSave(
            ctsLogsSparkQueriesStudiesSingleParamStudies.Execution.avg2024AllStationSpainContinental.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
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
                  paramNameToShow = study.studyParamAbbrev,
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
                  ctsSparkQueriesInterestingStudies.Execution.PrecAndPressEvolFromStartForEachState.climateParams,
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
              $"indicativo".alias("station_id"),
              $"nombre".alias("station_name"),
              $"provincia".alias("state"),
              $"latitud".alias("lat_dms"),
              $"longitud".alias("long_dms"),
              $"altitud".alias("altitude")
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
          ).groupBy(column._1.as(column._2))
          .agg(countDistinct($"indicativo").as("count"))
          .select(
            col(column._2),
            $"count"
          )
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
          col(paramIntervals.head._1).cast(DoubleType)
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
              countDistinct("indicativo").as("count")
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
              month($"fecha").as("month")
            ).agg(
              round(avg($"tmed"), 1).as("temp_monthly_avg"),
              round(sum($"prec"), 1).as("prec_monthly_sum")
            ).orderBy($"month")
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getTopNClimateParamInALapse(
      climateParam: String,
      paramNameToShow: String,
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
            $"stations.indicativo".alias("station_id"),
            $"stations.nombre".alias("station_name"),
            $"stations.provincia".alias("state"),
            col(s"${climateParam}_avg").alias(s"${paramNameToShow}_daily_avg"),
            $"stations.latitud".alias("lat_dms"),
            $"stations.longitud".alias("long_dms"),
            $"stations.altitud".alias("altitude")
          ).orderBy(if (highest) col(s"${paramNameToShow}_daily_avg").desc else col(s"${paramNameToShow}_daily_avg").asc)
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
                $"provincia".alias("state"),
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
                $"stations.indicativo".alias("station_id"),
                $"stations.nombre".alias("station_name"),
                $"stations.provincia".alias("state"),
                $"stations.latitud".alias("lat_dms"),
                $"stations.longitud".alias("long_dms"),
                $"stations.altitud".alias("altitude"),
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
      climateParams: Seq[(String, String)],
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
            acc.filter(col(climateParam._1).isNotNull)
          }
          .filter($"indicativo" === stationId)
          .select(Seq(col("fecha").alias("date")) ++ climateParams.map(param => round(col(param._1), 1).alias(s"${param._2}_daily_avg")): _*)
          .orderBy("date")
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getAllStationsByStatesAvgClimateParamInALapse(
      climateParam: String,
      paramNameToShow: String,
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
            $"stations.indicativo".alias("station_id"),
            $"stations.nombre".alias("station_name"),
            $"stations.provincia".alias("state"),
            round(col(s"${climateParam}_avg"), 1).alias(s"${paramNameToShow}_daily_avg"),
            $"stations.lat_dec".alias("lat_dec"),
            $"stations.long_dec".alias("long_dec"),
            $"stations.altitud".alias("altitude")
          )
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationClimateParamRegressionModelInALapse(
      stationId: String,
      climateParam: String,
      startYear: String,
      endYear: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")

        val filteredDF = meteoDf
          .filter($"indicativo" === stationId)
          .filter(endYear match {
            case Some(end) => $"fecha".between(lit(startYear), lit(end))
            case None => year($"fecha") === startYear
          })
          .filter(col(climateParam).isNotNull)
          .withColumn("year", year($"fecha"))
          .groupBy("year")
          .agg(avg(col(climateParam)).as("climate_param_avg"))
          .select(
            $"year".alias("x"),
            $"climate_param_avg".alias("y")
          )

        val (meanX, meanY) = filteredDF.agg(avg("x"), avg("y")).as[(Double, Double)].first() match {
          case (mx, my) => (mx, my)
        }

        val (beta1, beta0) = filteredDF.withColumn("x_diff", $"x" - meanX)
          .withColumn("y_diff", $"y" - meanY)
          .agg(
            sum($"x_diff" * $"y_diff").as("num"),
            sum($"x_diff" * $"x_diff").as("den")
          ).as[(Double, Double)].first() match {
          case (num, den) =>
            val b1 = num / den
            val b0 = meanY - b1 * meanX
            (b1, b0)
        }

        Right(Seq((stationId, beta1, beta0)).toDF("station_id", "beta_1", "beta_0"))

      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getTopNClimateParamIncrementInAYearLapse(
      stationIds: Seq[String],
      regressionModels: DataFrame,
      climateParam: String,
      paramNameToShow: String,
      startYear: Int,
      endYear: Int,
      highest: Boolean = true,
      topN: Int = 5
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as("meteo")
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as("stations")

        Right(
          regressionModels
            .filter($"station_id".isin(stationIds: _*))
            .withColumn(
              "inc",
              ($"beta_1" * lit(endYear) + $"beta_0") - ($"beta_1" * lit(startYear) + $"beta_0")
            )
            .select($"station_id".as("indicativo"), $"inc")
            .join(
              meteoDf
                .filter($"indicativo".isin(stationIds: _*))
                .filter(col(climateParam).isNotNull)
                .withColumn("year", year($"fecha"))
                .filter($"year".between(startYear, endYear))
                .groupBy("indicativo")
                .agg(avg(col(climateParam)).as(s"${climateParam}_daily_avg")),
              Seq("indicativo"),
              "inner"
            )
            .join(stationDf, Seq("indicativo"), "inner")
            .withColumn("inc_perc", $"inc" / col(s"${climateParam}_daily_avg") * 100)
            .select(
              round($"inc", 1).alias("inc"),
              round($"inc_perc", 1).alias("inc_prec"),
              round(col(s"${climateParam}_daily_avg"), 1).alias(s"${paramNameToShow}_daily_avg"),
              $"stations.indicativo".alias("station_id"),
              $"stations.nombre".alias("station_name"),
              $"stations.provincia".alias("state"),
              $"stations.latitud".alias("lat_dms"),
              $"stations.longitud".alias("long_dms"),
              $"stations.altitud".alias("altitude")
            )
            .orderBy(if (highest) $"inc".desc else $"inc".asc)
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
