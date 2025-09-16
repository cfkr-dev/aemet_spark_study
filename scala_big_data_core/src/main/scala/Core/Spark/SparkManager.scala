package Core.Spark

import Config.{GlobalConf, SparkConf}
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.JSONUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkManager {
  private val ctsExecutionGlobalConf = SparkConf.Constants.init.execution.globalConf
  private val ctsExecutionDataframeConf = SparkConf.Constants.init.execution.dataframeConf
  private val ctsSchemaAemetAllMeteoInfo = GlobalConf.Constants.schema.aemetConf.allMeteoInfo
  private val ctsSchemaAemetAllStation = GlobalConf.Constants.schema.aemetConf.allStationInfo
  private val ctsSchemaSparkMeteo = GlobalConf.Constants.schema.sparkConf.meteoDf
  private val ctsSchemaSparkAllStation = GlobalConf.Constants.schema.sparkConf.stationsDf
  private val ctsAllStationSpecialValues = ctsExecutionDataframeConf.allStationsDf.specialValues

  private object SparkCore {
    private val ctsExecutionSessionConf = SparkConf.Constants.init.execution.sessionConf
    private val ctsInitLogs = SparkConf.Constants.init.log
    private val ctsAllMeteoInfoSpecialValues = ctsExecutionDataframeConf.allMeteoInfoDf.specialValues

    val sparkSession: SparkSession = createSparkSession(
      ctsExecutionSessionConf.sessionName,
      ctsExecutionSessionConf.sessionMaster,
      ctsExecutionSessionConf.sessionLogLevel
    )

    def startSparkSession(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsInitLogs.sessionConf.startSparkSession)
      SparkCore.sparkSession.conf.getAll.foreach {case (k, v) => printlnConsoleMessage(NotificationType.Information, s"$k = $v")}
      SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName).count()
      SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName).count()
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsInitLogs.sessionConf.endSparkSession)
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

    def saveDataframeAsJSON(dataframe: DataFrame, path: String): Either[Exception, String] = {
      try {
        dataframe.write
          .mode(SaveMode.Overwrite)
          .json(path)

        Right(path)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

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
        StructType(
          metadataJSON(ctsExecutionDataframeConf.aemetMetadataStructure.schemaDef).arr.map(field => {
            StructField(
              field(ctsExecutionDataframeConf.aemetMetadataStructure.fieldId).str,
              StringType,
              !field(ctsExecutionDataframeConf.aemetMetadataStructure.fieldRequired).bool
            )
          }).toArray
        )
      }

      Right(session
        .read.format(ctsExecutionGlobalConf.readConfig.readFormat)
        .option(ctsExecutionGlobalConf.readConfig.readMode, value = true)
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

    object dataframes {
      private val ctsStorageAemet = SparkConf.Constants.init.storage.aemetConf
      private val ctsStorageIfapaAemetFormat = SparkConf.Constants.init.storage.ifapaAemetFormatConf
      private val ctsUtils = GlobalConf.Constants.utils

      val allMeteoInfo: DataFrame =
        formatAllMeteoInfoDataframe(
          createDataframeFromJSONAndAemetMetadata(
            sparkSession,
            ctsStorageAemet.allMeteoInfo.dirs.data,
            ctsStorageAemet.allMeteoInfo.filepaths.metadata
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df.union(createDataframeFromJSONAndAemetMetadata(
              sparkSession,
              ctsStorageIfapaAemetFormat.singleStationMeteoInfo.dirs.data,
              ctsStorageIfapaAemetFormat.singleStationMeteoInfo.filepaths.metadata
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
            ctsStorageAemet.allStationInfo.dirs.data,
            ctsStorageAemet.allStationInfo.filepaths.metadata
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df.union(createDataframeFromJSONAndAemetMetadata(
              sparkSession,
              ctsStorageIfapaAemetFormat.singleStationInfo.dirs.data,
              ctsStorageIfapaAemetFormat.singleStationInfo.filepaths.metadata
            ) match {
              case Left(exception) => throw exception
              case Right(df) => df
            })
          }
        ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      private def formatAllMeteoInfoDataframe(dataframe: DataFrame): DataFrame = {
        val formatters: Map[String, String => Column] = Map(
          ctsSchemaAemetAllMeteoInfo.fecha ->
            (column => to_date(col(column), ctsUtils.formats.dateFormat)),
          ctsSchemaAemetAllMeteoInfo.provincia ->
            (column => udf((value: String) => Map(
              ctsAllMeteoInfoSpecialValues.provincia.staCruzDeTenerife ->
                ctsAllMeteoInfoSpecialValues.provincia.santaCruzDeTenerife,
              ctsAllMeteoInfoSpecialValues.provincia.baleares ->
                ctsAllMeteoInfoSpecialValues.provincia.illesBalears,
              ctsAllMeteoInfoSpecialValues.provincia.almeriaSc ->
                ctsAllMeteoInfoSpecialValues.provincia.almeria
            ).getOrElse(value, value)).apply(col(column))),
          ctsSchemaAemetAllMeteoInfo.altitud ->
            (column => col(column).cast(IntegerType)),
          ctsSchemaAemetAllMeteoInfo.tMed ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.prec ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.prec.acum, lit(null).cast(DoubleType))
                .otherwise(
                  when(col(column) === ctsAllMeteoInfoSpecialValues.prec.ip, lit(0.0).cast(DoubleType))
                    .otherwise(round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1))
                )
            }),
          ctsSchemaAemetAllMeteoInfo.tMin ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.horaTMin ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
                  )
                )
            }),
          ctsSchemaAemetAllMeteoInfo.tMax ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.horaTMax ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
                  )
                )
            }),
          ctsSchemaAemetAllMeteoInfo.dir ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.dir.noData || col(column) === ctsAllMeteoInfoSpecialValues.dir.variable, lit(null).cast(IntegerType))
                .otherwise(regexp_replace(col(column), ",", "").cast(IntegerType))
            }),
          ctsSchemaAemetAllMeteoInfo.velMedia ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.racha ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.horaRacha ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
                  )
                )
            }),
          ctsSchemaAemetAllMeteoInfo.sol ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.presMax ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.horaPresMax ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
                  )
                )
            }),
          ctsSchemaAemetAllMeteoInfo.presMin ->
            (column => round(regexp_replace(col(column), ",", ".").cast(DoubleType), 1)),
          ctsSchemaAemetAllMeteoInfo.horaPresMin ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
                  )
                )
            }),
          ctsSchemaAemetAllMeteoInfo.hrMedia ->
            (column => col(column).cast(IntegerType)),
          ctsSchemaAemetAllMeteoInfo.hrMax ->
            (column => col(column).cast(IntegerType)),
          ctsSchemaAemetAllMeteoInfo.horaHrMax ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
                  )
                )
            }),
          ctsSchemaAemetAllMeteoInfo.hrMin ->
            (column => col(column).cast(IntegerType)),
          ctsSchemaAemetAllMeteoInfo.horaHrMin ->
            (column => {
              when(col(column) === ctsAllMeteoInfoSpecialValues.genericHour.varias, lit(null).cast(TimestampType))
                .otherwise(
                  to_timestamp(
                    concat_ws(" ", col(ctsSchemaAemetAllMeteoInfo.fecha), col(column)),
                    s"${ctsUtils.formats.dateFormat} ${ctsUtils.formats.hourMinuteFormat}"
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
        val formatters: Map[String, String => Column] = Map(
          ctsSchemaAemetAllStation.provincia ->
            (column => udf((value: String) => Map(
              ctsAllStationSpecialValues.provincia.staCruzDeTenerife ->
                ctsAllStationSpecialValues.provincia.santaCruzDeTenerife,
              ctsAllStationSpecialValues.provincia.baleares ->
                ctsAllStationSpecialValues.provincia.illesBalears,
              ctsAllStationSpecialValues.provincia.almeriaSc ->
                ctsAllStationSpecialValues.provincia.almeria
            ).getOrElse(value, value)).apply(col(column))),
          ctsSchemaAemetAllStation.altitud ->
            (column => col(column).cast(IntegerType)),
          ctsSchemaSparkAllStation.latDec ->
            (_ => round(udf((dms: String) => {
              val degrees = dms.substring(0, 2).toInt
              val minutes = dms.substring(2, 4).toInt
              val seconds = dms.substring(4, 6).toInt
              val direction = dms.last
              val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
              if (direction == ctsAllStationSpecialValues.genericCardinalCoord.south ||
                direction == ctsAllStationSpecialValues.genericCardinalCoord.west
              ) -decimal else decimal
            }).apply(col(ctsSchemaAemetAllStation.latitud)), 6)),
          ctsSchemaSparkAllStation.longDec ->
            (_ => round(udf((dms: String) => {
              val degrees = dms.substring(0, 2).toInt
              val minutes = dms.substring(2, 4).toInt
              val seconds = dms.substring(4, 6).toInt
              val direction = dms.last
              val decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
              if (direction == ctsAllStationSpecialValues.genericCardinalCoord.south ||
                direction == ctsAllStationSpecialValues.genericCardinalCoord.west
              ) -decimal else decimal
            }).apply(col(ctsSchemaAemetAllStation.longitud)), 6)),
        )

        formatters.foldLeft(dataframe) {
          case (accumulatedDf, (colName, transformationFunc)) =>
            accumulatedDf.withColumn(colName, transformationFunc(colName))
        }
      }
    }
  }

  object SparkQueries {

    import SparkCore.sparkSession.implicits._

    private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf

    def execute(): Unit = {
      SparkCore.startSparkSession()
      Stations.execute()
      Climograph.execute()
      SingleParamStudies.execute()
      InterestingStudies.execute()
    }

    private case class FetchAndSaveInfo(
      dataframe: DataFrame,
      pathToSave: String,
      title: String = "",
      showInfoMessage: String = ctsGlobalLogs.showInfo,
      saveInfoMessage: String = ctsGlobalLogs.saveInfo,
      saveAsJSON: Boolean = false
    )

    private def simpleFetchAndSave(
      queryTitle: String = "",
      queries: Seq[FetchAndSaveInfo],
      encloseHalfLengthStart: Int = 35
    ): Seq[DataFrame] = {
      if (queryTitle != "")
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startQuery.format(
          queryTitle
        ), encloseHalfLength = encloseHalfLengthStart)

      queries.foreach(subQuery => {
        if (subQuery.title != "")
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startSubQuery.format(
            subQuery.title
          ), encloseHalfLength = encloseHalfLengthStart + 5)

        printlnConsoleMessage(NotificationType.Information, subQuery.showInfoMessage)
        subQuery.dataframe.show()

        printlnConsoleMessage(NotificationType.Information, subQuery.saveInfoMessage.format(
          subQuery.pathToSave
        ))

        if (!subQuery.saveAsJSON)
          SparkCore.saveDataframeAsParquet(subQuery.dataframe, subQuery.pathToSave) match {
            case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
            case Right(_) => ()
          }
        else
          SparkCore.saveDataframeAsJSON(subQuery.dataframe, subQuery.pathToSave) match {
            case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
            case Right(_) => ()
          }

        if (subQuery.title != "")
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endSubQuery.format(
            subQuery.title
          ), encloseHalfLength = encloseHalfLengthStart + 5)
      })

      if (queryTitle != "")
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endQuery.format(
          queryTitle
        ), encloseHalfLength = encloseHalfLengthStart)

      queries.map(query => query.dataframe)
    }

    object Stations {
      private val ctsExecution = SparkConf.Constants.queries.execution.stationsConf
      private val ctsLogs = SparkConf.Constants.queries.log.stationsConf
      private val ctsStorage = SparkConf.Constants.queries.storage.stationsConf

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
          ctsLogs.studyName
        ))

        // Station count evolution from the start of registers
        simpleFetchAndSave(
          ctsLogs.stationCountEvolFromStart,
          List(
            FetchAndSaveInfo(
              getStationCountByColumnInLapse(
                column = (
                  year(col(ctsExecution.countEvolFromStart.param)),
                  ctsExecution.countEvolFromStart.paramSelectName
                ),
                startDate = ctsExecution.countEvolFromStart.startDate,
                endDate = Some(ctsExecution.countEvolFromStart.endDate)) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.countEvolFromStart.data
            )
          )
        )

        // Count of stations by state in 2024
        simpleFetchAndSave(
          ctsLogs.stationCountByState2024,
          List(
            FetchAndSaveInfo(
              getStationCountByColumnInLapse(
                column = (
                  col(ctsExecution.countByState2024.param),
                  ctsExecution.countByState2024.paramSelectName
                ),
                startDate = ctsExecution.countByState2024.startDate) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.countByState2024.data
            )
          )
        )

        // Count of stations by altitude in 2024
        simpleFetchAndSave(
          ctsLogs.stationCountByAltitude2024,
          List(
            FetchAndSaveInfo(
              getStationsCountByParamIntervalsInALapse(
                paramIntervals = ctsExecution.countByAltitude2024.intervals,
                startDate = ctsExecution.countByAltitude2024.startDate) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.countByAltitude2024.data
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
          ctsLogs.studyName
        ))
      }
    }

    object Climograph {
      private val ctsExecution = SparkConf.Constants.queries.execution.climographConf
      private val ctsLogs = SparkConf.Constants.queries.log.climographConf
      private val ctsStorage = SparkConf.Constants.queries.storage.climographConf

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
          ctsLogs.studyName
        ))

        ctsExecution.stationsRegistries.foreach(climateGroup => {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.startFetchingClimateGroup.format(
            climateGroup.climateGroupName
          ), encloseHalfLength = 35)

          climateGroup.climates.foreach(climateRegistry => {
            simpleFetchAndSave(
              ctsLogs.fetchingClimate.format(
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
                    ctsStorage.climograph.dataStation.format(
                      climateGroup.climateGroupName,
                      climateRegistry.climateName,
                      registry.location.replace(" ", "_")
                    ),
                    ctsLogs.fetchingClimateLocationStation.format(
                      registry.location.capitalize,
                    ),
                    saveAsJSON = true
                  ),
                  FetchAndSaveInfo(
                    getStationMonthlyAvgTempAndSumPrecInAYear(
                      registry.stationId,
                      ctsExecution.observationYear
                    ) match {
                      case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                        return
                      case Right(dataFrame: DataFrame) => dataFrame
                    },
                    ctsStorage.climograph.dataTempAndPrec.format(
                      climateGroup.climateGroupName,
                      climateRegistry.climateName,
                      registry.location.replace(" ", "_")
                    ),
                    ctsLogs.fetchingClimateLocationTempPrec.format(
                      registry.location.capitalize,
                    )
                  )
                )
              }),
              encloseHalfLengthStart = 40
            )
          })
        })

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
          ctsLogs.studyName
        ))
      }
    }

    object SingleParamStudies {
      private val ctsExecution = SparkConf.Constants.queries.execution.singleParamStudiesConf
      private val ctsLogs = SparkConf.Constants.queries.log.singleParamStudiesConf
      private val ctsStorage = SparkConf.Constants.queries.storage.singleParamStudiesConf

      def execute(): Unit = {
        ctsExecution.singleParamStudiesValues.foreach(study => {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
            study.studyParam.replace("_", " ")
          ))

          // Top 10 places with the highest temperatures in 2024
          simpleFetchAndSave(
            ctsLogs.top10Highest2024.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.top10Highest2024.startDate,
                  endDate = None) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10.dataHighest2024.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the highest eratures in the last decade
          simpleFetchAndSave(
            ctsLogs.top10HighestDecade.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.top10HighestDecade.startDate,
                  endDate = Some(ctsExecution.top10HighestDecade.endDate)) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10.dataHighestDecade.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the highest eratures from the start of registers
          simpleFetchAndSave(
            ctsLogs.top10HighestGlobal.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.top10HighestGlobal.startDate,
                  endDate = Some(ctsExecution.top10HighestGlobal.endDate)) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10.dataHighestGlobal.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the lowest eratures in 2024
          simpleFetchAndSave(
            ctsLogs.top10Lowest2024.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.top10Lowest2024.startDate,
                  endDate = None,
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10.dataLowest2024.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the lowest eratures in the last decade
          simpleFetchAndSave(
            ctsLogs.top10LowestDecade.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.top10LowestDecade.startDate,
                  endDate = Some(ctsExecution.top10LowestDecade.endDate),
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10.dataLowestDecade.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 10 places with the lowest eratures from the start of registers
          simpleFetchAndSave(
            ctsLogs.top10LowestGlobal.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.top10LowestGlobal.startDate,
                  endDate = Some(ctsExecution.top10LowestGlobal.endDate),
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10.dataLowestGlobal.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // temperature evolution from the start of registers for each state
          val regressionModelDf: DataFrame = simpleFetchAndSave(
            ctsLogs.evolFromStartForEachState.format(
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
                  ctsStorage.evolFromStartForEachState.dataStation.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSc.replace(" ", "_")
                  ),
                  ctsLogs.evolFromStartForEachStateStartStation.format(
                    registry.stateName.capitalize
                  ),
                  saveAsJSON = true
                ),
                FetchAndSaveInfo(
                  getClimateParamInALapseById(
                    registry.stationId,
                    List(
                      (study.dataframeColName, study.studyParamAbbrev)
                    ),
                    registry.startDateLatest,
                    Some(registry.endDateLatest)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataEvol.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSc.replace(" ", "_")
                  ),
                  ctsLogs.evolFromStartForEachStateStart.format(
                    registry.stateName,
                    study.studyParam.replace("_", " ")
                  )
                ),
                FetchAndSaveInfo(
                  getClimateYearlyGroupById(
                    registry.stationId,
                    List(
                      (study.dataframeColName, study.studyParamAbbrev)
                    ),
                    List(study.colAggMethod),
                    registry.startDateGlobal,
                    Some(registry.endDateGlobal)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataEvolYearlyGroup.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSc.replace(" ", "_")
                  ),
                  ctsLogs.evolFromStartForEachStateYearlyGroup.format(
                    registry.stateName,
                    study.studyParam.replace("_", " "),
                    study.colAggMethod
                  )
                ),
                FetchAndSaveInfo(
                  getStationClimateParamRegressionModelInALapse(
                    registry.stationId,
                    study.dataframeColName,
                    study.colAggMethod,
                    registry.startDateGlobal,
                    Some(registry.endDateGlobal)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataEvolRegression.format(
                    study.studyParamAbbrev,
                    registry.stateNameNoSc.replace(" ", "_")
                  ),
                  ctsLogs.evolFromStartForEachStateStartRegression.format(
                    registry.stateName.capitalize,
                    study.studyParam.replace("_", " ")
                  )
                )
              )
            })
          ).zipWithIndex.filter {
            case (_, idx) => idx % 4 == 3
          }.map(_._1).reduce(_ union _).persist(StorageLevel.MEMORY_AND_DISK_SER)

          regressionModelDf.count()

          // Top 5 highest increment of temperature
          simpleFetchAndSave(
            ctsLogs.top5HighestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(registry => registry.stationId),
                  regressionModels = regressionModelDf,
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  aggMethodName = study.colAggMethod,
                  startYear = ctsExecution.top5HighestInc.startYear,
                  endYear = ctsExecution.top5HighestInc.endYear
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top5Inc.dataHighest.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Top 5 lowest increment of temperature
          simpleFetchAndSave(
            ctsLogs.top5LowestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(registry => registry.stationId),
                  regressionModels = regressionModelDf,
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  aggMethodName = study.colAggMethod,
                  startYear = ctsExecution.top5LowestInc.startYear,
                  endYear = ctsExecution.top5LowestInc.endYear,
                  highest = false
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top5Inc.dataLowest.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          regressionModelDf.unpersist()

          // Get average temperature in 2024 for all station in the spanish continental territory
          simpleFetchAndSave(
            ctsLogs.avg2024AllStationSpainContinental.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.avg2024AllStationSpain.startDate,
                  states = Some(ctsExecution.avg2024AllStationSpain.continentalStates)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.avg2024AllStationsSpain.dataContinental.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          // Get average temperature in 2024 for all station in the canary islands territory
          simpleFetchAndSave(
            ctsLogs.avg2024AllStationSpainCanary.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  paramNameToShow = study.studyParamAbbrev,
                  startDate = ctsExecution.avg2024AllStationSpain.startDate,
                  states = Some(ctsExecution.avg2024AllStationSpain.canaryIslandStates)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.avg2024AllStationsSpain.dataCanary.format(
                  study.studyParamAbbrev
                )
              )
            )
          )

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
            study.studyParam
          ))
        })
      }
    }

    object InterestingStudies {
      private val ctsExecution = SparkConf.Constants.queries.execution.interestingStudiesConf
      private val ctsLogs = SparkConf.Constants.queries.log.interestingStudiesConf
      private val ctsStorage = SparkConf.Constants.queries.storage.interestingStudiesConf

      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
          ctsLogs.studyName
        ))

        // Precipitation and pressure evolution from the start of registers for each state
        simpleFetchAndSave(
          ctsLogs.precAndPressureEvolFromStartForEachState,
          ctsExecution.stationRegistries.flatMap(registry => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(registry.stationId) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataStation.format(
                  registry.stateNameNoSc.replace(" ", "_")
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateStartStation.format(
                  registry.stateName.capitalize
                ),
                saveAsJSON = true
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  registry.stationId,
                  ctsExecution.precAndPressEvolFromStartForEachState.climateParams,
                  registry.startDateLatest,
                  Some(registry.endDateLatest)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataEvol.format(
                  registry.stateNameNoSc.replace(" ", "_")
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateStartEvol.format(
                  registry.stateName
                )
              ),
              FetchAndSaveInfo(
                getClimateYearlyGroupById(
                  registry.stationId,
                  ctsExecution.precAndPressEvolFromStartForEachState.climateParams,
                  ctsExecution.precAndPressEvolFromStartForEachState.colAggMethods,
                  registry.startDateGlobal,
                  Some(registry.endDateGlobal)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataEvolYearlyGroup.format(
                  registry.stateNameNoSc.replace(" ", "_")
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateYearlyGroup.format(
                  registry.stateName
                )
              ),
            )
          })
        )

        // Top 10 better places for wind power generation in the last decade
        simpleFetchAndSave(
          ctsLogs.top10BetterWindPower,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10BetterWindPower.climateParams,
                startDate = ctsExecution.top10BetterWindPower.startDate,
                endDate = Some(ctsExecution.top10BetterWindPower.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataBetterWindPower
            )
          )
        )

        // Top 10 better places for sun power generation in the last decade
        simpleFetchAndSave(
          ctsLogs.top10BetterSunPower,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10BetterSunPower.climateParams,
                startDate = ctsExecution.top10BetterSunPower.startDate,
                endDate = Some(ctsExecution.top10BetterSunPower.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataBetterSunPower
            )
          )
        )

        // Top 10 places with the highest incidence of torrential rains in the last decade
        simpleFetchAndSave(
          ctsLogs.top10TorrentialRains,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10TorrentialRains.climateParams,
                startDate = ctsExecution.top10TorrentialRains.startDate,
                endDate = Some(ctsExecution.top10TorrentialRains.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataTorrentialRains
            )
          )
        )

        // Top 10 the highest incidence of storms in the last decade
        simpleFetchAndSave(
          ctsLogs.top10Storms,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10Storms.climateParams,
                startDate = ctsExecution.top10Storms.startDate,
                endDate = Some(ctsExecution.top10Storms.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataStorms
            )
          )
        )

        // Top 10 better places for agriculture in the last decade
        simpleFetchAndSave(
          ctsLogs.top10Agriculture,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10Agriculture.climateParams,
                startDate = ctsExecution.top10Agriculture.startDate,
                endDate = Some(ctsExecution.top10Agriculture.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataAgriculture
            )
          )
        )

        // Top 10 the highest incidence of droughts in the last decade
        simpleFetchAndSave(
          ctsLogs.top10Droughts,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10Droughts.climateParams,
                startDate = ctsExecution.top10Droughts.startDate,
                endDate = Some(ctsExecution.top10Droughts.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataDroughts
            )
          )
        )

        // Top 10 the highest incidence of fires in the last decade
        simpleFetchAndSave(
          ctsLogs.top10Fires,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10Fires.climateParams,
                startDate = ctsExecution.top10Fires.startDate,
                endDate = Some(ctsExecution.top10Fires.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataFires
            )
          )
        )

        // Top 10 the highest incidence of heat waves in the last decade
        simpleFetchAndSave(
          ctsLogs.top10HeatWaves,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10HeatWaves.climateParams,
                startDate = ctsExecution.top10HeatWaves.startDate,
                endDate = Some(ctsExecution.top10HeatWaves.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataHeatWaves
            )
          )
        )

        // Top 10 the highest incidence of frosts in the last decade
        simpleFetchAndSave(
          ctsLogs.top10Frosts,
          List(
            FetchAndSaveInfo(
              getTopNClimateConditionsInALapse(
                climateParams = ctsExecution.top10Frosts.climateParams,
                startDate = ctsExecution.top10Frosts.startDate,
                endDate = Some(ctsExecution.top10Frosts.endDate),
                groupByState = true
              ) match {
                case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                  return
                case Right(dataFrame: DataFrame) => dataFrame
              },
              ctsStorage.top10.dataFrosts
            )
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
          ctsLogs.studyName
        ))
      }
    }

    private def getStationInfoById(
      stationId: String
    )
    : Either[Exception, DataFrame] = {
      try {
        val allStationsDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        Right(
          allStationsDf
            .filter(col(ctsSchemaAemetAllStation.indicativo) === stationId)
            .select(
              col(ctsSchemaAemetAllStation.indicativo).alias(ctsSchemaSparkAllStation.stationId),
              col(ctsSchemaAemetAllStation.nombre).alias(ctsSchemaSparkAllStation.stationName),
              col(ctsSchemaAemetAllStation.provincia).alias(ctsSchemaSparkAllStation.state),
              col(ctsSchemaAemetAllStation.latitud).alias(ctsSchemaSparkAllStation.latDms),
              col(ctsSchemaAemetAllStation.longitud).alias(ctsSchemaSparkAllStation.longDms),
              col(ctsSchemaAemetAllStation.altitud).alias(ctsSchemaSparkAllStation.altitude)
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

        Right(
          meteoDf.filter(
            endDate match {
              case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
              case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
            }
          ).groupBy(column._1.as(column._2))
          .agg(countDistinct(col(ctsSchemaAemetAllMeteoInfo.indicativo)).as(ctsExecutionDataframeConf.specialColumns.count))
          .select(
            col(column._2),
            col(ctsExecutionDataframeConf.specialColumns.count)
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

        val param = col(paramIntervals.head._1)

        val filteredDf = meteoDf.filter(
          endDate match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }
        ).select(
          col(ctsSchemaAemetAllMeteoInfo.indicativo),
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
              lit(actualMin).as(ctsExecutionDataframeConf.specialColumns.minValue),
              lit(actualMax).as(ctsExecutionDataframeConf.specialColumns.maxValue),
              countDistinct(ctsSchemaAemetAllMeteoInfo.indicativo).as(ctsExecutionDataframeConf.specialColumns.count)
            )
          }.reduce(_.union(_)).orderBy(ctsExecutionDataframeConf.specialColumns.minValue)
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

        Right(
          meteoDf
            .filter(
              col(ctsSchemaAemetAllMeteoInfo.tMed).isNotNull &&
                col(ctsSchemaAemetAllMeteoInfo.prec).isNotNull &&
                col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId &&
                year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === observationYear
            ).groupBy(
              month(col(ctsSchemaAemetAllMeteoInfo.fecha)).as(ctsExecutionDataframeConf.specialColumns.month)
            ).agg(
              round(avg(col(ctsSchemaAemetAllMeteoInfo.tMed)), 1).as(ctsExecutionDataframeConf.specialColumns.tempMonthlyAvg),
              round(sum(col(ctsSchemaAemetAllMeteoInfo.prec)), 1).as(ctsExecutionDataframeConf.specialColumns.precMonthlySum)
            ).orderBy(col(ctsExecutionDataframeConf.specialColumns.month))
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        Right(
          meteoDf.filter(endDate match {
            case Some(endDate) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(endDate))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }).filter(col(climateParam).isNotNull)
          .groupBy(col(ctsSchemaAemetAllMeteoInfo.indicativo))
          .agg(avg(col(climateParam)).as(ctsExecutionDataframeConf.specialColumns.colAvg.format(climateParam)))
          .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
          .select(
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.indicativo
            )).alias(ctsSchemaSparkAllStation.stationId),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.nombre
            )).alias(ctsSchemaSparkAllStation.stationName),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.provincia
            )).alias(ctsSchemaSparkAllStation.state),
            col(ctsExecutionDataframeConf.specialColumns.colAvg.format(
              climateParam
            )).alias(ctsExecutionDataframeConf.specialColumns.colDailyAvg.format(paramNameToShow)),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.latitud
            )).alias(ctsSchemaSparkAllStation.latDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.longitud
            )).alias(ctsSchemaSparkAllStation.longDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.altitud
            )).alias(ctsSchemaSparkAllStation.altitude),
          ).orderBy(
            if (highest)
              col(ctsExecutionDataframeConf.specialColumns.colDailyAvg.format(paramNameToShow)).desc
            else
              col(ctsExecutionDataframeConf.specialColumns.colDailyAvg.format(paramNameToShow)).asc
          )
          .limit(topN)
          .withColumn(ctsExecutionDataframeConf.specialColumns.top, monotonically_increasing_id() + 1)
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        val filteredDf = meteoDf.filter(endDate match {
          case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
          case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
        })
        .filter(climateParams.map { case (paramName, minValue, maxValue) =>
          col(paramName).isNotNull && col(paramName).between(minValue, maxValue)
        }.reduce(_ && _))

        Right(
          (if (groupByState) {
            filteredDf
              .groupBy(ctsSchemaAemetAllMeteoInfo.provincia)
              .agg(count(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsExecutionDataframeConf.specialColumns.daysWithConds))
              .orderBy(
                if (highest)
                  col(ctsExecutionDataframeConf.specialColumns.daysWithConds).desc
                else
                  col(ctsExecutionDataframeConf.specialColumns.daysWithConds).asc
              )
              .withColumn(ctsExecutionDataframeConf.specialColumns.top, monotonically_increasing_id() + 1)
              .select(
                col(ctsSchemaAemetAllMeteoInfo.provincia).alias(ctsSchemaSparkAllStation.state),
                col(ctsExecutionDataframeConf.specialColumns.top)
              )
          } else {
            filteredDf
              .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo)
              .agg(countDistinct(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsExecutionDataframeConf.specialColumns.daysWithConds))
              .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
              .orderBy(
                if (highest)
                  col(ctsExecutionDataframeConf.specialColumns.daysWithConds).desc
                else
                  col(ctsExecutionDataframeConf.specialColumns.daysWithConds).asc
              )
              .withColumn(ctsExecutionDataframeConf.specialColumns.top, monotonically_increasing_id() + 1)
              .select(
                col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                  ctsSchemaAemetAllStation.indicativo
                )).alias(ctsSchemaSparkAllStation.stationId),
                col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                  ctsSchemaAemetAllStation.nombre
                )).alias(ctsSchemaSparkAllStation.stationName),
                col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                  ctsSchemaAemetAllStation.provincia
                )).alias(ctsSchemaSparkAllStation.state),
                col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                  ctsSchemaAemetAllStation.latitud
                )).alias(ctsSchemaSparkAllStation.latDms),
                col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                  ctsSchemaAemetAllStation.longitud
                )).alias(ctsSchemaSparkAllStation.longDms),
                col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                  ctsSchemaAemetAllStation.altitud
                )).alias(ctsSchemaSparkAllStation.altitude),
                col(ctsExecutionDataframeConf.specialColumns.top)
              )
          })
          .orderBy(col(ctsExecutionDataframeConf.specialColumns.top).asc)
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        
        Right(
          climateParams.foldLeft(
            meteoDf.filter(endDate match {
              case Some(endDate) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(endDate))
              case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
            })
          ) { (acc, climateParam) =>
            acc.filter(col(climateParam._1).isNotNull)
          }
          .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
          .select(
            Seq(col(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsExecutionDataframeConf.specialColumns.date)) ++
            climateParams.map(param => round(col(param._1), 1).alias(
              ctsExecutionDataframeConf.specialColumns.colDailyAvg.format(param._2)
            )): _*
          )
          .orderBy(ctsExecutionDataframeConf.specialColumns.date)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getClimateYearlyGroupById(
      stationId: String,
      climateParams: Seq[(String, String)],
      aggMethodNames: Seq[String],
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        require(climateParams.length == aggMethodNames.length)

        val meteoDf = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

        val filteredDf = climateParams.foldLeft(
          meteoDf
            .filter(endDate match {
              case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
              case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
            })
            .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
            .withColumn(ctsExecutionDataframeConf.specialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
        ) { (acc, climateParam) =>
          acc.filter(col(climateParam._1).isNotNull)
        }

        val aggColumns = climateParams.zip(aggMethodNames).map {
          case ((colName, paramName), op) =>
            val baseCol = col(colName)
            val aggCol = op.toLowerCase match {
              case ctsExecutionGlobalConf.groupMethods.avg => round(avg(baseCol), 1)
              case ctsExecutionGlobalConf.groupMethods.sum => round(sum(baseCol), 1)
              case ctsExecutionGlobalConf.groupMethods.min => round(min(baseCol), 1)
              case ctsExecutionGlobalConf.groupMethods.max => round(max(baseCol), 1)
              case other => throw new IllegalArgumentException(other)
            }
            aggCol.alias(ctsExecutionDataframeConf.specialColumns.colYearlyGrouped.format(paramName, op))
        }

        val resultDf = filteredDf
          .groupBy(ctsExecutionDataframeConf.specialColumns.year)
          .agg(aggColumns.head, aggColumns.tail: _*)
          .orderBy(ctsExecutionDataframeConf.specialColumns.year)

        Right(resultDf)

      } catch {
        case ex: Exception => Left(ex)
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
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        Right(
          meteoDf.filter(endDate match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }).filter(states match {
            case Some(stateList) => col(ctsSchemaAemetAllMeteoInfo.provincia).isin(stateList: _*)
            case None => lit(true)
          }).filter(col(climateParam).isNotNull)
          .groupBy(col(ctsSchemaAemetAllMeteoInfo.indicativo))
          .agg(avg(col(climateParam)).as(ctsExecutionDataframeConf.specialColumns.colAvg.format(climateParam)))
          .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
          .select(
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.indicativo
            )).alias(ctsSchemaSparkAllStation.stationId),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.nombre
            )).alias(ctsSchemaSparkAllStation.stationName),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.provincia
            )).alias(ctsSchemaSparkAllStation.state),
            round(
              col(ctsExecutionDataframeConf.specialColumns.colAvg.format(climateParam)), 1
            ).alias(ctsExecutionDataframeConf.specialColumns.colDailyAvg.format(paramNameToShow)),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.latitud
            )).alias(ctsSchemaSparkAllStation.latDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.longitud
            )).alias(ctsSchemaSparkAllStation.longDms),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaSparkAllStation.latDec
            )).alias(ctsSchemaSparkAllStation.latDec),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaSparkAllStation.longDec
            )).alias(ctsSchemaSparkAllStation.longDec),
            col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
              ctsSchemaAemetAllStation.altitud
            )).alias(ctsSchemaSparkAllStation.altitude),
          )
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getStationClimateParamRegressionModelInALapse(
      stationId: String,
      climateParam: String,
      aggMethodName: String,
      startYear: String,
      endYear: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)

        val aggMethod: Column => Column = aggMethodName.toLowerCase match {
          case ctsExecutionGlobalConf.groupMethods.avg => avg
          case ctsExecutionGlobalConf.groupMethods.sum => sum
          case ctsExecutionGlobalConf.groupMethods.min => min
          case ctsExecutionGlobalConf.groupMethods.max => max
          case other => throw new IllegalArgumentException(other)
        }

        val filteredDF = meteoDf
          .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
          .filter(endYear match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startYear), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startYear
          })
          .filter(col(climateParam).isNotNull)
          .withColumn(ctsExecutionDataframeConf.specialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
          .groupBy(ctsExecutionDataframeConf.specialColumns.year)
          .agg(aggMethod(col(climateParam)).as(ctsExecutionDataframeConf.specialColumns.climateParamAvg))
          .select(
            col(ctsExecutionDataframeConf.specialColumns.year).alias(ctsExecutionDataframeConf.specialColumns.x),
            col(ctsExecutionDataframeConf.specialColumns.climateParamAvg).alias(ctsExecutionDataframeConf.specialColumns.y)
          )

        val (meanX, meanY) = filteredDF.agg(
          avg(ctsExecutionDataframeConf.specialColumns.x),
          avg(ctsExecutionDataframeConf.specialColumns.y)
        ).as[(Double, Double)].first() match {
          case (mx, my) => (mx, my)
        }

        val (beta1, beta0) = filteredDF.withColumn(
            ctsExecutionDataframeConf.specialColumns.xDiff, col(ctsExecutionDataframeConf.specialColumns.x) - meanX
          )
          .withColumn(
            ctsExecutionDataframeConf.specialColumns.yDiff, col(ctsExecutionDataframeConf.specialColumns.y) - meanY
          )
          .agg(
            sum(
              col(ctsExecutionDataframeConf.specialColumns.xDiff) * col(ctsExecutionDataframeConf.specialColumns.yDiff)
            ).as(ctsExecutionDataframeConf.specialColumns.num),
            sum(
              col(ctsExecutionDataframeConf.specialColumns.xDiff) * col(ctsExecutionDataframeConf.specialColumns.xDiff)
            ).as(ctsExecutionDataframeConf.specialColumns.den)
          ).as[(Double, Double)].first() match {
          case (num, den) =>
            val b1 = num / den
            val b0 = meanY - b1 * meanX
            (b1, b0)
        }

        Right(Seq((stationId, beta1, beta0)).toDF(
          ctsSchemaSparkMeteo.stationId,
          ctsExecutionDataframeConf.specialColumns.beta1,
          ctsExecutionDataframeConf.specialColumns.beta0
        ))
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    private def getTopNClimateParamIncrementInAYearLapse(
      stationIds: Seq[String],
      regressionModels: DataFrame,
      climateParam: String,
      paramNameToShow: String,
      aggMethodName: String,
      startYear: Int,
      endYear: Int,
      highest: Boolean = true,
      topN: Int = 5
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        val aggMethod: Column => Column = aggMethodName.toLowerCase match {
          case ctsExecutionGlobalConf.groupMethods.avg => avg
          case ctsExecutionGlobalConf.groupMethods.sum => sum
          case ctsExecutionGlobalConf.groupMethods.min => min
          case ctsExecutionGlobalConf.groupMethods.max => max
          case other => throw new IllegalArgumentException(other)
        }

        Right(
          regressionModels
            .filter(col(ctsSchemaSparkMeteo.stationId).isin(stationIds: _*))
            .withColumn(
              ctsExecutionDataframeConf.specialColumns.inc,
              (col(ctsExecutionDataframeConf.specialColumns.beta1) * lit(endYear) + col(ctsExecutionDataframeConf.specialColumns.beta0)) - (col(ctsExecutionDataframeConf.specialColumns.beta1) * lit(startYear) + col(ctsExecutionDataframeConf.specialColumns.beta0))
            )
            .select(col(ctsSchemaSparkMeteo.stationId).as(ctsSchemaAemetAllMeteoInfo.indicativo), col(ctsExecutionDataframeConf.specialColumns.inc))
            .join(
              meteoDf
                .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo).isin(stationIds: _*))
                .filter(col(climateParam).isNotNull)
                .withColumn(ctsExecutionDataframeConf.specialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
                .filter(col(ctsExecutionDataframeConf.specialColumns.year).between(startYear, endYear))
                .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo, ctsExecutionDataframeConf.specialColumns.year)
                .agg(aggMethod(col(climateParam)).as(ctsExecutionDataframeConf.specialColumns.colYearlyGrouped.format(climateParam, aggMethodName)))
                .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo)
                .agg(avg(col(ctsExecutionDataframeConf.specialColumns.colYearlyGrouped.format(climateParam, aggMethodName))).as(ctsExecutionDataframeConf.specialColumns.globalColYearlyAvg.format(climateParam))),
              Seq(ctsSchemaAemetAllMeteoInfo.indicativo),
              "inner"
            )
            .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
            .withColumn(ctsExecutionDataframeConf.specialColumns.incPerc, col(ctsExecutionDataframeConf.specialColumns.inc) / col(ctsExecutionDataframeConf.specialColumns.globalColYearlyAvg.format(climateParam, aggMethodName)) * 100)
            .select(
              round(col(ctsExecutionDataframeConf.specialColumns.inc), 1).alias(ctsExecutionDataframeConf.specialColumns.inc),
              round(col(ctsExecutionDataframeConf.specialColumns.incPerc), 1).alias(ctsExecutionDataframeConf.specialColumns.incPerc),
              round(col(ctsExecutionDataframeConf.specialColumns.globalColYearlyAvg.format(climateParam, aggMethodName)), 1).alias(ctsExecutionDataframeConf.specialColumns.globalColYearlyAvg.format(paramNameToShow, aggMethodName)),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.indicativo
              )).alias(ctsSchemaSparkAllStation.stationId),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.nombre
              )).alias(ctsSchemaSparkAllStation.stationName),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.provincia
              )).alias(ctsSchemaSparkAllStation.state),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.latitud
              )).alias(ctsSchemaSparkAllStation.latDms),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.longitud
              )).alias(ctsSchemaSparkAllStation.longDms),
              col(ctsExecutionDataframeConf.allStationsDf.aliasCol.format(
                ctsSchemaAemetAllStation.altitud
              )).alias(ctsSchemaSparkAllStation.altitude),
            )
            .orderBy(
              if (highest)
                col(ctsExecutionDataframeConf.specialColumns.inc).desc
              else
                col(ctsExecutionDataframeConf.specialColumns.inc).asc
            )
            .limit(topN)
            .withColumn(ctsExecutionDataframeConf.specialColumns.top, monotonically_increasing_id() + 1)
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
