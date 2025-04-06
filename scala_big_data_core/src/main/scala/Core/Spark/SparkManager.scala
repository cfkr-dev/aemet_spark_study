package Core.Spark

import Utils.JSONUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import Config.ConstantsV2._
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}

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

      spark
    }

    private def createDataframeFromJSON(session: SparkSession,
                                        sourcePath: String,
                                        metadataPath: String,
                                        schemaFunction: ujson.Value => StructType): Either[Exception, DataFrame] = {
      Right(session
        .read.format("json")
        .option("multiline", value = true)
        .schema(
          schemaFunction(
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

    private def createDataframeSchemaAemet(metadataJSON: ujson.Value): StructType = {
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
      val allMeteoInfo: DataFrame = createDataframeFromJSON(
        sparkSession,
        ctsStorageDataAemetAllMeteoInfo.Dirs.dataRegistry,
        ctsStorageDataAemetAllMeteoInfo.FilePaths.metadata,
        createDataframeSchemaAemet
      ) match {
        case Left(exception) => throw exception
        case Right(df) => df.union(createDataframeFromJSON(
          sparkSession,
          ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.Dirs.dataRegistry,
          ctsStorageDataIfapaAemetFormatSingleStationMeteoInfo.FilePaths.metadata,
          createDataframeSchemaAemet
        ) match {
          case Left(exception) => throw exception
          case Right(df) => df
        })
      }

      val allStations: DataFrame = createDataframeFromJSON(
        sparkSession,
        ctsStorageDataAemetAllStationInfo.Dirs.dataRegistry,
        ctsStorageDataAemetAllStationInfo.FilePaths.metadata,
        createDataframeSchemaAemet
      ) match {
        case Left(exception) => throw exception
        case Right(df) => df.union(createDataframeFromJSON(
          sparkSession,
          ctsStorageDataIfapaAemetFormatSingleStationInfo.Dirs.dataRegistry,
          ctsStorageDataIfapaAemetFormatSingleStationInfo.FilePaths.metadata,
          createDataframeSchemaAemet
        ) match {
          case Left(exception) => throw exception
          case Right(df) => df
        })
      }
    }
  }

  object SparkQueries {
    import SparkCore.sparkSession.implicits._

    private val ctsSparkQueriesGlobal = Spark.Queries.Global
    private val ctsLogsSparkQueriesStudiesGlobal = Logs.SparkQueries.Studies.Global

    object Climograph {
      private val ctsSparkQueriesClimograph = Spark.Queries.Climograph
      private val ctsLogsSparkQueriesStudiesClimograph = Logs.SparkQueries.Studies.Climograph
      private val ctsStorageDataSparkClimograph = Storage.DataSpark.Climograph

      def execute(): Unit = {
        def fetchAndSaveClimate(climateName: String, locationToStationId: Map[String, String], observationYear: Int, locationToPathToSave: Map[String, Map[String,String]]): Unit = {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.startFetchingClimate.format(
            climateName
          ), encloseHalfLength = 40)

          case class FetchResult(dfResult: DataFrame, pathToSave: String, infoTextShow: String, infoTextSave: String)
          case class FetchResultGroup(tempAndPrec: FetchResult, stationInfo: FetchResult)

          val showFetchResultAndSave: (FetchResult => Unit) = { fetchResult =>
            printlnConsoleMessage(NotificationType.Information, fetchResult.infoTextShow)
            fetchResult.dfResult.show()

            printlnConsoleMessage(NotificationType.Information, fetchResult.infoTextSave)
            SparkCore.saveDataframeAsParquet(fetchResult.dfResult, fetchResult.pathToSave) match {
              case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
              case Right(_) => ()
            }
          }

          locationToStationId.map {
            case (key, value) =>
              FetchResultGroup(
                FetchResult(
                  getStationMonthlyAvgTempAndPrecInAYear(value, observationYear) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  locationToPathToSave(key)(ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id),
                  ctsLogsSparkQueriesStudiesClimograph.Execution.infoShowDataframeTempAndPrecInfo.format(key.capitalize),
                  ctsLogsSparkQueriesStudiesClimograph.Execution.infoSaveDataframeTempAndPrecInfo.format(
                    key.capitalize,
                    locationToPathToSave(key)(ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id)
                  ),
                ),
                FetchResult(
                  getStationInfoById(value) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  locationToPathToSave(key)(ctsSparkQueriesGlobal.Methods.GetStationInfoById.id),
                  ctsLogsSparkQueriesStudiesClimograph.Execution.infoShowDataframeStationInfo.format(key.capitalize),
                  ctsLogsSparkQueriesStudiesClimograph.Execution.infoSaveDataframeStationInfo.format(
                    key.capitalize,
                    locationToPathToSave(key)(ctsSparkQueriesGlobal.Methods.GetStationInfoById.id)
                  ),
                )
              )
          }.toList.foreach(element => {
            showFetchResultAndSave(element.tempAndPrec)
            showFetchResultAndSave(element.stationInfo)
          })

          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.endFetchingClimate.format(
            climateName
          ), encloseHalfLength = 40)
        }

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.startStudy.format(
          ctsLogsSparkQueriesStudiesClimograph.studyName
        ))

        // --- Arid climates ---
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.startFetchingClimateGroup.format(
          ctsSparkQueriesGlobal.Climates.Arid.climateGroupName
        ), encloseHalfLength = 35)

        // - BWh -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Arid.Climates.BWh.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Arid.BWh.peninsula,
            ctsSparkQueriesClimograph.Locations.canaryIslands -> ctsSparkQueriesClimograph.Climates.Arid.BWh.canaryIslands,
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BWh.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BWh.Peninsula.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.canaryIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BWh.Canary.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BWh.Canary.Dirs.stationResult
            ),
          )
        )

        // - BWk -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Arid.Climates.BWk.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Arid.BWk.peninsula
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BWk.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BWk.Peninsula.Dirs.stationResult
            )
          )
        )

        // - BSh -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Arid.Climates.BSh.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Arid.BSh.peninsula,
            ctsSparkQueriesClimograph.Locations.canaryIslands -> ctsSparkQueriesClimograph.Climates.Arid.BSh.canaryIslands,
            ctsSparkQueriesClimograph.Locations.balearIslands -> ctsSparkQueriesClimograph.Climates.Arid.BSh.balearIslands,
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BSh.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BSh.Peninsula.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.canaryIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BSh.Canary.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BSh.Canary.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.balearIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BSh.Balear.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BSh.Balear.Dirs.stationResult
            ),
          )
        )

        // - BSk -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Arid.Climates.BSk.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Arid.BSk.peninsula,
            ctsSparkQueriesClimograph.Locations.canaryIslands -> ctsSparkQueriesClimograph.Climates.Arid.BSk.canaryIslands,
            ctsSparkQueriesClimograph.Locations.balearIslands -> ctsSparkQueriesClimograph.Climates.Arid.BSk.balearIslands,
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BSk.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BSk.Peninsula.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.canaryIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BSk.Canary.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BSk.Canary.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.balearIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Arid.BSk.Balear.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Arid.BSk.Balear.Dirs.stationResult
            ),
          )
        )

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.endFetchingClimateGroup.format(
          ctsSparkQueriesGlobal.Climates.Arid.climateGroupName
        ), encloseHalfLength = 35)

        // --- Warm climates ---
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.startFetchingClimateGroup.format(
          ctsSparkQueriesGlobal.Climates.Warm.climateGroupName
        ), encloseHalfLength = 35)

        // - Csa -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Warm.Climates.Csa.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Warm.Csa.peninsula,
            ctsSparkQueriesClimograph.Locations.canaryIslands -> ctsSparkQueriesClimograph.Climates.Warm.Csa.canaryIslands,
            ctsSparkQueriesClimograph.Locations.balearIslands -> ctsSparkQueriesClimograph.Climates.Warm.Csa.balearIslands,
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Csa.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Csa.Peninsula.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.canaryIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Csa.Canary.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Csa.Canary.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.balearIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Csa.Balear.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Csa.Balear.Dirs.stationResult
            ),
          )
        )
        
        // - Csb -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Warm.Climates.Csb.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Warm.Csb.peninsula,
            ctsSparkQueriesClimograph.Locations.canaryIslands -> ctsSparkQueriesClimograph.Climates.Warm.Csb.canaryIslands,
            ctsSparkQueriesClimograph.Locations.balearIslands -> ctsSparkQueriesClimograph.Climates.Warm.Csb.balearIslands,
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Csb.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Csb.Peninsula.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.canaryIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Csb.Canary.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Csb.Canary.Dirs.stationResult
            ),
            ctsSparkQueriesClimograph.Locations.balearIslands -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Csb.Balear.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Csb.Balear.Dirs.stationResult
            ),
          )
        )
        
        // - Cfa -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Warm.Climates.Cfa.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Warm.Cfa.peninsula
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Cfa.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Cfa.Peninsula.Dirs.stationResult
            )
          )
        )
        
        // - Cfb -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Warm.Climates.Cfb.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Warm.Cfb.peninsula
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Warm.Cfb.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Warm.Cfb.Peninsula.Dirs.stationResult
            )
          )
        )
        
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.endFetchingClimateGroup.format(
          ctsSparkQueriesGlobal.Climates.Warm.climateGroupName
        ), encloseHalfLength = 35)

        // --- Cold climates ---
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.startFetchingClimateGroup.format(
          ctsSparkQueriesGlobal.Climates.Cold.climateGroupName
        ), encloseHalfLength = 35)

        // - Dsb -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Cold.Climates.Dsb.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Cold.Dsb.peninsula
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Cold.Dsb.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Cold.Dsb.Peninsula.Dirs.stationResult
            )
          )
        )
        
        // - Dfb -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Cold.Climates.Dfb.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Cold.Dfb.peninsula
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Cold.Dfb.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Cold.Dfb.Peninsula.Dirs.stationResult
            )
          )
        )
        
        // - Dfc -
        fetchAndSaveClimate(
          climateName = ctsSparkQueriesGlobal.Climates.Cold.Climates.Dfc.climateName,
          locationToStationId = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> ctsSparkQueriesClimograph.Climates.Cold.Dfc.peninsula
          ),
          observationYear = ctsSparkQueriesClimograph.observationYear,
          locationToPathToSave = Map(
            ctsSparkQueriesClimograph.Locations.peninsula -> Map(
              ctsSparkQueriesGlobal.Methods.GetStationMonthlyAvgTempAndPrecInAYear.id -> ctsStorageDataSparkClimograph.Cold.Dfc.Peninsula.Dirs.tempAndPrecResult,
              ctsSparkQueriesGlobal.Methods.GetStationInfoById.id -> ctsStorageDataSparkClimograph.Cold.Dfc.Peninsula.Dirs.stationResult
            )
          )
        )
        
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesClimograph.Execution.endFetchingClimateGroup.format(
          ctsSparkQueriesGlobal.Climates.Cold.climateGroupName
        ), encloseHalfLength = 35)

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogsSparkQueriesStudiesGlobal.endStudy.format(
          ctsLogsSparkQueriesStudiesClimograph.studyName
        ))
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

    private def getStationMonthlyAvgTempAndPrecInAYear(stationId: String, observationYear: Int): Either[Exception, DataFrame] = {
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
          ).cache()

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
  }
}
