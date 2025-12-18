package Spark.Core

import Spark.Config.{GlobalConf, SparkConf}
import Utils.ChronoUtils
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.Storage.Core.Storage
import Utils.Storage.JSON.JSONStorageBackend.writeJSON
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import ujson.read

/**
 * SparkManager is the top-level coordinator for Spark session lifecycle
 * and query-driven studies in the project.
 *
 * Responsibilities:
 * - Initialize and configure Spark via `SparkCore`.
 * - Expose query workflows grouped under `SparkQueries` which build on the
 *   preformatted DataFrames available in `SparkCore.dataframes`.
 * - Provide a small set of utility methods to persist DataFrames using the
 *   configured storage backend.
 *
 * This object is designed for programmatic invocation (for example, from a
 * main runner) and orchestrates the main study executions.
 */
object SparkManager {
  private val ctsExecutionDataframeConf = SparkConf.Constants.init.execution.dataframeConf
  private val ctsSchemaAemetAllMeteoInfo = GlobalConf.Constants.schema.aemetConf.allMeteoInfo
  private val ctsSchemaAemetAllStation = GlobalConf.Constants.schema.aemetConf.allStationInfo
  private val ctsSchemaSparkAllStation = GlobalConf.Constants.schema.sparkConf.stationsDf
  private val ctsSpecialColumns = GlobalConf.Constants.schema.sparkConf.specialColumns
  private val ctsGroupMethods = GlobalConf.Constants.schema.sparkConf.groupMethods
  private val ctsAllStationSpecialValues = ctsExecutionDataframeConf.allStationsDf.specialValues
  private val ctsGlobalInit = GlobalConf.Constants.init
  private val ctsGlobalUtils = GlobalConf.Constants.utils

  private val chronometer = ChronoUtils.Chronometer()

  private implicit val dataStorage: Storage = GlobalConf.Constants.dataStorage

  /**
   * SparkCore provides the internal SparkSession management and prepared
   * DataFrames used by higher-level query workflows.
   *
   * Responsibilities:
   * - Create and configure the SparkSession according to runtime environment
   *   (EMR or local, with optional S3 endpoint mock settings).
   * - Resolve and expose the resolved storage prefix used for reads/writes.
   * - Read and normalize raw Parquet inputs into two cached DataFrames:
   *   `allMeteoInfo` and `allStations`.
   *
   * The object is private to keep session and storage configuration
   * encapsulated; other parts of the application should access its
   * formatted DataFrames and persistence helpers rather than managing the
   * SparkSession directly.
   */
  private object SparkCore {
    private val ctsExecutionSessionConf = SparkConf.Constants.init.execution.sessionConf
    private val ctsInitLogs = SparkConf.Constants.init.log
    private val ctsAllMeteoInfoSpecialValues = ctsExecutionDataframeConf.allMeteoInfoDf.specialValues

    val showResults: Boolean = ctsExecutionSessionConf.showResults

    val sparkSession: SparkSession = createSparkSession(
      ctsExecutionSessionConf.sessionName,
      ctsExecutionSessionConf.sessionMaster,
      ctsExecutionSessionConf.sessionLogLevel
    )

    private val storagePrefix: String = setStoragePrefix(ctsGlobalInit.environmentVars.values.storagePrefix)

    /**
     * Start the Spark session and perform a lightweight initialization check.
     *
     * This method starts an internal chronometer, prints session configuration,
     * and triggers evaluation of key cached DataFrames to ensure they are
     * materialized and the environment is ready for downstream queries.
     */
    def startSparkSession(): Unit = {
      chronometer.start()
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsInitLogs.sessionConf.startSparkSessionCheckStats.format(
        if (ctsExecutionSessionConf.runningInEmr) {
          ctsGlobalUtils.errors.notAvailable
        } else {
          ctsExecutionSessionConf.sessionStatsUrl
        }
      ))
      SparkCore.sparkSession.conf.getAll.foreach {case (k, v) => printlnConsoleMessage(NotificationType.Information, s"$k = $v")}
      SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName).count()
      SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName).count()
    }

    /**
     * Stop the Spark session, print timing information, and perform a
     * graceful shutdown.
     *
     * The method logs elapsed time measured by the internal chronometer and
     * waits a small configured pause before closing the SparkSession.
     */
    def endSparkSession(): Unit = {
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsInitLogs.sessionConf.endSparkSession)
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.chrono.chronoResult.format(chronometer.stop()))
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalUtils.betweenStages.infoText.format(ctsGlobalUtils.betweenStages.millisBetweenStages / 1000))
      Thread.sleep(ctsGlobalUtils.betweenStages.millisBetweenStages)
      SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName).unpersist()
      SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName).unpersist()
      sparkSession.stop()
      printlnConsoleEnclosedMessage(NotificationType.Information, ctsInitLogs.sessionConf.endSparkSessionClosed)
    }

    /**
     * Persist a DataFrame as Parquet using the configured storage prefix.
     *
     * This helper wraps the DataFrame write call and returns an Either with
     * the destination path on success or the thrown Exception on failure.
     *
     * @param dataframe DataFrame to persist
     * @param path destination path relative to the storage prefix
     * @return Right(path) on success, Left(Exception) on failure
     */
    def saveDataframeAsParquet(dataframe: DataFrame, path: String): Either[Exception, String] = {
      try {
        dataframe.write
          .mode(SaveMode.Overwrite)
          .parquet(storagePrefix + path)

        Right(path)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Persist a DataFrame as a single JSON file using the project's JSON
     * storage helper.
     *
     * This converts the DataFrame to JSON strings and delegates writing to
     * the JSON backend. Returns an Either signaling success or failure.
     *
     * @param dataframe DataFrame to persist as JSON
     * @param path destination path for the JSON file
     * @return Right(path) on success, Left(Exception) on failure
     */
    def saveDataframeAsJSON(dataframe: DataFrame, path: String): Either[Exception, String] = {
      try {
        writeJSON(
          path,
          read(dataframe.toJSON.collect().mkString(","))
        )

        Right(path)
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Create and configure a SparkSession instance.
     *
     * Behavior:
     * - When running in EMR (controlled by configuration), returns an
     *   application SparkSession using getOrCreate().
     * - Otherwise, optionally applies S3 mock configurations (access/secret
     *   key, endpoint, path-style access, and SSL) when an S3 endpoint is
     *   provided in the environment configuration.
     * - Sets the Spark log level and clears the catalog cache before
     *   returning the session.
     *
     * @param name application name for the SparkSession
     * @param master master URL (e.g., local[*] or spark://...)
     * @param logLevel logging level for the Spark context
     * @return initialized and configured SparkSession
     */
    private def createSparkSession(name: String, master: String, logLevel: String): SparkSession = {
      val spark = if (ctsExecutionSessionConf.runningInEmr) {
        SparkSession.builder()
          .appName(name)
          .getOrCreate()
      } else {
        ctsGlobalInit.environmentVars.values.awsS3Endpoint match {
          case Some(endpoint) => SparkSession.builder()
            .appName(name)
            .master(master)
            .config(ctsExecutionSessionConf.s3AMockConfiguration.names.accessKey, ctsExecutionSessionConf.s3AMockConfiguration.values.accessKey)
            .config(ctsExecutionSessionConf.s3AMockConfiguration.names.secretKey, ctsExecutionSessionConf.s3AMockConfiguration.values.secretKey)
            .config(ctsExecutionSessionConf.s3AMockConfiguration.names.endpoint, endpoint)
            .config(ctsExecutionSessionConf.s3AMockConfiguration.names.pathStyleAccess, ctsExecutionSessionConf.s3AMockConfiguration.values.pathStyleAccess)
            .config(ctsExecutionSessionConf.s3AMockConfiguration.names.sslEnabled, ctsExecutionSessionConf.s3AMockConfiguration.values.sslEnabled)
            .getOrCreate()
          case None => SparkSession.builder()
            .appName(name)
            .master(master)
            .getOrCreate()
        }
      }

      spark.sparkContext
        .setLogLevel(logLevel)

      spark.catalog.clearCache()

      spark
    }

    /**
     * Validate and resolve the configured storage prefix.
     *
     * This helper reads the optional storage prefix from configuration, throws
     * an Exception if it is not present, and determines whether the prefix
     * refers to an S3 bucket or a local path by delegating to the project's
     * storage backend. When S3 is used, the returned prefix is in the form
     * `s3a://<bucket>`; otherwise the local path fragment is returned.
     *
     * @param prefix optional configured storage prefix (usually from env)
     * @return resolved storage prefix string (S3 or local path)
     * @throws Exception when the prefix is not defined in the environment
     */
    private def setStoragePrefix(prefix: Option[String]): String = {
      val checkedPrefix = prefix.getOrElse(
        throw new Exception(ctsGlobalUtils.errors.environmentVariableNotFound.format(
          ctsGlobalInit.environmentVars.names.storagePrefix
        ))
      )

      val (bucket, rest, isS3) = dataStorage.selectS3orLocal(checkedPrefix + "/")

      if (isS3) s"s3a://$bucket" else rest
    }

    /**
     * Create a DataFrame by reading Parquet from the configured storage
     * location.
     *
     * The function composes the resolved `storagePrefix` with `sourcePath` and
     * delegates to Spark to read the Parquet files. It returns an Either that
     * contains the DataFrame on success or an Exception on failure.
     *
     * @param session active SparkSession used for reading
     * @param sourcePath relative path (under the storage prefix) to the Parquet data
     * @return Right(DataFrame) on success, Left(Exception) on failure
     */
    private def createDataframeFromParquet(
      session: SparkSession,
      sourcePath: String
    ): Either[Exception, DataFrame] = {
      Right(session
        .read
        .parquet(storagePrefix + sourcePath)
      )
    }

    /**
     * Collection of prebuilt, formatted DataFrames used by queries.
     *
     * The `dataframes` object composes DataFrames by reading Parquet inputs
     * and applying formatting functions to normalize types and values.
     */
    object dataframes {
      private val ctsStorageAemet = SparkConf.Constants.init.storage.aemetConf
      private val ctsStorageIfapaAemetFormat = SparkConf.Constants.init.storage.ifapaAemetFormatConf
      private val ctsUtils = GlobalConf.Constants.utils

      val allMeteoInfo: DataFrame =
        formatAllMeteoInfoDataframe(
          createDataframeFromParquet(
            sparkSession,
            ctsStorageAemet.allMeteoInfo.dirs.data
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df.union(createDataframeFromParquet(
              sparkSession,
              ctsStorageIfapaAemetFormat.singleStationMeteoInfo.dirs.data
            ) match {
              case Left(exception) => throw exception
              case Right(df) => df
            })
          }
        ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val allStations: DataFrame =
        formatAllStationsDataframe(
          createDataframeFromParquet(
            sparkSession,
            ctsStorageAemet.allStationInfo.dirs.data
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df.union(createDataframeFromParquet(
              sparkSession,
              ctsStorageIfapaAemetFormat.singleStationInfo.dirs.data
            ) match {
              case Left(exception) => throw exception
              case Right(df) => df
            })
          }
        ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      /**
       * Format the merged AEMET/IFAPA meteorological DataFrame.
       *
       * This function applies a set of column-specific transformations to
       * convert raw string fields into typed columns (dates, timestamps,
       * integers, doubles) and to handle special placeholder values (e.g.
       * "varias", no-data markers). Use this to obtain a typed DataFrame
       * ready for analytical queries.
       *
       * @param dataframe raw dataframe read from Parquet with string values
       * @return transformed DataFrame with typed and normalized columns
       */
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

      /**
       * Format the stations DataFrame (convert coordinates and normalize
       * textual fields).
       *
       * Responsibilities:
       * - Normalize province names using configured special-values mapping.
       * - Cast altitude to integer type.
       * - Convert DMS coordinate strings (degrees/minutes/seconds with
       *   cardinal direction) into decimal degrees.
       *
       * @param dataframe raw stations DataFrame read from Parquet
       * @return DataFrame with normalized station fields and decimal coordinates
       */
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

  /**
   * SparkQueries groups query-oriented workflows that produce final datasets
   * (studies, plots, and aggregations) based on the preformatted DataFrames in
   * `SparkCore.dataframes`.
   *
   * Use `execute()` to run the full set of queries in sequence (it starts and
   * stops the Spark session around the set of studies).
   */
  object SparkQueries {

    import SparkCore.sparkSession.implicits._

    private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf

    /**
     * Execute the complete set of query-driven studies.
     *
     * Behavior:
     * - Starts the Spark session (initialization and sanity checks).
     * - Executes each study (Stations, Climograph, SingleParamStudies,
     *   InterestingStudies) in sequence.
     * - Stops the Spark session when finished.
     */
    def execute(): Unit = {
      SparkCore.startSparkSession()
      Stations.execute()
      Climograph.execute()
      SingleParamStudies.execute()
      InterestingStudies.execute()
      SparkCore.endSparkSession()
    }

    /**
     * Small container describing a query to run and where to persist it.
     *
     * @param dataframe DataFrame result of the query
     * @param pathToSave destination path to store the DataFrame
     * @param title optional human-readable title for logging
     * @param showInfoMessage message shown before printing the DataFrame
     * @param saveInfoMessage message template shown before saving
     * @param saveAsJSON whether to save the result as JSON instead of Parquet
     */
    private case class FetchAndSaveInfo(
      dataframe: DataFrame,
      pathToSave: String,
      title: String = "",
      showInfoMessage: String = ctsGlobalLogs.showInfo,
      saveInfoMessage: String = ctsGlobalLogs.saveInfo,
      saveAsJSON: Boolean = false
    )

    /**
     * Simple helper to run a group of queries, display and persist their
     * results.
     *
     * This function handles logging for query start/end, sub-query messages,
     * printing DataFrame samples, and saving either as Parquet or JSON using
     * the `SparkCore` helpers.
     *
     * @param queryTitle optional title for the overall query batch
     * @param queries list of FetchAndSaveInfo describing sub-queries
     * @param encloseHalfLengthStart formatting width for enclosed console messages
     * @return sequence of DataFrames produced by the provided queries
     */
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

        if (SparkCore.showResults) {
          printlnConsoleMessage(NotificationType.Information, subQuery.showInfoMessage)
          subQuery.dataframe.show()
        }

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

    /**
     * Stations study: executes several simple queries related to station counts
     * and distributions, persisting results for downstream visualization.
     */
    private object Stations {
      private val ctsExecution = SparkConf.Constants.queries.execution.stationsConf
      private val ctsLogs = SparkConf.Constants.queries.log.stationsConf
      private val ctsStorage = SparkConf.Constants.queries.storage.stationsConf

      /**
       * Execute the Stations study: run station-related queries and persist results.
       */
      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
          ctsLogs.studyName
        ))

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

    /**
     * Climograph study: groups stations into climate groups and produces
     * station-level and monthly aggregates used for climographs.
     */
    private object Climograph {
      private val ctsExecution = SparkConf.Constants.queries.execution.climographConf
      private val ctsLogs = SparkConf.Constants.queries.log.climographConf
      private val ctsStorage = SparkConf.Constants.queries.storage.climographConf

      /**
       * Execute the Climograph study: produce station-level and monthly aggregates
       * used for climographs and persist the outputs.
       */
      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
          ctsLogs.studyName
        ))

        ctsExecution.stationsRecords.foreach(climateGroup => {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsLogs.startFetchingClimateGroup.format(
            climateGroup.climateGroupName
          ), encloseHalfLength = 35)

          climateGroup.climates.foreach(climateRecord => {
            simpleFetchAndSave(
              ctsLogs.fetchingClimate.format(
                climateRecord.climateName
              ),
              climateRecord.records.flatMap(record => {
                List(
                  FetchAndSaveInfo(
                    getStationInfoById(record.stationId) match {
                      case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                        return
                      case Right(dataFrame: DataFrame) => dataFrame
                    },
                    ctsStorage.climograph.dataStation.format(
                      climateGroup.climateGroupName,
                      climateRecord.climateName,
                      record.location.replace(" ", "_")
                    ),
                    ctsLogs.fetchingClimateLocationStation.format(
                      record.location.capitalize,
                    ),
                    saveAsJSON = true
                  ),
                  FetchAndSaveInfo(
                    getStationMonthlyAvgTempAndSumPrecInAYear(
                      record.stationId,
                      (ctsExecution.studyParamNames.temperature, ctsExecution.studyParamNames.precipitation),
                      ctsExecution.observationYear
                    ) match {
                      case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                        return
                      case Right(dataFrame: DataFrame) => dataFrame
                    },
                    ctsStorage.climograph.dataTempAndPrec.format(
                      climateGroup.climateGroupName,
                      climateRecord.climateName,
                      record.location.replace(" ", "_")
                    ),
                    ctsLogs.fetchingClimateLocationTempPrec.format(
                      record.location.capitalize,
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

    /**
     * SingleParamStudies: runs parameter-specific studies (top-N, evolution,
     * averages) for a configured list of climate parameters.
     */
    private object SingleParamStudies {
      private val ctsExecution = SparkConf.Constants.queries.execution.singleParamStudiesConf
      private val ctsLogs = SparkConf.Constants.queries.log.singleParamStudiesConf
      private val ctsStorage = SparkConf.Constants.queries.storage.singleParamStudiesConf

      /**
       * Execute the configured single-parameter studies (top-N, evolutions, etc.).
       */
      def execute(): Unit = {
        ctsExecution.singleParamStudiesValues.foreach(study => {
          printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
            study.studyParam.replace("_", " ")
          ))

          simpleFetchAndSave(
            ctsLogs.top10Highest2024.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          simpleFetchAndSave(
            ctsLogs.top10HighestDecade.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          simpleFetchAndSave(
            ctsLogs.top10HighestGlobal.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          simpleFetchAndSave(
            ctsLogs.top10Lowest2024.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          simpleFetchAndSave(
            ctsLogs.top10LowestDecade.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          simpleFetchAndSave(
            ctsLogs.top10LowestGlobal.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          val regressionModelDf: DataFrame = simpleFetchAndSave(
            ctsLogs.evolFromStartForEachState.format(
              study.studyParam.capitalize
            ),
            study.reprStationRegs.flatMap(record => {
              List(
                FetchAndSaveInfo(
                  getStationInfoById(record.stationIdGlobal) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataStationGlobal.format(
                    study.studyParamAbbrev,
                    record.stateNameNoSc
                  ),
                  ctsLogs.evolFromStartForEachStateStartStationGlobal.format(
                    record.stateName.capitalize
                  ),
                  saveAsJSON = true
                ),
                FetchAndSaveInfo(
                  getStationInfoById(record.stationIdLatest) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataStationLatest.format(
                    study.studyParamAbbrev,
                    record.stateNameNoSc
                  ),
                  ctsLogs.evolFromStartForEachStateStartStationLatest.format(
                    record.stateName.capitalize
                  ),
                  saveAsJSON = true
                ),
                FetchAndSaveInfo(
                  getClimateParamInALapseById(
                    record.stationIdLatest,
                    List(
                      (study.dataframeColName, study.studyParamAbbrev)
                    ),
                    List(study.colAggMethod),
                    record.startDateLatest,
                    Some(record.endDateLatest)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataEvol.format(
                    study.studyParamAbbrev,
                    record.stateNameNoSc
                  ),
                  ctsLogs.evolFromStartForEachStateStart.format(
                    record.stateName,
                    study.studyParam.replace("_", " ")
                  )
                ),
                FetchAndSaveInfo(
                  getClimateYearlyGroupById(
                    record.stationIdGlobal,
                    List(
                      (study.dataframeColName, study.studyParamAbbrev)
                    ),
                    List(study.colAggMethod),
                    record.startDateGlobal,
                    Some(record.endDateGlobal)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataEvolYearlyGroup.format(
                    study.studyParamAbbrev,
                    record.stateNameNoSc
                  ),
                  ctsLogs.evolFromStartForEachStateYearlyGroup.format(
                    record.stateName,
                    study.studyParam.replace("_", " "),
                    study.colAggMethod
                  )
                ),
                FetchAndSaveInfo(
                  getStationClimateParamRegressionModelInALapse(
                    record.stationIdGlobal,
                    study.dataframeColName,
                    study.colAggMethod,
                    record.startDateGlobal,
                    Some(record.endDateGlobal)
                  ) match {
                    case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                      return
                    case Right(dataFrame: DataFrame) => dataFrame
                  },
                  ctsStorage.evolFromStartForEachState.dataEvolRegression.format(
                    study.studyParamAbbrev,
                    record.stateNameNoSc
                  ),
                  ctsLogs.evolFromStartForEachStateStartRegression.format(
                    record.stateName.capitalize,
                    study.studyParam.replace("_", " ")
                  )
                )
              )
            })
          ).zipWithIndex.filter {
            case (_, idx) => idx >= 4 && (idx - 4) % 5 == 0
          }.map(_._1).reduce(_ union _).persist(StorageLevel.MEMORY_AND_DISK_SER)

          regressionModelDf.count()

          simpleFetchAndSave(
            ctsLogs.top5HighestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(record => record.stationIdGlobal),
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

          simpleFetchAndSave(
            ctsLogs.top5LowestInc.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getTopNClimateParamIncrementInAYearLapse(
                  stationIds = study.reprStationRegs.map(record => record.stationIdGlobal),
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

          simpleFetchAndSave(
            ctsLogs.avg2024AllStationSpainContinental.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

          simpleFetchAndSave(
            ctsLogs.avg2024AllStationSpainCanary.format(
              study.studyParam
            ),
            List(
              FetchAndSaveInfo(
                getAllStationsByStatesAvgClimateParamInALapse(
                  climateParam = study.dataframeColName,
                  aggMethodName = study.colAggMethod,
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

    /**
     * InterestingStudies: executes more specialized studies that combine
     * different climate parameters and produce aggregated comparisons by state
     * and other dimensions.
     */
    private object InterestingStudies {
      private val ctsExecution = SparkConf.Constants.queries.execution.interestingStudiesConf
      private val ctsLogs = SparkConf.Constants.queries.log.interestingStudiesConf
      private val ctsStorage = SparkConf.Constants.queries.storage.interestingStudiesConf

      /**
       * Execute the set of interesting multi-parameter studies and persist results.
       */
      def execute(): Unit = {
        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.startStudy.format(
          ctsLogs.studyName
        ))

        simpleFetchAndSave(
          ctsLogs.precAndPressureEvolFromStartForEachState,
          ctsExecution.stationRecords.flatMap(record => {
            List(
              FetchAndSaveInfo(
                getStationInfoById(record.stationIdGlobal) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataStationGlobal.format(
                  record.stateNameNoSc
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateStartStationGlobal.format(
                  record.stateName.capitalize
                ),
                saveAsJSON = true
              ),
              FetchAndSaveInfo(
                getStationInfoById(record.stationIdLatest) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataStationLatest.format(
                  record.stateNameNoSc
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateStartStationLatest.format(
                  record.stateName.capitalize
                ),
                saveAsJSON = true
              ),
              FetchAndSaveInfo(
                getClimateParamInALapseById(
                  record.stationIdLatest,
                  ctsExecution.precAndPressEvolFromStartForEachState.climateParams,
                  ctsExecution.precAndPressEvolFromStartForEachState.colAggMethods,
                  record.startDateLatest,
                  Some(record.endDateLatest)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataEvol.format(
                  record.stateNameNoSc
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateStartEvol.format(
                  record.stateName
                )
              ),
              FetchAndSaveInfo(
                getClimateYearlyGroupById(
                  record.stationIdGlobal,
                  ctsExecution.precAndPressEvolFromStartForEachState.climateParams,
                  ctsExecution.precAndPressEvolFromStartForEachState.colAggMethods,
                  record.startDateGlobal,
                  Some(record.endDateGlobal)
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.precAndPressEvol.dataEvolYearlyGroup.format(
                  record.stateNameNoSc
                ),
                ctsLogs.precAndPressureEvolFromStartForEachStateYearlyGroup.format(
                  record.stateName
                )
              ),
            )
          })
        )

        ctsExecution.top10States.foreach(top10 => {
          simpleFetchAndSave(
            ctsLogs.top10States.format(top10.name),
            List(
              FetchAndSaveInfo(
                getTopNClimateConditionsInALapse(
                  climateParams = top10.climateParams,
                  startDate = top10.startDate,
                  endDate = Some(top10.endDate),
                  groupByState = true
                ) match {
                  case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
                    return
                  case Right(dataFrame: DataFrame) => dataFrame
                },
                ctsStorage.top10States.dataTop.format(top10.nameAbbrev)
              )
            )
          )
        })

        printlnConsoleEnclosedMessage(NotificationType.Information, ctsGlobalLogs.endStudy.format(
          ctsLogs.studyName
        ))
      }
    }

    /**
     * Retrieve station information by station identifier.
     *
     * @param stationId station code to search in the stations DataFrame
     * @return Right(DataFrame) with a single-row DataFrame containing basic station info, or Left(Exception)
     */
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

    /**
     * Count distinct stations grouped by a column over a time lapse.
     *
     * @param column tuple containing a Column expression and the output column name
     * @param startDate start of the interval (YYYY-MM-DD or year for yearly queries)
     * @param endDate optional end date (if omitted a single-year aggregation is applied)
     * @return Right(DataFrame) with counts or Left(Exception) on failure
     */
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
          .agg(countDistinct(col(ctsSchemaAemetAllMeteoInfo.indicativo)).as(ctsSpecialColumns.count))
          .select(
            col(column._2),
            col(ctsSpecialColumns.count)
          )
          .orderBy(column._2)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Count stations falling into parameter intervals for a given lapse.
     *
     * For each interval the function computes min/max bounds if they are
     * specified as infinities in configuration, then counts stations whose
     * parameter lies in the interval.
     *
     * @param paramIntervals list of (columnName, min, max)
     * @param startDate start date or year
     * @param endDate optional end date
     * @return Right(DataFrame) with rows per interval or Left(Exception)
     */
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
              lit(actualMin).as(ctsSpecialColumns.minValue),
              lit(actualMax).as(ctsSpecialColumns.maxValue),
              countDistinct(ctsSchemaAemetAllMeteoInfo.indicativo).as(ctsSpecialColumns.count)
            )
          }.reduce(_.union(_)).orderBy(ctsSpecialColumns.minValue)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Compute monthly averages for temperature and monthly sum of precipitation
     * for a station in a given year.
     *
     * @param stationId station code
     * @param studyParams tuple of (temperatureColumnName, precipitationColumnName)
     * @param observationYear year to observe
     * @return Right(DataFrame) with month, avg(temp) and sum(precip) columns
     */
    private def getStationMonthlyAvgTempAndSumPrecInAYear(
      stationId: String,
      studyParams: (String, String),
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
              month(col(ctsSchemaAemetAllMeteoInfo.fecha)).as(ctsSpecialColumns.month)
            ).agg(
              round(avg(col(ctsSchemaAemetAllMeteoInfo.tMed)), 1).as(ctsSpecialColumns.colMonthlyGrouped.format(studyParams._1, ctsGroupMethods.avg)),
              round(sum(col(ctsSchemaAemetAllMeteoInfo.prec)), 1).as(ctsSpecialColumns.colMonthlyGrouped.format(studyParams._2, ctsGroupMethods.sum))
            ).orderBy(col(ctsSpecialColumns.month))
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Compute top-N stations for a climate parameter within a given lapse.
     *
     * @param climateParam column name to aggregate
     * @param aggMethodName aggregation method (avg/sum/min/max)
     * @param paramNameToShow short name to use in result column
     * @param startDate start of lapse (or year)
     * @param endDate optional end date
     * @param topN number of rows to return
     * @param highest whether to return the highest (true) or lowest (false)
     * @return Right(DataFrame) with top-N stations or Left(Exception)
     */
    private def getTopNClimateParamInALapse(
      climateParam: String,
      aggMethodName: String,
      paramNameToShow: String,
      startDate: String,
      endDate: Option[String] = None,
      topN: Int = 10,
      highest: Boolean = true
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        val aggMethod: Column => Column = aggMethodName.toLowerCase match {
          case ctsGroupMethods.avg => avg
          case ctsGroupMethods.sum => sum
          case ctsGroupMethods.min => min
          case ctsGroupMethods.max => max
          case other => throw new IllegalArgumentException(other)
        }

        Right(
          meteoDf.filter(endDate match {
            case Some(endDate) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(endDate))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }).filter(col(climateParam).isNotNull)
          .groupBy(col(ctsSchemaAemetAllMeteoInfo.indicativo))
          .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.colGrouped.format(climateParam, aggMethodName)))
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
            round(col(ctsSpecialColumns.colGrouped.format(
              climateParam, aggMethodName
            )), 1).alias(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)),
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
              col(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)).desc
            else
              col(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)).asc
          )
          .limit(topN)
          .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Compute top-N stations (or states) matching a set of climate parameter
     * conditions within a lapse.
     *
     * @param climateParams list of (paramName, min, max) conditions
     * @param startDate start of lapse
     * @param endDate optional end date
     * @param groupByState whether the aggregation is by state or by station
     * @param topN max rows to return
     * @param highest whether to return highest or lowest results
     * @return Right(DataFrame) with top-N results, Left(Exception) on failure
     */
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
              .agg(count(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsSpecialColumns.daysWithConds))
              .orderBy(
                if (highest)
                  col(ctsSpecialColumns.daysWithConds).desc
                else
                  col(ctsSpecialColumns.daysWithConds).asc
              )
              .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
              .select(
                col(ctsSchemaAemetAllMeteoInfo.provincia).alias(ctsSchemaSparkAllStation.state),
                col(ctsSpecialColumns.top)
              )
          } else {
            filteredDf
              .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo)
              .agg(countDistinct(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsSpecialColumns.daysWithConds))
              .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
              .orderBy(
                if (highest)
                  col(ctsSpecialColumns.daysWithConds).desc
                else
                  col(ctsSpecialColumns.daysWithConds).asc
              )
              .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
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
                col(ctsSpecialColumns.top)
              )
          })
          .orderBy(col(ctsSpecialColumns.top).asc)
          .limit(topN)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Retrieve daily climate parameters for a station in a lapse and return
     * a DataFrame with dates and the requested parameters aggregated per day.
     *
     * @param stationId station code
     * @param climateParams sequence of (columnName, displayName)
     * @param aggMethodNames aggregation method names for each param
     * @param startDate start of lapse
     * @param endDate optional end date
     * @return Right(DataFrame) with daily series or Left(Exception)
     */
    private def getClimateParamInALapseById(
      stationId: String,
      climateParams: Seq[(String, String)],
      aggMethodNames: Seq[String],
      startDate: String,
      endDate: Option[String] = None
    ): Either[Exception, DataFrame] = {
      try {
        require(climateParams.length == aggMethodNames.length)

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
            Seq(col(ctsSchemaAemetAllMeteoInfo.fecha).alias(ctsSpecialColumns.date)) ++
            climateParams.zip(aggMethodNames).map(param => round(col(param._1._1), 1).alias(
              ctsSpecialColumns.colDailyGrouped.format(param._1._2, param._2)
            )): _*
          )
          .orderBy(ctsSpecialColumns.date)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Aggregate yearly groups for a station according to provided parameters
     * and aggregation methods.
     *
     * @param stationId station code
     * @param climateParams sequence of (colName, displayName)
     * @param aggMethodNames aggregation methods corresponding to each param
     * @param startDate start year or date
     * @param endDate optional end year or date
     * @return Right(DataFrame) grouped by year or Left(Exception)
     */
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
            .withColumn(ctsSpecialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
        ) { (acc, climateParam) =>
          acc.filter(col(climateParam._1).isNotNull)
        }

        val aggColumns = climateParams.zip(aggMethodNames).map {
          case ((colName, paramName), op) =>
            val baseCol = col(colName)
            val aggCol = op.toLowerCase match {
              case ctsGroupMethods.avg => round(avg(baseCol), 1)
              case ctsGroupMethods.sum => round(sum(baseCol), 1)
              case ctsGroupMethods.min => round(min(baseCol), 1)
              case ctsGroupMethods.max => round(max(baseCol), 1)
              case other => throw new IllegalArgumentException(other)
            }
            aggCol.alias(ctsSpecialColumns.colYearlyGrouped.format(paramName, op))
        }

        val resultDf = filteredDf
          .groupBy(ctsSpecialColumns.year)
          .agg(aggColumns.head, aggColumns.tail: _*)
          .orderBy(ctsSpecialColumns.year)

        Right(resultDf)

      } catch {
        case ex: Exception => Left(ex)
      }
    }

    /**
     * Compute averages for all stations filtered by optional set of states.
     *
     * @param climateParam column to aggregate
     * @param aggMethodName aggregation method (avg/sum/min/max)
     * @param paramNameToShow display name used in result columns
     * @param startDate start of lapse
     * @param endDate optional end date
     * @param states optional list of state names to filter
     * @return Right(DataFrame) with aggregated station averages or Left(Exception)
     */
    private def getAllStationsByStatesAvgClimateParamInALapse(
      climateParam: String,
      aggMethodName: String,
      paramNameToShow: String,
      startDate: String,
      endDate: Option[String] = None,
      states: Option[Seq[String]] = None
    ): Either[Exception, DataFrame] = {
      try {
        val meteoDf: DataFrame = SparkCore.dataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName)
        val stationDf: DataFrame = SparkCore.dataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName)

        val aggMethod: Column => Column = aggMethodName.toLowerCase match {
          case ctsGroupMethods.avg => avg
          case ctsGroupMethods.sum => sum
          case ctsGroupMethods.min => min
          case ctsGroupMethods.max => max
          case other => throw new IllegalArgumentException(other)
        }

        Right(
          meteoDf.filter(endDate match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startDate), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startDate.toInt
          }).filter(states match {
            case Some(stateList) => col(ctsSchemaAemetAllMeteoInfo.provincia).isin(stateList: _*)
            case None => lit(true)
          }).filter(col(climateParam).isNotNull)
          .groupBy(col(ctsSchemaAemetAllMeteoInfo.indicativo))
          .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.colGrouped.format(climateParam, aggMethodName)))
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
              col(ctsSpecialColumns.colGrouped.format(climateParam, aggMethodName)), 1
            ).alias(ctsSpecialColumns.colGrouped.format(paramNameToShow, aggMethodName)),
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

    /**
     * Compute a linear regression (beta1, beta0) for a climate parameter for
     * a station over a year range and return the model parameters as a small
     * DataFrame.
     *
     * @param stationId station code
     * @param climateParam parameter column name
     * @param aggMethodName aggregation method for yearly grouping
     * @param startYear start year
     * @param endYear optional end year
     * @return Right(DataFrame(stationId, beta1, beta0)) or Left(Exception)
     */
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
          case ctsGroupMethods.avg => avg
          case ctsGroupMethods.sum => sum
          case ctsGroupMethods.min => min
          case ctsGroupMethods.max => max
          case other => throw new IllegalArgumentException(other)
        }

        val filteredDF = meteoDf
          .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo) === stationId)
          .filter(endYear match {
            case Some(end) => col(ctsSchemaAemetAllMeteoInfo.fecha).between(lit(startYear), lit(end))
            case None => year(col(ctsSchemaAemetAllMeteoInfo.fecha)) === startYear
          })
          .filter(col(climateParam).isNotNull)
          .withColumn(ctsSpecialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
          .groupBy(ctsSpecialColumns.year)
          .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.climateParamGrouped.format(ctsGroupMethods.avg)))
          .select(
            col(ctsSpecialColumns.year).alias(ctsSpecialColumns.x),
            col(ctsSpecialColumns.climateParamGrouped.format(ctsGroupMethods.avg)).alias(ctsSpecialColumns.y)
          )

        val (meanX, meanY) = filteredDF.agg(
          avg(ctsSpecialColumns.x),
          avg(ctsSpecialColumns.y)
        ).as[(Double, Double)].first() match {
          case (mx, my) => (mx, my)
        }

        val (beta1, beta0) = filteredDF.withColumn(
            ctsSpecialColumns.xDiff, col(ctsSpecialColumns.x) - meanX
          )
          .withColumn(
            ctsSpecialColumns.yDiff, col(ctsSpecialColumns.y) - meanY
          )
          .agg(
            sum(
              col(ctsSpecialColumns.xDiff) * col(ctsSpecialColumns.yDiff)
            ).as(ctsSpecialColumns.num),
            sum(
              col(ctsSpecialColumns.xDiff) * col(ctsSpecialColumns.xDiff)
            ).as(ctsSpecialColumns.den)
          ).as[(Double, Double)].first() match {
          case (num, den) =>
            val b1 = num / den
            val b0 = meanY - b1 * meanX
            (b1, b0)
        }

        Right(Seq((stationId, beta1, beta0)).toDF(
          ctsSchemaSparkAllStation.stationId,
          ctsSpecialColumns.beta1,
          ctsSpecialColumns.beta0
        ))
      } catch {
        case exception: Exception => Left(exception)
      }
    }

    /**
     * Compute the top-N stations with the highest (or lowest) increment
     * of a climate parameter between two years using precomputed regression models.
     *
     * @param stationIds list of station codes to consider
     * @param regressionModels DataFrame containing regression model params
     * @param climateParam climate parameter column
     * @param paramNameToShow display name used in outputs
     * @param aggMethodName aggregation method used for yearly groupings
     * @param startYear start year for increment calculation
     * @param endYear end year for increment calculation
     * @param highest true to return highest increments, false for lowest
     * @param topN number of rows to return
     * @return Right(DataFrame) with top-N stations and increment metrics or Left(Exception)
     */
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
          case ctsGroupMethods.avg => avg
          case ctsGroupMethods.sum => sum
          case ctsGroupMethods.min => min
          case ctsGroupMethods.max => max
          case other => throw new IllegalArgumentException(other)
        }

        Right(
          regressionModels
            .filter(col(ctsSchemaSparkAllStation.stationId).isin(stationIds: _*))
            .withColumn(
              ctsSpecialColumns.inc,
              (col(ctsSpecialColumns.beta1) * lit(endYear) + col(ctsSpecialColumns.beta0)) - (col(ctsSpecialColumns.beta1) * lit(startYear) + col(ctsSpecialColumns.beta0))
            )
            .select(col(ctsSchemaSparkAllStation.stationId).as(ctsSchemaAemetAllMeteoInfo.indicativo), col(ctsSpecialColumns.inc))
            .join(
              meteoDf
                .filter(col(ctsSchemaAemetAllMeteoInfo.indicativo).isin(stationIds: _*))
                .filter(col(climateParam).isNotNull)
                .withColumn(ctsSpecialColumns.year, year(col(ctsSchemaAemetAllMeteoInfo.fecha)))
                .filter(col(ctsSpecialColumns.year).between(startYear, endYear))
                .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo, ctsSpecialColumns.year)
                .agg(aggMethod(col(climateParam)).as(ctsSpecialColumns.colYearlyGrouped.format(climateParam, aggMethodName)))
                .groupBy(ctsSchemaAemetAllMeteoInfo.indicativo)
                .agg(avg(col(ctsSpecialColumns.colYearlyGrouped.format(climateParam, aggMethodName))).as(ctsSpecialColumns.globalColYearlyAvg.format(climateParam))),
              Seq(ctsSchemaAemetAllMeteoInfo.indicativo),
              "inner"
            )
            .join(stationDf, Seq(ctsSchemaAemetAllStation.indicativo), "inner")
            .withColumn(ctsSpecialColumns.incPerc, col(ctsSpecialColumns.inc) / col(ctsSpecialColumns.globalColYearlyAvg.format(climateParam, aggMethodName)) * 100)
            .select(
              round(col(ctsSpecialColumns.inc), 1).alias(ctsSpecialColumns.inc),
              round(col(ctsSpecialColumns.incPerc), 1).alias(ctsSpecialColumns.incPerc),
              round(col(ctsSpecialColumns.globalColYearlyAvg.format(climateParam, aggMethodName)), 1).alias(ctsSpecialColumns.globalColYearlyAvg.format(paramNameToShow, aggMethodName)),
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
                col(ctsSpecialColumns.inc).desc
              else
                col(ctsSpecialColumns.inc).asc
            )
            .limit(topN)
            .withColumn(ctsSpecialColumns.top, monotonically_increasing_id() + 1)
        )
      } catch {
        case exception: Exception => Left(exception)
      }
    }
  }
}
