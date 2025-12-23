package Spark.Core.Session

import Spark.Config.{GlobalConf, SparkConf}
import Utils.Storage.Core.Storage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Container for session-scoped DataFrames used across queries and studies.
 *
 * @param allStations DataFrame with station metadata
 * @param allMeteoInfo DataFrame with merged meteorological observations
 */
case class SessionDataframes(allStations: DataFrame, allMeteoInfo: DataFrame)

/**
 * Represents storage configuration and backend for a session.
 *
 * @param dataStorage storage backend implementation used to read/write data
 * @param storagePrefix resolved storage path prefix (e.g., s3a://bucket or local path)
 */
case class SessionStorage(dataStorage: Storage, storagePrefix: String)

/**
 * Manager responsible for creating and configuring Spark sessions and
 * preparing session-scoped resources.
 *
 * Responsibilities:
 * - Builds and configures a `SparkSession` according to environment and
 *   configuration (supports EMR and local with optional S3 mock settings).
 * - Resolves the storage prefix and prepares a `SessionStorage` instance.
 * - Loads and formats the session DataFrames (`allStations` and `allMeteoInfo`) and
 *   persists them in memory for fast access by queries and studies.
 *
 * Use `createSparkSessionCore()` to obtain a fully initialized `SparkSessionCore`.
 */
object SparkSessionManager {
  private val ctsGlobalInit = GlobalConf.Constants.init
  private val ctsGlobalUtils = GlobalConf.Constants.utils
  private val ctsSchemaAemetAllStation = GlobalConf.Constants.schema.aemetConf.allStationInfo
  private val ctsSchemaAemetAllMeteoInfo = GlobalConf.Constants.schema.aemetConf.allMeteoInfo
  private val ctsSchemaSparkAllStation = GlobalConf.Constants.schema.sparkConf.stationsDf
  private val ctsExecutionSessionConf = SparkConf.Constants.init.execution.sessionConf
  private val ctsExecutionDataframeConf = SparkConf.Constants.init.execution.dataframeConf
  private val ctsStorageAemet = SparkConf.Constants.init.storage.aemetConf
  private val ctsStorageIfapaAemetFormat = SparkConf.Constants.init.storage.ifapaAemetFormatConf
  private val ctsAllStationSpecialValues = ctsExecutionDataframeConf.allStationsDf.specialValues
  private val ctsAllMeteoInfoSpecialValues = ctsExecutionDataframeConf.allMeteoInfoDf.specialValues

  /**
   * Create and configure a SparkSession instance.
   *
   * Behavior:
   * - When running in EMR (controlled by configuration), returns an
   * application SparkSession using getOrCreate().
   * - Otherwise, optionally applies S3 mock configurations (access/secret
   * key, endpoint, path-style access, and SSL) when an S3 endpoint is
   * provided in the environment configuration.
   * - Sets the Spark log level and clears the catalog cache before
   * returning the session.
   *
   * @param name     application name for the SparkSession
   * @param master   master URL (e.g., local[*] or spark://...)
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
  private def setStoragePrefix(dataStorage: Storage, prefix: Option[String]): String = {
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
   * @param session    active SparkSession used for reading
   * @param sourcePath relative path (under the storage prefix) to the Parquet data
   * @return Right(DataFrame) on success, Left(Exception) on failure
   */
  private def createDataframeFromParquet(
    session: SparkSession,
    storagePrefix: String,
    sourcePath: String
  ): Either[Exception, DataFrame] = {
    Right(session
      .read
      .parquet(storagePrefix + sourcePath)
    )
  }

  /**
   * Format the stations DataFrame (convert coordinates and normalize
   * textual fields).
   *
   * Responsibilities:
   * - Normalize province names using configured special-values mapping.
   * - Cast altitude to integer type.
   * - Convert DMS coordinate strings (degrees/minutes/seconds with
   * cardinal direction) into decimal degrees.
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
        (column => to_date(col(column), ctsGlobalUtils.formats.dateFormat)),
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
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
                s"${ctsGlobalUtils.formats.dateFormat} ${ctsGlobalUtils.formats.hourMinuteFormat}"
              )
            )
        }),
    )

    formatters.foldLeft(dataframe) {
      case (accumulatedDf, (colName, transformationFunc)) =>
        accumulatedDf.withColumn(colName, transformationFunc(colName))
    }
  }

  def createSparkSessionCore(): SparkSessionCore = {
    val sparkSession: SparkSession = createSparkSession(
      ctsExecutionSessionConf.sessionName,
      ctsExecutionSessionConf.sessionMaster,
      ctsExecutionSessionConf.sessionLogLevel
    )

    val dataStorage: Storage = GlobalConf.Constants.dataStorage
    val sessionStorage: SessionStorage = SessionStorage(
      dataStorage,
      setStoragePrefix(dataStorage, ctsGlobalInit.environmentVars.values.storagePrefix)
    )

    val sessionDataframes: SessionDataframes = SessionDataframes(
      formatAllStationsDataframe(
        createDataframeFromParquet(
          sparkSession,
          sessionStorage.storagePrefix,
          ctsStorageAemet.allStationInfo.dirs.data
        ) match {
          case Left(exception) => throw exception
          case Right(df) => df.union(createDataframeFromParquet(
            sparkSession,
            sessionStorage.storagePrefix,
            ctsStorageIfapaAemetFormat.singleStationInfo.dirs.data
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df
          })
        }
      ).persist(StorageLevel.MEMORY_AND_DISK_SER),
      formatAllMeteoInfoDataframe(
        createDataframeFromParquet(
          sparkSession,
          sessionStorage.storagePrefix,
          ctsStorageAemet.allMeteoInfo.dirs.data
        ) match {
          case Left(exception) => throw exception
          case Right(df) => df.union(createDataframeFromParquet(
            sparkSession,
            sessionStorage.storagePrefix,
            ctsStorageIfapaAemetFormat.singleStationMeteoInfo.dirs.data
          ) match {
            case Left(exception) => throw exception
            case Right(df) => df
          })
        }
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)
    )

    SparkSessionCore(sparkSession, sessionDataframes, sessionStorage)
  }
}