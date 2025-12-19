package Spark.Core.Session

import Spark.Config.{GlobalConf, SparkConf}
import Utils.ChronoUtils
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import Utils.Storage.Core.Storage
import Utils.Storage.JSON.JSONStorageBackend.writeJSON
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ujson.read

case class SparkSessionCore(
  sparkSession: SparkSession,
  sessionDataframes: SessionDataframes,
  sessionStorage: SessionStorage
) {
  private val ctsGlobalUtils = GlobalConf.Constants.utils
  private val ctsInitLogs = SparkConf.Constants.init.log
  private val ctsExecutionSessionConf = SparkConf.Constants.init.execution.sessionConf
  private val ctsExecutionDataframeConf = SparkConf.Constants.init.execution.dataframeConf

  private val chronometer = ChronoUtils.Chronometer()
  private implicit val dataStorage: Storage = sessionStorage.dataStorage

  val showResults: Boolean = ctsExecutionSessionConf.showResults

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
    sparkSession.conf.getAll.foreach { case (k, v) => printlnConsoleMessage(NotificationType.Information, s"$k = $v") }
    sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName).count()
    sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName).count()
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
    sessionDataframes.allStations.as(ctsExecutionDataframeConf.allStationsDf.aliasName).unpersist()
    sessionDataframes.allMeteoInfo.as(ctsExecutionDataframeConf.allMeteoInfoDf.aliasName).unpersist()
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
   * @param path      destination path relative to the storage prefix
   * @return Right(path) on success, Left(Exception) on failure
   */
  def saveDataframeAsParquet(dataframe: DataFrame, path: String): Either[Exception, String] = {
    try {
      dataframe.write
        .mode(SaveMode.Overwrite)
        .parquet(sessionStorage.storagePrefix + path)

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
   * @param path      destination path for the JSON file
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
}