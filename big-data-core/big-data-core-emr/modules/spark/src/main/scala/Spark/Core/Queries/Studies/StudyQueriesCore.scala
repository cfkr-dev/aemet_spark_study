package Spark.Core.Queries.Studies

import Spark.Config.SparkConf
import Spark.Core.Session.SparkSessionCore
import Utils.ConsoleLogUtils.Message.{NotificationType, printlnConsoleEnclosedMessage, printlnConsoleMessage}
import org.apache.spark.sql.DataFrame

class StudyQueriesCore(sparkSessionCore: SparkSessionCore) {
  private val ctsGlobalLogs = SparkConf.Constants.queries.log.globalConf

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
  protected case class FetchAndSaveInfo(
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
   * the `sparkSessionCore` helpers.
   *
   * @param queryTitle optional title for the overall query batch
   * @param queries list of FetchAndSaveInfo describing sub-queries
   * @param encloseHalfLengthStart formatting width for enclosed console messages
   * @return sequence of DataFrames produced by the provided queries
   */
  protected def simpleFetchAndSave(
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

      if (sparkSessionCore.showResults) {
        printlnConsoleMessage(NotificationType.Information, subQuery.showInfoMessage)
        subQuery.dataframe.show()
      }

      printlnConsoleMessage(NotificationType.Information, subQuery.saveInfoMessage.format(
        subQuery.pathToSave
      ))

      if (!subQuery.saveAsJSON)
        sparkSessionCore.saveDataframeAsParquet(subQuery.dataframe, subQuery.pathToSave) match {
          case Left(exception: Exception) => printlnConsoleMessage(NotificationType.Warning, exception.toString)
          case Right(_) => ()
        }
      else
        sparkSessionCore.saveDataframeAsJSON(subQuery.dataframe, subQuery.pathToSave) match {
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
}
