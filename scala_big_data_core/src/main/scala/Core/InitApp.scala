package Core

import Core.DataExtraction.Aemet.AemetAPIClient
import Core.DataExtraction.Ifapa.{IfapaAPIClient, IfapaToAemetConverter}
import Core.PlotGeneration.PlotGenerator
import Core.Spark.SparkManager.SparkQueries

object InitApp extends App {
  AemetAPIClient.aemetDataExtraction()
  IfapaAPIClient.ifapaDataExtraction()
  IfapaToAemetConverter.ifapaToAemetConversion()
  SparkQueries.execute()
  PlotGenerator.generate()
}
