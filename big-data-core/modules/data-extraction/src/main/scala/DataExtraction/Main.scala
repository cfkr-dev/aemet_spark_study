package DataExtraction

import DataExtraction.Core.Aemet.AemetAPIClient
import DataExtraction.Core.Ifapa.{IfapaAPIClient, IfapaToAemetConverter}

object Main extends App {
  AemetAPIClient.aemetDataExtraction()
  IfapaAPIClient.ifapaDataExtraction()
  IfapaToAemetConverter.ifapaToAemetConversion()
}
