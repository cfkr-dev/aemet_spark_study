package DataExtraction.Config.DataExtractionConf.Log

case class DataReformatterConf(
  aemetReformatStationsDataStart: String,
  aemetReformatStationsDataEnd: String,
  aemetReformatMeteoDataStart: String,
  aemetReformatMeteoDataEnd: String,
  ifapaReformatStationsDataStart: String,
  ifapaReformatStationsDataEnd: String,
  ifapaReformatMeteoDataStart: String,
  ifapaReformatMeteoDataEnd: String,
  reformatFile: String
)