package DataExtraction.Config.DataExtractionConf.Execution

case class MetadataStructure(
  schemaDef: String,
  fieldId: String,
  fieldRequired: String
)

case class DataReformatterSingleStationMeteoInfo(
  startDate: String,
  endDate: String
)

case class DataReformatterSingleStationInfo(
  stateCode: String,
  stationCode: String
)

case class DataReformatterIfapa(
  singleStationMeteoInfo: DataReformatterSingleStationMeteoInfo,
  singleStationInfo: DataReformatterSingleStationInfo
)

case class DataReformatterConf(
  metadataStructure: MetadataStructure,
  ifapa: DataReformatterIfapa,
  jsonFilesTargetChunkSize: Int,
  filenameDelimiterChar: String
)