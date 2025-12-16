package DataExtraction.Config.DataExtractionConf.Execution

case class MetadataStructure(
  schemaDef: String,
  fieldId: String,
  fieldRequired: String
)

case class DataReformatterConf(metadataStructure: MetadataStructure, jsonFilesTargetChunkSize: Int)