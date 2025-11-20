package Config.SparkConf.Queries.Execution

case class ClimographStudyParamNames(
  temperature: String,
  precipitation: String
)

case class ClimographLocations(
  peninsula: String,
  canaryIslands: String,
  balearIslands: String
)

case class LocationRecord(
  location: String,
  stationId: String
)

case class ClimateRecord(
  climateName: String,
  records: List[LocationRecord]
)

case class ClimateGroupRecord(
  climateGroupName: String,
  climates: List[ClimateRecord]
)

case class ClimographConf(
  observationYear: Int,
  studyParamNames: ClimographStudyParamNames,
  locations: ClimographLocations,
  stationsRecords: List[ClimateGroupRecord]
)