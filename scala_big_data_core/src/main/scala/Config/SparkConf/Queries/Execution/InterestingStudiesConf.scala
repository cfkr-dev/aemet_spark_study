package Config.SparkConf.Queries.Execution

case class InterestingStudiesRepresentativeStationRecord(
  stateName: String,
  stateNameNoSc: String,
  stationIdGlobal: String,
  stationIdLatest: String,
  startDateLatest: String,
  endDateLatest: String,
  startDateGlobal: String,
  endDateGlobal: String
)

case class PrecAndPressEvolFromStartForEachState(
  climateParams: List[(String, String)],
  colAggMethods: List[String]
)

case class InterestingStudyTop10(
  name: String,
  nameAbbrev: String,
  climateParams: List[(String, Double, Double)],
  startDate: String,
  endDate: String
)

case class InterestingStudiesConf(
  stationRecords: List[InterestingStudiesRepresentativeStationRecord],
  precAndPressEvolFromStartForEachState: PrecAndPressEvolFromStartForEachState,
  top10States: List[InterestingStudyTop10]
)