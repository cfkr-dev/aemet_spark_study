package Config.SparkConf.Queries.Execution

case class StudyParamNames(
  temperature: String,
  precipitation: String,
  windVelocity: String,
  pressure: String,
  sunRadiation: String,
  relativeHumidity: String
)

case class StudyParamAbbrev(
  temperature: String,
  precipitation: String,
  windVelocity: String,
  pressure: String,
  sunRadiation: String,
  relativeHumidity: String
)

case class SingleParamStudyRepresentativeStationRegistry(
  stateName: String,
  stateNameNoSc: String,
  stationId: String,
  startDate: String,
  endDate: String
)

case class StudyRegistry(
  studyParam: String,
  studyParamAbbrev: String,
  dataframeColName: String,
  reprStationRegs: List[SingleParamStudyRepresentativeStationRegistry]
)

case class Top10Highest2024(
  startDate: String
)

case class Top10HighestDecade(
  startDate: String,
  endDate: String
)

case class Top10HighestGlobal(
  startDate: String,
  endDate: String
)

case class Top10Lowest2024(
  startDate: String
)

case class Top10LowestDecade(
  startDate: String,
  endDate: String
)

case class Top10LowestGlobal(
  startDate: String,
  endDate: String
)

case class Top5HighestInc(
  startYear: Int,
  endYear: Int
)

case class Top5LowestInc(
  startYear: Int,
  endYear: Int
)

case class Avg2024AllStationSpain(
  startDate: String,
  canaryIslandStates: List[String]
)


case class SingleParamStudiesConf(
  studyParamNames: StudyParamNames,
  studyParamAbbrev: StudyParamAbbrev,
  singleParamStudiesValues: List[StudyRegistry],
  top10Highest2024: Top10Highest2024,
  top10HighestDecade: Top10HighestDecade,
  top10HighestGlobal: Top10HighestGlobal,
  top10Lowest2024: Top10Lowest2024,
  top10LowestDecade: Top10LowestDecade,
  top10LowestGlobal: Top10LowestGlobal,
  top5HighestInc: Top5HighestInc,
  top5LowestInc: Top5LowestInc,
  avg2024AllStationSpain: Avg2024AllStationSpain
)