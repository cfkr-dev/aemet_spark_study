package Config.SparkConf.Execution

case class InterestingStudiesRepresentativeStationRegistry(
  stateName: String,
  stateNameNoSc: String,
  stationId: String,
  startDate: String,
  endDate: String
)

case class PrecAndPressEvolFromStartForEachState(
  climateParams: List[(String, String)]
)

case class InterestingStudyTop10(
  climateParams: List[(String, Double, Double)],
  startDate: String,
  endDate: String
)

case class InterestingStudiesConf(
  stationRegistries: List[InterestingStudiesRepresentativeStationRegistry],
  precAndPressEvolFromStartForEachState: PrecAndPressEvolFromStartForEachState,
  top10BetterWindPower: InterestingStudyTop10,
  top10BetterSunPower: InterestingStudyTop10,
  top10TorrentialRains: InterestingStudyTop10,
  top10Storms: InterestingStudyTop10,
  top10Agriculture: InterestingStudyTop10,
  top10Droughts: InterestingStudyTop10,
  top10Fires: InterestingStudyTop10,
  top10HeatWaves: InterestingStudyTop10,
  top10Frosts: InterestingStudyTop10
)