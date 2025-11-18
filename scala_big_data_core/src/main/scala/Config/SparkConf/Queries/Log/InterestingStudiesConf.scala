package Config.SparkConf.Queries.Log

case class InterestingStudiesConf(
  studyName: String,
  precAndPressureEvolFromStartForEachState: String,
  precAndPressureEvolFromStartForEachStateStartStationGlobal: String,
  precAndPressureEvolFromStartForEachStateStartStationLatest: String,
  precAndPressureEvolFromStartForEachStateStartEvol: String,
  precAndPressureEvolFromStartForEachStateYearlyGroup: String,
  top10BetterWindPower: String,
  top10BetterSunPower: String,
  top10TorrentialRains: String,
  top10Storms: String,
  top10Agriculture: String,
  top10Droughts: String,
  top10Fires: String,
  top10HeatWaves: String,
  top10Frosts: String
)