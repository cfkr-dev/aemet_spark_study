package Config.SparkConf.Queries.Log

case class InterestingStudiesConf(
  studyName: String,
  precAndPressureEvolFromStartForEachState: String,
  precAndPressureEvolFromStartForEachStateStartStationGlobal: String,
  precAndPressureEvolFromStartForEachStateStartStationLatest: String,
  precAndPressureEvolFromStartForEachStateStartEvol: String,
  precAndPressureEvolFromStartForEachStateYearlyGroup: String,
  top10States: String
)