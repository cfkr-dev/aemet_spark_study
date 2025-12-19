package Spark.Config.SparkConf.Queries.Execution

case class StationCountEvolFromStart(
  startDate: String,
  endDate: String,
  param: String,
  paramSelectName: String
)

case class StationCountByState2024(
  startDate: String,
  param: String,
  paramSelectName: String
)

case class StationCountByAltitude2024(
  startDate: String,
  intervals: List[(String, Double, Double)]
)

case class StationsConf(
  countEvolFromStart: StationCountEvolFromStart,
  countByState2024: StationCountByState2024,
  countByAltitude2024: StationCountByAltitude2024
)