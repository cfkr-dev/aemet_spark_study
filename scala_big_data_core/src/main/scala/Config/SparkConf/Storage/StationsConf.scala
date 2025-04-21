package Config.SparkConf.Storage


case class CountEvolFromStart(
  data: String
)

case class CountByState2024(
  data: String
)

case class CountByAltitude2024(
  data: String
)

case class StationsConf(
  baseDir: String,
  countEvolFromStart: CountEvolFromStart,
  countByState2024: CountByState2024,
  countByAltitude2024: CountByAltitude2024
)