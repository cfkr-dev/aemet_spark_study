package Config.SparkConf.Init.Execution

case class ReadConfig(
  readFormat: String,
  readMode: String
)

case class AggregationMethods(
  sum: String,
  avg: String,
  max: String,
  min: String
)

case class GlobalConf(
  readConfig: ReadConfig,
  groupMethods: AggregationMethods
)