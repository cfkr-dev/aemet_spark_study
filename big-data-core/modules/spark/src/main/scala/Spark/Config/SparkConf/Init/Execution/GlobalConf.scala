package Spark.Config.SparkConf.Init.Execution

case class ReadConfig(
  readFormat: String,
  readMode: String
)

case class GlobalConf(
  readConfig: ReadConfig
)