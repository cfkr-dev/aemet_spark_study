package Spark.Config.SparkConf.Init.Log

case class SessionConf(
  startSparkSessionCheckStats: String,
  endSparkSession: String,
  endSparkSessionClosed: String,
  startQueries: String,
  endQueries: String
)