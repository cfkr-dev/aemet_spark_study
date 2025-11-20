package Config.SparkConf.Init.Execution

case class SessionConf(
  sessionName: String,
  sessionMaster: String,
  sessionLogLevel: String,
  sessionStatsUrl: String,
  sessionResultFilesPrefix: String
)
