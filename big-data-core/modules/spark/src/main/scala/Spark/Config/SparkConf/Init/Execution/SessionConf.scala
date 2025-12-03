package Spark.Config.SparkConf.Init.Execution

case class S3AMockConfigurationNames(
  accessKey: String,
  secretKey: String,
  endpoint: String,
  pathStyleAccess: String,
  sslEnabled: String
)

case class S3AMockConfigurationValues(
  accessKey: String,
  secretKey: String,
  pathStyleAccess: Boolean,
  sslEnabled: Boolean
)

case class S3AMockConfiguration(
  names: S3AMockConfigurationNames,
  values: S3AMockConfigurationValues
)

case class SessionConf(
  sessionName: String,
  sessionMaster: String,
  sessionLogLevel: String,
  sessionStatsUrl: String,
  s3AMockConfiguration: S3AMockConfiguration
)
