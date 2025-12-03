package DataExtraction.Config.GlobalConf

case class EnvironmentVarsNames(
  runningInEmr: String,
  awsS3Endpoint: String,
  storagePrefix: String,
  aemetOpenapiApiKey: String
)

case class EnvironmentVarsValues(
  runningInEmr: Option[Boolean],
  awsS3Endpoint: Option[String],
  storagePrefix: Option[String],
  aemetOpenapiApiKey: Option[String]
)

case class EnvironmentVars(
  names: EnvironmentVarsNames,
  values: EnvironmentVarsValues
)

case class InitConf(environmentVars: EnvironmentVars, storageBaseData: String)