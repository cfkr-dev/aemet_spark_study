package PlotGeneration.Config.GlobalConf

case class EnvironmentVarsNames(
  awsS3Endpoint: String,
  storagePrefix: String,
  storageBase: String,
  aemetOpenapiApiKey: String,
  autoPlotUrlBase: String
)

case class EnvironmentVarsValues(
  awsS3Endpoint: Option[String],
  storagePrefix: Option[String],
  storageBase: Option[String],
  aemetOpenapiApiKey: Option[String],
  autoPlotUrlBase: Option[String]
)

case class EnvironmentVars(
  names: EnvironmentVarsNames,
  values: EnvironmentVarsValues
)

case class InitConf(environmentVars: EnvironmentVars, storageBaseData: Option[String])