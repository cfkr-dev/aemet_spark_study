package Config.GlobalConf

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

case class UtilsFormats(
  dateHour: String,
  dateHourUtc: String,
  dateHourZoned: String,
  dateFormat: String,
  dateFormatFile: String,
  hourMinuteFormat: String
)

case class UtilsErrors(
  failOnGettingJson: String,
  errorInReadingFile: String,
  errorInDirectoryCreation: String,
  environmentVariableNotFound: String
)

case class Chrono(
  chronoResult: String
)

case class BetweenStages(
  millisBetweenStages: Int,
  infoText: String
)

case class AwsS3(
  endpointEnvName: String
)

case class UtilsConf(environmentVars: EnvironmentVars, formats: UtilsFormats, errors: UtilsErrors, chrono: Chrono, betweenStages: BetweenStages)
