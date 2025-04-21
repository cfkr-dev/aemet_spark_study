package Config.GlobalConf

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
  errorInDirectoryCreation: String
)

case class UtilsConf(formats: UtilsFormats, errors: UtilsErrors)
