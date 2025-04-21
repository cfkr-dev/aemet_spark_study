package Config.SparkConf.Storage

case class ClimographDirs(
  dataStation: String,
  dataTempAndPrec: String
)

case class ClimographConf(
  baseDir: String,
  climograph: ClimographDirs
)