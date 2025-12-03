package PlotGeneration.Config.PlotGenerationConf.Storage

case class CountEvol(
  dataSrc: String,
  dataDest: String
)

case class CountByState2024(
  dataSrc: String,
  dataDest: String
)

case class CountByAltitude2024(
  dataSrc: String,
  dataDest: String
)

case class StationsConf(
  baseSrcDir: String,
  baseDestDir: String,
  countEvol: CountEvol,
  countByState2024: CountByState2024,
  countByAltitude2024: CountByAltitude2024
)