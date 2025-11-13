package Config.PlotGenerationConf.Storage

case class Top10(
  dataSrc: String,
  dataDest: String
)

case class SingleParamStudiesConf(
  baseSrcDir: String,
  baseDestDir: String,
  top10: Top10
)