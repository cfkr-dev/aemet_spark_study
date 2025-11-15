package Config.PlotGenerationConf.Storage

case class Top10(
  dataSrc: String,
  dataDest: String
)

case class Top5Inc(
  dataSrc: String,
  dataDest: String
)

case class Evol2024(
  dataSrc: String,
  dataDest: String
)

case class Evol(
  dataSrcStation: String,
  evol2024: Evol2024
)

case class SingleParamStudiesConf(
  baseSrcDir: String,
  baseDestDir: String,
  top10: Top10,
  top5Inc: Top5Inc,
  evol: Evol
)