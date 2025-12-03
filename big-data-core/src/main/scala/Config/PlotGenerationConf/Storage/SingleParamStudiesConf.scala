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
  dataSrcStation: String,
  dataSrc: String,
  dataDest: String
)

case class EvolYearlyGroup(
  dataSrcStation: String,
  dataSrc: String,
  regressionSrc: String,
  dataDest: String
)

case class Evol(
  evol2024: Evol2024,
  evolYearlyGroup: EvolYearlyGroup
)

case class HeatMap2024(
  dataSrc: String,
  dataDest: String
)

case class SingleParamStudiesConf(
  baseSrcDir: String,
  baseDestDir: String,
  top10: Top10,
  top5Inc: Top5Inc,
  evol: Evol,
  heatMap2024: HeatMap2024
)