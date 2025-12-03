package Config.PlotGenerationConf.Storage

case class EvolPrecPress2024(
  dataSrcStation: String,
  dataSrc: String,
  dataDest: String
)

case class EvolPrecPressYearlyGroup(
  dataSrcStation: String,
  dataSrc: String,
  dataDest: String
)

case class EvolPrecPress(
  evol2024: EvolPrecPress2024,
  evolYearlyGroup: EvolPrecPressYearlyGroup
)

case class Top10States(
  dataSrc: String,
  dataDest: String
)

case class InterestingStudiesConf(
  baseSrcDir: String,
  baseDestDir: String,
  evolPrecPress: EvolPrecPress,
  top10States: Top10States
)