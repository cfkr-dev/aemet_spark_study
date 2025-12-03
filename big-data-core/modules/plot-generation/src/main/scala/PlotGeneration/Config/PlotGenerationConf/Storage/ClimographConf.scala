package PlotGeneration.Config.PlotGenerationConf.Storage

case class Climograph(
  dataSrcStation: String,
  dataSrcTempAndPrec: String,
  dataDest: String
)

case class ClimographConf(
  baseSrcDir: String,
  baseDestDir: String,
  climograph: Climograph
)