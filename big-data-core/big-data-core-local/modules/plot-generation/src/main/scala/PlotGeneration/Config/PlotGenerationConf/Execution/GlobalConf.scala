package PlotGeneration.Config.PlotGenerationConf.Execution

case class Formatters(
  timestamp: String,
  timestampYear: String
)

case class GlobalConf(
  formatters: Formatters
)