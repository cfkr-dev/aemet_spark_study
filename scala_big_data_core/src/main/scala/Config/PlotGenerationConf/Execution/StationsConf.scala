package Config.PlotGenerationConf.Execution

import Config.PlotGenerationConf.Execution.DTO.LinearDTO
import Config.PlotGenerationConf.Execution.DTO.TableDTO
import Config.PlotGenerationConf.Execution.DTO.PieDTO

case class CountEvol(
  uri: String,
  body: LinearDTO
)

case class CountByState2024(
  uri: String,
  body: TableDTO
)

case class CountByAltitude2024(
  uri: String,
  body: PieDTO
)

case class StationsConf(
  countEvol: CountEvol,
  countByState2024: CountByState2024,
  countByAltitude2024: CountByAltitude2024
)