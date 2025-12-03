package PlotGeneration.Config.PlotGenerationConf.Execution

import PlotGeneration.Config.PlotGenerationConf.Execution.DTO.{LinearDTO, PieDTO, TableDTO}

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