package Config.PlotGenerationConf.Execution

import Config.PlotGenerationConf.Execution.DTO.{BarDTO, HeatMapDTO, LinearDTO, LinearRegressionDTO}

case class Top10ValuesTemporal(
  value: String,
  title: String
)

case class Top10Values(
  order: List[String],
  temporal: List[Top10ValuesTemporal]
)

case class Top10(
  uri: String,
  body: BarDTO
)

case class Top5IncValues(
  order: List[String]
)

case class Top5Inc(
  uri: String,
  body: BarDTO
)

case class Evol2024(
  uri: String,
  body: LinearDTO
)

case class EvolYearlyGroup(
  uri: String,
  body: LinearRegressionDTO
)

case class HeatMap2024(
  uri: String,
  body: HeatMapDTO
)

case class SingleParamStudiesConf(
  top10Values: Top10Values,
  top10: Top10,
  top5IncValues: Top5IncValues,
  top5Inc: Top5Inc,
  evol2024: Evol2024,
  evolYearlyGroup: EvolYearlyGroup,
  heatMap2024Values: List[String],
  heatMap2024: HeatMap2024
)