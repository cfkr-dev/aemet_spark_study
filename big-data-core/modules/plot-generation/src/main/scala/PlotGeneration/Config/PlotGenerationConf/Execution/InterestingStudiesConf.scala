package PlotGeneration.Config.PlotGenerationConf.Execution

import PlotGeneration.Config.GlobalConf.Schema.MeteoParamsValuesEntry
import PlotGeneration.Config.PlotGenerationConf.Execution.DTO.{DoubleLinearDTO, TableDTO}

case class EvolPrecPressValues(
  precipitation: MeteoParamsValuesEntry,
  pressure: MeteoParamsValuesEntry
)

case class EvolPrecPress2024(
  uri: String,
  body: DoubleLinearDTO
)

case class EvolPrecPressYearlyGroup(
  uri: String,
  body: DoubleLinearDTO
)

case class Top10StatesValuesEntry(
  name: String,
  nameAbbrev: String
)

case class Top10States(
  uri: String,
  body: TableDTO
)

case class InterestingStudiesConf(
  evolPrecPressValues: EvolPrecPressValues,
  evolPrecPress2024: EvolPrecPress2024,
  evolPrecPressYearlyGroup: EvolPrecPressYearlyGroup,
  top10StatesValues: List[Top10StatesValuesEntry],
  top10States: Top10States
)