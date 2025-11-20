package Config.PlotGenerationConf.Execution

import Config.GlobalConf.Schema.MeteoParamsValuesEntry
import Config.PlotGenerationConf.Execution.DTO.DoubleLinearDTO

case class EvolPrecPressValues(
  temperature: MeteoParamsValuesEntry,
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

case class InterestingStudiesConf(
  evolPrecPressValues: EvolPrecPressValues,
  evolPrecPress2024: EvolPrecPress2024,
  evolPrecPressYearlyGroup: EvolPrecPressYearlyGroup,
)