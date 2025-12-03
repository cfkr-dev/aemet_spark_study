package PlotGeneration.Config.PlotGenerationConf.Execution


import PlotGeneration.Config.GlobalConf.Schema.MeteoParamsValuesEntry
import PlotGeneration.Config.PlotGenerationConf.Execution.DTO.ClimographDTO

case class Locations(
  peninsula: String,
  canaryIslands: String,
  balearIslands: String
)

case class ClimateRecord(
  climateName: String,
  locations: List[String]
)

case class ClimateRecords(
  climateGroupName: String,
  climates: List[ClimateRecord]
)

case class ClimographMeteoParams(
  temperature: MeteoParamsValuesEntry,
  precipitation: MeteoParamsValuesEntry
)

case class ClimographValues(
  locations: Locations,
  climateRecords: List[ClimateRecords],
  meteoParams: ClimographMeteoParams
)

case class Climograph(
  uri: String,
  body: ClimographDTO
)

case class ClimographConf(
  climographValues: ClimographValues,
  climograph: Climograph
)