package Config.PlotGenerationConf.Execution


import Config.GlobalConf.Schema.MeteoParamsValuesEntry
import Config.PlotGenerationConf.Execution.DTO.ClimographDTO

case class Locations(
  peninsula: String,
  canaryIslands: String,
  balearIslands: String
)

case class ClimateRegistry(
  climateName: String,
  locations: List[String]
)

case class ClimateRegistries(
  climateGroupName: String,
  climates: List[ClimateRegistry]
)

case class ClimographMeteoParams(
  temperature: MeteoParamsValuesEntry,
  precipitation: MeteoParamsValuesEntry
)

case class ClimographValues(
  locations: Locations,
  climateRegistries: List[ClimateRegistries],
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