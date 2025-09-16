package Config.PlotGenerationConf.Execution

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

case class Climograph(
  uri: String,
  body: ClimographDTO
)

case class ClimographConf(
  locations: Locations,
  climateRegistries: List[ClimateRegistries],
  climograph: Climograph
)