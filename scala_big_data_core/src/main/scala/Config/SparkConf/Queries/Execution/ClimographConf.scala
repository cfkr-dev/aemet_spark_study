package Config.SparkConf.Queries.Execution

case class ClimographLocations(
  peninsula: String,
  canaryIslands: String,
  balearIslands: String
)

case class LocationRegistry(
  location: String,
  stationId: String
)

case class ClimateRegistry(
  climateName: String,
  registries: List[LocationRegistry]
)

case class ClimateGroupRegistry(
  climateGroupName: String,
  climates: List[ClimateRegistry]
)

case class ClimographConf(
  observationYear: Int,
  locations: ClimographLocations,
  stationsRegistries: List[ClimateGroupRegistry]
)