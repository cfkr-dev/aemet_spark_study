package Config.Global.Schema

case class SparkMeteoDf(
  date: String,
  stationId: String,
  stationName: String,
  state: String,
  altitude: String,
  tempAvg: String,
  prec: String,
  tempMin: String,
  hourTempMin: String,
  tempMax: String,
  hourTempMax: String,
  dir: String,
  velAvg: String,
  velMax: String,
  hourVelMax: String,
  sunMaxRadHours: String,
  pressMax: String,
  hourPressMax: String,
  pressMin: String,
  hourPressMin: String,
  relHumAvg: String,
  relHumMax: String,
  hourRelHumMax: String,
  relHumMin: String,
  hourRelHumMin: String
)

case class SparkStationDf(
  latDms: String,
  state: String,
  stationId: String,
  altitude: String,
  stationName: String,
  longDms: String,
  latDec: String,
  longDec: String
)

case class SparkConf(meteoDf: SparkMeteoDf, stationsDf: SparkStationDf)
