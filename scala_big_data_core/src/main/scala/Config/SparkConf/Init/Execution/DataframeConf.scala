package Config.SparkConf.Init.Execution

case class AemetMetadataStructure(
  schemaDef: String,
  fieldId: String,
  fieldRequired: String
)

case class Provincia(
  staCruzDeTenerife: String,
  santaCruzDeTenerife: String,
  baleares: String,
  illesBalears: String,
  almeriaSc: String,
  almeria: String
)

case class Prec(
  acum: String,
  ip: String
)

case class GenericHour(
  varias: String
)

case class Dir(
  noData: String,
  variable: String
)

case class AllMeteoInfoDfSpecialValues(
  provincia: Provincia,
  prec: Prec,
  genericHour: GenericHour,
  dir: Dir
)

case class AllMeteoInfoDf(
  aliasName: String,
  spacialValues: AllMeteoInfoDfSpecialValues
)

case class GenericCardinalCoord(
  north: String,
  south: String,
  east: String,
  west: String
)

case class AllStationsDfSpecialValues(
  provincia: Provincia,
  genericCardinalCoord: GenericCardinalCoord
)

case class AllStationsDf(
  aliasName: String,
  spacialValues: AllStationsDfSpecialValues
)

case class SpecialColumns(
  count: String,
  top: String,
  minValue: String,
  maxValue: String,
  year: String,
  yearly: String,
  month: String,
  monthly: String,
  day: String,
  daily: String,
  colAvg: String,
  colSum: String,
  daysWithConds: String,
  climateParam: String,
  x: String,
  y: String,
  xDiff: String,
  yDiff: String,
  num: String,
  den: String,
  beta1: String,
  beta0: String,
  inc: String,
  incPerc: String
)

case class DataframeConf(
  aemetMetadataStructure: AemetMetadataStructure,
  allMeteoInfoDf: AllMeteoInfoDf,
  allStationsDf: AllStationsDf,
  specialColumns: SpecialColumns
)