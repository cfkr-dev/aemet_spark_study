package Config.GlobalConf.Schema

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

case class SparkSpecialColumns(
  date: String,
  year: String,
  yearly: String,
  month: String,
  monthly: String,
  day: String,
  daily: String,
  colGrouped: String,
  colDailyGrouped: String,
  colMonthlyGrouped: String,
  colYearlyGrouped: String,
  climateParamGrouped: String,
  globalColYearlyAvg: String,
  daysWithConds: String,
  state: String,
  count: String,
  top: String,
  minValue: String,
  maxValue: String,
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

case class GroupMethods(
  sum: String,
  avg: String,
  max: String,
  min: String
)

case class MeteoParamsValuesEntry(
  studyParamName: String,
  studyParamAbbrev: String,
  studyParamUnit: String,
  colAggMethod: String
)

case class MeteoParamsValues(
  temperature: MeteoParamsValuesEntry,
  precipitation: MeteoParamsValuesEntry,
  windVelocity: MeteoParamsValuesEntry,
  pressure: MeteoParamsValuesEntry,
  sunRadiation: MeteoParamsValuesEntry,
  relativeHumidity: MeteoParamsValuesEntry
) {
  def toList: List[MeteoParamsValuesEntry] = List(
    temperature,
    precipitation,
    windVelocity,
    pressure,
    sunRadiation,
    relativeHumidity
  )
}

case class StateValueEntry(
  stateName: String,
  stateNameNoSc: String
)

case class StateValues(
  aCoruna: StateValueEntry,
  albacete: StateValueEntry,
  alicante: StateValueEntry,
  almeria: StateValueEntry,
  araba: StateValueEntry,
  asturias: StateValueEntry,
  avila: StateValueEntry,
  badajoz: StateValueEntry,
  barcelona: StateValueEntry,
  bizkaia: StateValueEntry,
  burgos: StateValueEntry,
  caceres: StateValueEntry,
  cadiz: StateValueEntry,
  cantabria: StateValueEntry,
  castellon: StateValueEntry,
  ceuta: StateValueEntry,
  ciudadReal: StateValueEntry,
  cordoba: StateValueEntry,
  cuenca: StateValueEntry,
  gipuzkoa: StateValueEntry,
  girona: StateValueEntry,
  granada: StateValueEntry,
  guadalajara: StateValueEntry,
  huelva: StateValueEntry,
  huesca: StateValueEntry,
  illesBalears: StateValueEntry,
  jaen: StateValueEntry,
  laRioja: StateValueEntry,
  lasPalmas: StateValueEntry,
  leon: StateValueEntry,
  lleida: StateValueEntry,
  lugo: StateValueEntry,
  madrid: StateValueEntry,
  malaga: StateValueEntry,
  melilla: StateValueEntry,
  murcia: StateValueEntry,
  navarra: StateValueEntry,
  ourense: StateValueEntry,
  palencia: StateValueEntry,
  pontevedra: StateValueEntry,
  salamanca: StateValueEntry,
  segovia: StateValueEntry,
  sevilla: StateValueEntry,
  soria: StateValueEntry,
  santaCruzDeTenerife: StateValueEntry,
  tarragona: StateValueEntry,
  teruel: StateValueEntry,
  toledo: StateValueEntry,
  valencia: StateValueEntry,
  valladolid: StateValueEntry,
  zamora: StateValueEntry,
  zaragoza: StateValueEntry
){
  def toList: List[StateValueEntry] = List(
    aCoruna,
    albacete,
    alicante,
    almeria,
    araba,
    asturias,
    avila,
    badajoz,
    barcelona,
    bizkaia,
    burgos,
    caceres,
    cadiz,
    cantabria,
    castellon,
    ceuta,
    ciudadReal,
    cordoba,
    cuenca,
    gipuzkoa,
    girona,
    granada,
    guadalajara,
    huelva,
    huesca,
    illesBalears,
    jaen,
    laRioja,
    lasPalmas,
    leon,
    lleida,
    lugo,
    madrid,
    malaga,
    melilla,
    murcia,
    navarra,
    ourense,
    palencia,
    pontevedra,
    salamanca,
    segovia,
    sevilla,
    soria,
    santaCruzDeTenerife,
    tarragona,
    teruel,
    toledo,
    valencia,
    valladolid,
    zamora,
    zaragoza
  )
}

case class SparkConf(
  stationsDf: SparkStationDf,
  specialColumns: SparkSpecialColumns,
  groupMethods: GroupMethods,
  meteoParamsValues: MeteoParamsValues,
  stateValues: StateValues
)
