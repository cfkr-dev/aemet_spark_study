package Config.PlotGenerationConf

case class ColumnNames(
  state: String,
  count: String,
  top: String,
  minValue: String,
  maxValue: String,
  date: String,
  year: String,
  yearly: String,
  month: String,
  monthly: String,
  day: String,
  daily: String,
  tempMonthlyAvg: String,
  precMonthlySum: String,
  colAvg: String,
  colSum: String,
  colDailyGrouped: String,
  colYearlyGrouped: String,
  globalColYearlyAvg: String,
  daysWithConds: String,
  climateParam: String,
  climateParamAvg: String,
  x: String,
  y: String,
  xDiff: String,
  yDiff: String,
  num: String,
  den: String,
  beta1: String,
  beta0: String,
  inc: String,
  incPerc: String,
)

case class GroupMethods(
  sum: String,
  avg: String,
  max: String,
  min: String
)

case class Formatters(
  timestamp: String,
  timestampYear: String
)

case class UtilsConf(columnNames: ColumnNames, groupMethods: GroupMethods, formatters: Formatters)