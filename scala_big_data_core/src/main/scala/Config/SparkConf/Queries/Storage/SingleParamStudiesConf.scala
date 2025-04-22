package Config.SparkConf.Queries.Storage

case class SingleParamStudiesTop10Dirs(
  dataHighest2024: String,
  dataHighestDecade: String,
  dataHighestGlobal: String,
  dataLowest2024: String,
  dataLowestDecade: String,
  dataLowestGlobal: String
)

case class EvolFromStartForEachStateDirs(
  dataStation: String,
  dataEvol: String,
  dataEvolRegression: String
)

case class Top5Dirs(
  dataHighest: String,
  dataLowest: String
)

case class Avg2024AllStationsSpain(
  dataContinental: String,
  dataCanary: String
)

case class SingleParamStudiesConf(
  baseDir: String,
  top10: SingleParamStudiesTop10Dirs,
  evolFromStartForEachState: EvolFromStartForEachStateDirs,
  top5Inc: Top5Dirs,
  avg2024AllStationsSpain: Avg2024AllStationsSpain
)