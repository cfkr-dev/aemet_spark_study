package Config.SparkConf.Queries.Storage

case class InterestingStudiesTop10Dirs (
  dataTop: String
)

case class PrecAndPressEvolDirs (
  dataStationGlobal: String,
  dataStationLatest: String,
  dataEvol: String,
  dataEvolYearlyGroup: String
)

case class InterestingStudiesConf(
  baseDir: String,
  top10States: InterestingStudiesTop10Dirs,
  precAndPressEvol: PrecAndPressEvolDirs
)