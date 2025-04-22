package Config.SparkConf.Queries.Storage

case class InterestingStudiesTop10Dirs (
  dataBetterWindPower: String,
  dataBetterSunPower: String,
  dataTorrentialRains: String,
  dataStorms: String,
  dataAgriculture: String,
  dataDroughts: String,
  dataFires: String,
  dataHeatWaves: String,
  dataFrosts: String
)

case class PrecAndPressEvolDirs (
  dataStation: String,
  dataEvol: String
)

case class InterestingStudiesConf(
  baseDir: String,
  top10: InterestingStudiesTop10Dirs,
  precAndPressEvol: PrecAndPressEvolDirs
)