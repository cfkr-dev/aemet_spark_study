package Spark.Config.SparkConf.Init.Storage

case class AemetAllMeteoInfoDirs(data: String)
case class AemetAllMeteoInfoFilenames(data: String)
case class AemetAllmeteoInfoFilepaths(data: String)
case class AemetAllMeteoInfo(dirs: AemetAllMeteoInfoDirs, filenames: AemetAllMeteoInfoFilenames, filepaths: AemetAllmeteoInfoFilepaths)

case class AemetAllStationInfoDirs(data: String)
case class AemetAllStationInfoFilenames(data: String)
case class AemetAllStationInfoFilepaths(data: String)
case class AemetAllStationInfo(dirs: AemetAllStationInfoDirs, filenames: AemetAllStationInfoFilenames, filepaths: AemetAllStationInfoFilepaths)

case class AemetConf(baseDir: String, allMeteoInfo: AemetAllMeteoInfo, allStationInfo: AemetAllStationInfo)
