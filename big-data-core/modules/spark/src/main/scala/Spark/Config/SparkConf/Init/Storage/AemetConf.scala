package Spark.Config.SparkConf.Init.Storage

case class AemetAllMeteoInfoDirs(data: String, metadata: String)
case class AemetAllMeteoInfoFilenames(data: String, metadata: String)
case class AemetAllmeteoInfoFilepaths(data: String, metadata: String)
case class AemetAllMeteoInfo(dirs: AemetAllMeteoInfoDirs, filenames: AemetAllMeteoInfoFilenames, filepaths: AemetAllmeteoInfoFilepaths)

case class AemetAllStationInfoDirs(data: String, metadata: String)
case class AemetAllStationInfoFilenames(data: String, metadata: String)
case class AemetAllStationInfoFilepaths(data: String, metadata: String)
case class AemetAllStationInfo(dirs: AemetAllStationInfoDirs, filenames: AemetAllStationInfoFilenames, filepaths: AemetAllStationInfoFilepaths)

case class AemetConf(baseDir: String, allMeteoInfo: AemetAllMeteoInfo, allStationInfo: AemetAllStationInfo)
