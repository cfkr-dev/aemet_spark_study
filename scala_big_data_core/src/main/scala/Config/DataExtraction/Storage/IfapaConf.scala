package Config.DataExtraction.Storage

case class IfapaSingleStationMeteoInfoDirs(data: String, metadata: String)
case class IfapaSingleStationMeteoInfoFilenames(data: String, metadata: String)
case class IfapaSingleStationMeteoInfoFilepaths(data: String, metadata: String)
case class IfapaSingleStationMeteoInfo(dirs: IfapaSingleStationMeteoInfoDirs, filenames: IfapaSingleStationMeteoInfoFilenames, filepaths: IfapaSingleStationMeteoInfoFilepaths)

case class IfapaSingleStationInfoDirs(data: String, metadata: String)
case class IfapaSingleStationInfoFilenames(data: String, metadata: String)
case class IfapaSingleStationInfoFilepaths(data: String, metadata: String)
case class IfapaSingleStationInfo(dirs: IfapaSingleStationInfoDirs, filenames: IfapaSingleStationInfoFilenames, filepaths: IfapaSingleStationInfoFilepaths)

case class IfapaConf(baseDir: String, singleStationMeteoInfo: IfapaSingleStationMeteoInfo, singleStationInfo: IfapaSingleStationInfo)
