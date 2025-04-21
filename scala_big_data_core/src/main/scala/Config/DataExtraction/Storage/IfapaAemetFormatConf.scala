package Config.DataExtraction.Storage

case class IfapaAemetFormatSingleStationMeteoInfoDirs(data: String, metadata: String)
case class IfapaAemetFormatSingleStationMeteoInfoFilenames(data: String, metadata: String)
case class IfapaAemetFormatSingleStationMeteoInfoFilepaths(data: String, metadata: String)
case class IfapaAemetFormatSingleStationMeteoInfo(dirs: IfapaAemetFormatSingleStationMeteoInfoDirs, filenames: IfapaAemetFormatSingleStationMeteoInfoFilenames, filepaths: IfapaAemetFormatSingleStationMeteoInfoFilepaths)

case class IfapaAemetFormatSingleStationInfoDirs(data: String, metadata: String)
case class IfapaAemetFormatSingleStationInfoFilenames(data: String, metadata: String)
case class IfapaAemetFormatSingleStationInfoFilepaths(data: String, metadata: String)
case class IfapaAemetFormatSingleStationInfo(dirs: IfapaAemetFormatSingleStationInfoDirs, filenames: IfapaAemetFormatSingleStationInfoFilenames, filepaths: IfapaAemetFormatSingleStationInfoFilepaths)

case class IfapaAemetFormatConf(baseDir: String, singleStationMeteoInfo: IfapaAemetFormatSingleStationMeteoInfo, singleStationInfo: IfapaAemetFormatSingleStationInfo)
