package DataExtraction.Config.DataExtractionConf

import DataExtraction.Config.GlobalConf
import Utils.Storage.Core
import Utils.Storage.PureConfig.PureConfigStorageBackend.readInternalConfig
import pureconfig.generic.auto._

case class ExecutionConf(aemetConf: Execution.AemetConf, ifapaConf: Execution.IfapaConf, globalConf: Execution.GlobalConf)
case class LogConf(aemetConf: Log.AemetConf, ifapaConf: Log.IfapaConf, ifapaAemetFormatConf: Log.IfapaAemetFormatConf)
case class StorageConf(aemetConf: Storage.AemetConf, ifapaConf: Storage.IfapaConf, ifapaAemetFormatConf: Storage.IfapaAemetFormatConf, globalConf: Storage.GlobalConf)
case class UrlConf(aemetConf: Url.AemetConf, ifapaConf: Url.IfapaConf)

object Constants {
  private val ctsGlobalInit = GlobalConf.Constants.init
  private implicit val dataStorage: Core.Storage = GlobalConf.Constants.dataStorage

  private val configDirPath: String = dataStorage.readDirectoryRecursive(
    s"${ctsGlobalInit.storageBaseData}config",
    includeDirs = Seq("/config/global", "/config/data_extraction")
  ).toString.replace("\\", "/").concat("/")

  val execution: ExecutionConf = ExecutionConf(
    aemetConf = readInternalConfig[Execution.AemetConf]("data_extraction/execution/aemet.conf", Some(configDirPath)),
    ifapaConf = readInternalConfig[Execution.IfapaConf]("data_extraction/execution/ifapa.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Execution.GlobalConf]("data_extraction/execution/global.conf", Some(configDirPath)),
  )

  val log: LogConf = LogConf(
    aemetConf = readInternalConfig[Log.AemetConf]("data_extraction/log/aemet.conf", Some(configDirPath)),
    ifapaConf = readInternalConfig[Log.IfapaConf]("data_extraction/log/ifapa.conf", Some(configDirPath)),
    ifapaAemetFormatConf = readInternalConfig[Log.IfapaAemetFormatConf]("data_extraction/log/ifapa-aemet-format.conf", Some(configDirPath)),
  )

  val storage: StorageConf = StorageConf(
    aemetConf = readInternalConfig[Storage.AemetConf]("data_extraction/storage/aemet.conf", Some(configDirPath)),
    ifapaConf = readInternalConfig[Storage.IfapaConf]("data_extraction/storage/ifapa.conf", Some(configDirPath)),
    ifapaAemetFormatConf = readInternalConfig[Storage.IfapaAemetFormatConf]("data_extraction/storage/ifapa-aemet-format.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Storage.GlobalConf]("data_extraction/storage/global.conf", Some(configDirPath)),
  )

  val url: UrlConf = UrlConf(
    aemetConf = readInternalConfig[Url.AemetConf]("data_extraction/url/aemet.conf", Some(configDirPath)),
    ifapaConf = readInternalConfig[Url.IfapaConf]("data_extraction/url/ifapa.conf", Some(configDirPath))
  )

  dataStorage.deleteLocalDirectoryRecursive(configDirPath)
}
