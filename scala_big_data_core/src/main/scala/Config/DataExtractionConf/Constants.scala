package Config.DataExtractionConf

import Utils.PureConfigUtils.readConfigFromFile
import pureconfig.generic.auto._

case class ExecutionConf(aemetConf: Execution.AemetConf, ifapaConf: Execution.IfapaConf, globalConf: Execution.GlobalConf)
case class LogConf(aemetConf: Log.AemetConf, ifapaConf: Log.IfapaConf, ifapaAemetFormatConf: Log.IfapaAemetFormatConf)
case class StorageConf(aemetConf: Storage.AemetConf, ifapaConf: Storage.IfapaConf, ifapaAemetFormatConf: Storage.IfapaAemetFormatConf, globalConf: Storage.GlobalConf)
case class UrlConf(aemetConf: Url.AemetConf, ifapaConf: Url.IfapaConf)

object Constants {
  val execution: ExecutionConf = ExecutionConf(
    aemetConf = readConfigFromFile[Execution.AemetConf]("config/data_extraction/execution/aemet.conf"),
    ifapaConf = readConfigFromFile[Execution.IfapaConf]("config/data_extraction/execution/ifapa.conf"),
    globalConf = readConfigFromFile[Execution.GlobalConf]("config/data_extraction/execution/global.conf"),
  )

  val log: LogConf = LogConf(
    aemetConf = readConfigFromFile[Log.AemetConf]("config/data_extraction/log/aemet.conf"),
    ifapaConf = readConfigFromFile[Log.IfapaConf]("config/data_extraction/log/ifapa.conf"),
    ifapaAemetFormatConf = readConfigFromFile[Log.IfapaAemetFormatConf]("config/data_extraction/log/ifapa-aemet-format.conf"),
  )

  val storage: StorageConf = StorageConf(
    aemetConf = readConfigFromFile[Storage.AemetConf]("config/data_extraction/storage/aemet.conf"),
    ifapaConf = readConfigFromFile[Storage.IfapaConf]("config/data_extraction/storage/ifapa.conf"),
    ifapaAemetFormatConf = readConfigFromFile[Storage.IfapaAemetFormatConf]("config/data_extraction/storage/ifapa-aemet-format.conf"),
    globalConf = readConfigFromFile[Storage.GlobalConf]("config/data_extraction/storage/global.conf"),
  )

  val url: UrlConf = UrlConf(
    aemetConf = readConfigFromFile[Url.AemetConf]("config/data_extraction/url/aemet.conf"),
    ifapaConf = readConfigFromFile[Url.IfapaConf]("config/data_extraction/url/ifapa.conf")
  )
}
