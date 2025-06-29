package Config.PlotGenerationConf

import Utils.PureConfigUtils.readConfigFromFile
import pureconfig.generic.auto._

case class ExecutionConf(stationsConf: Execution.StationsConf)
case class StorageConf(stationsConf: Storage.StationsConf, globalConf: Storage.GlobalConf)
case class UrlConf(globalConf: Url.GlobalConf)

object Constants {
  val execution: ExecutionConf = ExecutionConf(
    stationsConf = readConfigFromFile[Execution.StationsConf]("config/plot_generation/execution/stations.conf"),
  )

  val storage: StorageConf = StorageConf(
    stationsConf = readConfigFromFile[Storage.StationsConf]("config/plot_generation/storage/stations.conf"),
    globalConf = readConfigFromFile[Storage.GlobalConf]("config/plot_generation/storage/global.conf")
  )

  val url: UrlConf = UrlConf(
    globalConf = readConfigFromFile[Url.GlobalConf]("config/plot_generation/url/global.conf"),
  )

  val utils: UtilsConf = readConfigFromFile[UtilsConf]("config/plot_generation/utils.conf")
}