package Config.PlotGenerationConf

import Utils.PureConfigUtils.readConfigFromFile
import pureconfig.generic.auto._

case class ExecutionConf(stationsConf: Execution.StationsConf, climographConf: Execution.ClimographConf, singleParamStudiesConf: Execution.SingleParamStudiesConf)
case class StorageConf(stationsConf: Storage.StationsConf, climographConf: Storage.ClimographConf, singleParamStudiesConf: Storage.SingleParamStudiesConf, globalConf: Storage.GlobalConf)
case class LogConf(stationsConf: Log.StationsConf, climographConf: Log.ClimographConf, singleParamStudiesConf: Log.SingleParamStudiesConf, globalConf: Log.GlobalConf)
case class UrlConf(globalConf: Url.GlobalConf)

object Constants {
  val execution: ExecutionConf = ExecutionConf(
    stationsConf = readConfigFromFile[Execution.StationsConf]("config/plot_generation/execution/stations.conf"),
    climographConf = readConfigFromFile[Execution.ClimographConf]("config/plot_generation/execution/climograph.conf"),
    singleParamStudiesConf = readConfigFromFile[Execution.SingleParamStudiesConf]("config/plot_generation/execution/single-param-studies.conf")
  )

  val storage: StorageConf = StorageConf(
    stationsConf = readConfigFromFile[Storage.StationsConf]("config/plot_generation/storage/stations.conf"),
    climographConf = readConfigFromFile[Storage.ClimographConf]("config/plot_generation/storage/climograph.conf"),
    singleParamStudiesConf = readConfigFromFile[Storage.SingleParamStudiesConf]("config/plot_generation/storage/single-param-studies.conf"),
    globalConf = readConfigFromFile[Storage.GlobalConf]("config/plot_generation/storage/global.conf")
  )

  val log: LogConf = LogConf(
    stationsConf = readConfigFromFile[Log.StationsConf]("config/plot_generation/log/stations.conf"),
    climographConf = readConfigFromFile[Log.ClimographConf]("config/plot_generation/log/climograph.conf"),
    singleParamStudiesConf = readConfigFromFile[Log.SingleParamStudiesConf]("config/plot_generation/log/single-param-studies.conf"),
    globalConf = readConfigFromFile[Log.GlobalConf]("config/plot_generation/log/global.conf")
  )

  val url: UrlConf = UrlConf(
    globalConf = readConfigFromFile[Url.GlobalConf]("config/plot_generation/url/global.conf"),
  )

  val utils: UtilsConf = readConfigFromFile[UtilsConf]("config/plot_generation/utils.conf")
}