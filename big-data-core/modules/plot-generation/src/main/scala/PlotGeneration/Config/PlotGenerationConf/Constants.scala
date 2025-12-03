package PlotGeneration.Config.PlotGenerationConf

import PlotGeneration.Config.GlobalConf
import Utils.Storage.Core
import Utils.Storage.PureConfig.PureConfigStorageBackend.readInternalConfig
import pureconfig.generic.auto._

case class ExecutionConf(stationsConf: Execution.StationsConf, climographConf: Execution.ClimographConf, singleParamStudiesConf: Execution.SingleParamStudiesConf, interestingStudiesConf: Execution.InterestingStudiesConf, globalConf: Execution.GlobalConf)
case class StorageConf(stationsConf: Storage.StationsConf, climographConf: Storage.ClimographConf, singleParamStudiesConf: Storage.SingleParamStudiesConf, interestingStudiesConf: Storage.InterestingStudiesConf, globalConf: Storage.GlobalConf)
case class LogConf(stationsConf: Log.StationsConf, climographConf: Log.ClimographConf, singleParamStudiesConf: Log.SingleParamStudiesConf, interestingStudiesConf: Log.InterestingStudiesConf, globalConf: Log.GlobalConf)
case class UrlConf(globalConf: Url.GlobalConf)

object Constants {
  private val ctsGlobalInit = GlobalConf.Constants.init
  private implicit val dataStorage: Core.Storage = GlobalConf.Constants.dataStorage

  private val configDirPath: String = dataStorage.readDirectoryRecursive(
    s"${ctsGlobalInit.storageBaseData}config",
    includeDirs = Seq("/config/global", "/config/plot_generation")
  ).toString.replace("\\", "/").concat("/")

  val execution: ExecutionConf = ExecutionConf(
    stationsConf = readInternalConfig[Execution.StationsConf]("config/plot_generation/execution/stations.conf", Some(configDirPath)),
    climographConf = readInternalConfig[Execution.ClimographConf]("config/plot_generation/execution/climograph.conf", Some(configDirPath)),
    singleParamStudiesConf = readInternalConfig[Execution.SingleParamStudiesConf]("config/plot_generation/execution/single-param-studies.conf", Some(configDirPath)),
    interestingStudiesConf = readInternalConfig[Execution.InterestingStudiesConf]("config/plot_generation/execution/interesting-studies.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Execution.GlobalConf]("config/plot_generation/execution/global.conf", Some(configDirPath)),
  )

  val storage: StorageConf = StorageConf(
    stationsConf = readInternalConfig[Storage.StationsConf]("config/plot_generation/storage/stations.conf", Some(configDirPath)),
    climographConf = readInternalConfig[Storage.ClimographConf]("config/plot_generation/storage/climograph.conf", Some(configDirPath)),
    singleParamStudiesConf = readInternalConfig[Storage.SingleParamStudiesConf]("config/plot_generation/storage/single-param-studies.conf", Some(configDirPath)),
    interestingStudiesConf = readInternalConfig[Storage.InterestingStudiesConf]("config/plot_generation/storage/interesting-studies.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Storage.GlobalConf]("config/plot_generation/storage/global.conf", Some(configDirPath))
  )

  val log: LogConf = LogConf(
    stationsConf = readInternalConfig[Log.StationsConf]("config/plot_generation/log/stations.conf", Some(configDirPath)),
    climographConf = readInternalConfig[Log.ClimographConf]("config/plot_generation/log/climograph.conf", Some(configDirPath)),
    singleParamStudiesConf = readInternalConfig[Log.SingleParamStudiesConf]("config/plot_generation/log/single-param-studies.conf", Some(configDirPath)),
    interestingStudiesConf = readInternalConfig[Log.InterestingStudiesConf]("config/plot_generation/log/interesting-studies.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Log.GlobalConf]("config/plot_generation/log/global.conf", Some(configDirPath))
  )

  val url: UrlConf = UrlConf(
    globalConf = readInternalConfig[Url.GlobalConf]("config/plot_generation/url/global.conf", Some(configDirPath)),
  )

  dataStorage.deleteLocalDirectoryRecursive(configDirPath)
}