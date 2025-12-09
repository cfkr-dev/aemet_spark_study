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
  private val storageBaseData = GlobalConf.Constants.storageBaseData
  private implicit val dataStorage: Core.Storage = GlobalConf.Constants.dataStorage

  private val configDirPath: String = dataStorage.readDirectoryRecursive(
    s"$storageBaseData/config",
    includeDirs = Seq("/config/global", "/config/plot_generation")
  ).toString.replace("\\", "/").concat("/")

  val execution: ExecutionConf = ExecutionConf(
    stationsConf = readInternalConfig[Execution.StationsConf]("plot_generation/execution/stations.conf", Some(configDirPath)),
    climographConf = readInternalConfig[Execution.ClimographConf]("plot_generation/execution/climograph.conf", Some(configDirPath)),
    singleParamStudiesConf = readInternalConfig[Execution.SingleParamStudiesConf]("plot_generation/execution/single-param-studies.conf", Some(configDirPath)),
    interestingStudiesConf = readInternalConfig[Execution.InterestingStudiesConf]("plot_generation/execution/interesting-studies.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Execution.GlobalConf]("plot_generation/execution/global.conf", Some(configDirPath)),
  )

  val storage: StorageConf = StorageConf(
    stationsConf = readInternalConfig[Storage.StationsConf]("plot_generation/storage/stations.conf", Some(configDirPath)),
    climographConf = readInternalConfig[Storage.ClimographConf]("plot_generation/storage/climograph.conf", Some(configDirPath)),
    singleParamStudiesConf = readInternalConfig[Storage.SingleParamStudiesConf]("plot_generation/storage/single-param-studies.conf", Some(configDirPath)),
    interestingStudiesConf = readInternalConfig[Storage.InterestingStudiesConf]("plot_generation/storage/interesting-studies.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Storage.GlobalConf]("plot_generation/storage/global.conf", Some(configDirPath))
  )

  val log: LogConf = LogConf(
    stationsConf = readInternalConfig[Log.StationsConf]("plot_generation/log/stations.conf", Some(configDirPath)),
    climographConf = readInternalConfig[Log.ClimographConf]("plot_generation/log/climograph.conf", Some(configDirPath)),
    singleParamStudiesConf = readInternalConfig[Log.SingleParamStudiesConf]("plot_generation/log/single-param-studies.conf", Some(configDirPath)),
    interestingStudiesConf = readInternalConfig[Log.InterestingStudiesConf]("plot_generation/log/interesting-studies.conf", Some(configDirPath)),
    globalConf = readInternalConfig[Log.GlobalConf]("plot_generation/log/global.conf", Some(configDirPath))
  )

  val url: UrlConf = UrlConf(
    globalConf = readInternalConfig[Url.GlobalConf]("plot_generation/url/global.conf", Some(configDirPath)),
  )

  dataStorage.deleteLocalDirectoryRecursive(configDirPath)
}