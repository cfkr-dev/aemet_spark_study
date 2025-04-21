package Config.SparkConf

import Utils.PureConfigUtils.readConfigFromFile
import pureconfig.generic.auto._
import Utils.PureConfigUtils.doubleWithInfReader

case class ExecutionConf(
  climographConf: Execution.ClimographConf,
  interestingStudiesConf: Execution.InterestingStudiesConf,
  singleParamStudiesConf: Execution.SingleParamStudiesConf,
  stationsConf: Execution.StationsConf
)
case class LogConf(
  climographConf: Log.ClimographConf,
  globalConf: Log.GlobalConf,
  interestingStudiesConf: Log.InterestingStudiesConf,
  singleParamStudiesConf: Log.SingleParamStudiesConf,
  stationsConf: Log.StationsConf
)
case class StorageConf(
  climographConf: Storage.ClimographConf,
  globalConf: Storage.GlobalConf,
  interestingStudiesConf: Storage.InterestingStudiesConf,
  singleParamStudiesConf: Storage.SingleParamStudiesConf,
  stationsConf: Storage.StationsConf
)

object Constants {
  val execution: ExecutionConf = ExecutionConf(
    climographConf = readConfigFromFile[Execution.ClimographConf]("config/spark/execution/climograph.conf"),
    interestingStudiesConf = readConfigFromFile[Execution.InterestingStudiesConf]("config/spark/execution/interesting-studies.conf"),
    singleParamStudiesConf = readConfigFromFile[Execution.SingleParamStudiesConf]("config/spark/execution/single-param-studies.conf"),
    stationsConf = readConfigFromFile[Execution.StationsConf]("config/spark/execution/stations.conf")
  )

  val log: LogConf = LogConf(
    climographConf = readConfigFromFile[Log.ClimographConf]("config/spark/log/climograph.conf"),
    globalConf = readConfigFromFile[Log.GlobalConf]("config/spark/log/global.conf"),
    interestingStudiesConf = readConfigFromFile[Log.InterestingStudiesConf]("config/spark/log/interesting-studies.conf"),
    singleParamStudiesConf = readConfigFromFile[Log.SingleParamStudiesConf]("config/spark/log/single-param-studies.conf"),
    stationsConf = readConfigFromFile[Log.StationsConf]("config/spark/log/stations.conf")
  )

  val storage: StorageConf = StorageConf(
    climographConf = readConfigFromFile[Storage.ClimographConf]("config/spark/storage/climograph.conf"),
    globalConf = readConfigFromFile[Storage.GlobalConf]("config/spark/storage/global.conf"),
    interestingStudiesConf = readConfigFromFile[Storage.InterestingStudiesConf]("config/spark/storage/interesting-studies.conf"),
    singleParamStudiesConf = readConfigFromFile[Storage.SingleParamStudiesConf]("config/spark/storage/single-param-studies.conf"),
    stationsConf = readConfigFromFile[Storage.StationsConf]("config/spark/storage/stations.conf")
  )
}

