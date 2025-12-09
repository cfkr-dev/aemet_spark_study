package Spark.Config.SparkConf

import Spark.Config.GlobalConf
import Utils.Storage.Core
import Utils.Storage.PureConfig.PureConfigStorageBackend.readInternalConfig
import pureconfig.generic.auto._
import Utils.Storage.PureConfig.PureConfigStorageBackend.doubleWithInfReader

case class QueriesExecutionConf(
  climographConf: Queries.Execution.ClimographConf,
  interestingStudiesConf: Queries.Execution.InterestingStudiesConf,
  singleParamStudiesConf: Queries.Execution.SingleParamStudiesConf,
  stationsConf: Queries.Execution.StationsConf
)
case class QueriesLogConf(
  climographConf: Queries.Log.ClimographConf,
  globalConf: Queries.Log.GlobalConf,
  interestingStudiesConf: Queries.Log.InterestingStudiesConf,
  singleParamStudiesConf: Queries.Log.SingleParamStudiesConf,
  stationsConf: Queries.Log.StationsConf
)
case class QueriesStorageConf(
  climographConf: Queries.Storage.ClimographConf,
  globalConf: Queries.Storage.GlobalConf,
  interestingStudiesConf: Queries.Storage.InterestingStudiesConf,
  singleParamStudiesConf: Queries.Storage.SingleParamStudiesConf,
  stationsConf: Queries.Storage.StationsConf
)

case class QueriesConf(
  execution: QueriesExecutionConf,
  log: QueriesLogConf,
  storage: QueriesStorageConf
)

case class InitExecutionConf(
  dataframeConf: Init.Execution.DataframeConf,
  globalConf: Init.Execution.GlobalConf,
  sessionConf: Init.Execution.SessionConf
)

case class InitLogConf(
  sessionConf: Init.Log.SessionConf
)

case class InitStorageConf(
  aemetConf: Init.Storage.AemetConf,
  globalConf: Init.Storage.GlobalConf,
  ifapaAemetFormatConf: Init.Storage.IfapaAemetFormatConf,
)

case class InitConf(
  execution: InitExecutionConf,
  log: InitLogConf,
  storage: InitStorageConf
)

object Constants {
  private val storageBaseData = GlobalConf.Constants.storageBaseData
  private implicit val dataStorage: Core.Storage = GlobalConf.Constants.dataStorage

  private val configDirPath: String = dataStorage.readDirectoryRecursive(
    s"$storageBaseData/config",
    includeDirs = Seq("/config/global", "/config/spark")
  ).toString.replace("\\", "/").concat("/")

  val init: InitConf = InitConf(
    execution = InitExecutionConf(
      dataframeConf = readInternalConfig[Init.Execution.DataframeConf]("spark/init/execution/dataframe.conf", Some(configDirPath)),
      globalConf = readInternalConfig[Init.Execution.GlobalConf]("spark/init/execution/global.conf", Some(configDirPath)),
      sessionConf = readInternalConfig[Init.Execution.SessionConf]("spark/init/execution/session.conf", Some(configDirPath)),
    ),
    log = InitLogConf(
      sessionConf = readInternalConfig[Init.Log.SessionConf]("spark/init/log/session.conf", Some(configDirPath)),
    ),
    storage = InitStorageConf(
      aemetConf = readInternalConfig[Init.Storage.AemetConf]("spark/init/storage/aemet.conf", Some(configDirPath)),
      globalConf = readInternalConfig[Init.Storage.GlobalConf]("spark/init/storage/global.conf", Some(configDirPath)),
      ifapaAemetFormatConf = readInternalConfig[Init.Storage.IfapaAemetFormatConf]("spark/init/storage/ifapa-aemet-format.conf", Some(configDirPath)),
    )
  )

  val queries: QueriesConf = QueriesConf(
    execution = QueriesExecutionConf(
      climographConf = readInternalConfig[Queries.Execution.ClimographConf]("spark/queries/execution/climograph.conf", Some(configDirPath)),
      interestingStudiesConf = readInternalConfig[Queries.Execution.InterestingStudiesConf]("spark/queries/execution/interesting-studies.conf", Some(configDirPath)),
      singleParamStudiesConf = readInternalConfig[Queries.Execution.SingleParamStudiesConf]("spark/queries/execution/single-param-studies.conf", Some(configDirPath)),
      stationsConf = readInternalConfig[Queries.Execution.StationsConf]("spark/queries/execution/stations.conf", Some(configDirPath))
    ),
    log = QueriesLogConf(
      climographConf = readInternalConfig[Queries.Log.ClimographConf]("spark/queries/log/climograph.conf", Some(configDirPath)),
      globalConf = readInternalConfig[Queries.Log.GlobalConf]("spark/queries/log/global.conf", Some(configDirPath)),
      interestingStudiesConf = readInternalConfig[Queries.Log.InterestingStudiesConf]("spark/queries/log/interesting-studies.conf", Some(configDirPath)),
      singleParamStudiesConf = readInternalConfig[Queries.Log.SingleParamStudiesConf]("spark/queries/log/single-param-studies.conf", Some(configDirPath)),
      stationsConf = readInternalConfig[Queries.Log.StationsConf]("spark/queries/log/stations.conf", Some(configDirPath))
    ),
    storage = QueriesStorageConf(
      climographConf = readInternalConfig[Queries.Storage.ClimographConf]("spark/queries/storage/climograph.conf", Some(configDirPath)),
      globalConf = readInternalConfig[Queries.Storage.GlobalConf]("spark/queries/storage/global.conf", Some(configDirPath)),
      interestingStudiesConf = readInternalConfig[Queries.Storage.InterestingStudiesConf]("spark/queries/storage/interesting-studies.conf", Some(configDirPath)),
      singleParamStudiesConf = readInternalConfig[Queries.Storage.SingleParamStudiesConf]("spark/queries/storage/single-param-studies.conf", Some(configDirPath)),
      stationsConf = readInternalConfig[Queries.Storage.StationsConf]("spark/queries/storage/stations.conf", Some(configDirPath))
    )
  )

  dataStorage.deleteLocalDirectoryRecursive(configDirPath)
}

