package Config.SparkConf

import Utils.PureConfigUtils.readConfigFromFile
import pureconfig.generic.auto._
import Utils.PureConfigUtils.doubleWithInfReader

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
  val init: InitConf = InitConf(
    execution = InitExecutionConf(
      dataframeConf = readConfigFromFile[Init.Execution.DataframeConf]("config/spark/init/execution/dataframe.conf"),
      globalConf = readConfigFromFile[Init.Execution.GlobalConf]("config/spark/init/execution/global.conf"),
      sessionConf = readConfigFromFile[Init.Execution.SessionConf]("config/spark/init/execution/session.conf"),
    ),
    log = InitLogConf(
      sessionConf = readConfigFromFile[Init.Log.SessionConf]("config/spark/init/log/session.conf"),
    ),
    storage = InitStorageConf(
      aemetConf = readConfigFromFile[Init.Storage.AemetConf]("config/spark/init/storage/aemet.conf"),
      globalConf = readConfigFromFile[Init.Storage.GlobalConf]("config/spark/init/storage/global.conf"),
      ifapaAemetFormatConf = readConfigFromFile[Init.Storage.IfapaAemetFormatConf]("config/spark/init/storage/ifapaAemetFormat.conf"),
    )
  )

  val queries: QueriesConf = QueriesConf(
    execution = QueriesExecutionConf(
      climographConf = readConfigFromFile[Queries.Execution.ClimographConf]("config/spark/queries/execution/climograph.conf"),
      interestingStudiesConf = readConfigFromFile[Queries.Execution.InterestingStudiesConf]("config/spark/queries/execution/interesting-studies.conf"),
      singleParamStudiesConf = readConfigFromFile[Queries.Execution.SingleParamStudiesConf]("config/spark/queries/execution/single-param-studies.conf"),
      stationsConf = readConfigFromFile[Queries.Execution.StationsConf]("config/spark/queries/execution/stations.conf")
    ),
    log = QueriesLogConf(
      climographConf = readConfigFromFile[Queries.Log.ClimographConf]("config/spark/queries/log/climograph.conf"),
      globalConf = readConfigFromFile[Queries.Log.GlobalConf]("config/spark/queries/log/global.conf"),
      interestingStudiesConf = readConfigFromFile[Queries.Log.InterestingStudiesConf]("config/spark/queries/log/interesting-studies.conf"),
      singleParamStudiesConf = readConfigFromFile[Queries.Log.SingleParamStudiesConf]("config/spark/queries/log/single-param-studies.conf"),
      stationsConf = readConfigFromFile[Queries.Log.StationsConf]("config/spark/queries/log/stations.conf")
    ),
    storage = QueriesStorageConf(
      climographConf = readConfigFromFile[Queries.Storage.ClimographConf]("config/spark/queries/storage/climograph.conf"),
      globalConf = readConfigFromFile[Queries.Storage.GlobalConf]("config/spark/queries/storage/global.conf"),
      interestingStudiesConf = readConfigFromFile[Queries.Storage.InterestingStudiesConf]("config/spark/queries/storage/interesting-studies.conf"),
      singleParamStudiesConf = readConfigFromFile[Queries.Storage.SingleParamStudiesConf]("config/spark/queries/storage/single-param-studies.conf"),
      stationsConf = readConfigFromFile[Queries.Storage.StationsConf]("config/spark/queries/storage/stations.conf")
    )
  )
}

