package PlotGeneration.Config.GlobalConf

import Utils.Storage.Core.Storage
import Utils.Storage.PureConfig.PureConfigStorageBackend.readInternalConfig
import pureconfig.generic.auto._

case class SchemaConf(aemetConf: Schema.AemetConf, ifapaConf: Schema.IfapaConf, sparkConf: Schema.SparkConf, autoPlotConf: Schema.AutoPlotConf)

object Constants {
  val init: InitConf = readInternalConfig[InitConf]("config/global/init.conf")
  val storageBaseData: String = init.storageBaseData.getOrElse(
    throw new Exception(s"Environment variable not found (${init.environmentVars.names.storageBase})")
  )

  implicit val dataStorage: Storage = Storage(
    init.environmentVars.values.storagePrefix,
    init.environmentVars.values.awsS3Endpoint
  )

  private val configDirPath: String = dataStorage.readDirectoryRecursive(
    s"$storageBaseData/config",
    includeDirs = Seq("/config/global")
  ).toString.replace("\\", "/").concat("/")

  val schema: SchemaConf = SchemaConf(
    aemetConf = readInternalConfig[Schema.AemetConf]("global/schema/aemet.conf", Some(configDirPath)),
    ifapaConf = readInternalConfig[Schema.IfapaConf]("global/schema/ifapa.conf", Some(configDirPath)),
    sparkConf = readInternalConfig[Schema.SparkConf]("global/schema/spark.conf", Some(configDirPath)),
    autoPlotConf = readInternalConfig[Schema.AutoPlotConf]("global/schema/autoplot.conf", Some(configDirPath)),
  )

  val utils: UtilsConf = readInternalConfig[UtilsConf]("global/utils.conf", Some(configDirPath))

  dataStorage.deleteLocalDirectoryRecursive(configDirPath)
}