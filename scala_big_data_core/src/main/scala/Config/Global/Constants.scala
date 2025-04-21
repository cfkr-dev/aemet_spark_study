package Config.Global

import Utils.PureConfigUtils.readConfigFromFile
import pureconfig.generic.auto._

case class SchemaConf(aemetConf: Schema.AemetConf, ifapaConf: Schema.IfapaConf, sparkConf: Schema.SparkConf)

object Constants {
  val schema: SchemaConf = SchemaConf(
    aemetConf = readConfigFromFile[Schema.AemetConf]("config/global/schema/aemet.conf"),
    ifapaConf = readConfigFromFile[Schema.IfapaConf]("config/global/schema/ifapa.conf"),
    sparkConf = readConfigFromFile[Schema.SparkConf]("config/global/schema/spark.conf"),
  )

  val storage: StorageConf = readConfigFromFile[StorageConf]("config/global/storage.conf")

  val utils: UtilsConf = readConfigFromFile[UtilsConf]("config/global/utils.conf")
}