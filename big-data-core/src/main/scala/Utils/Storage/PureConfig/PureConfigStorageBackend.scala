package Utils.Storage.PureConfig

import com.typesafe.config.{ConfigFactory, Config => TypesafeConfig}
import pureconfig.error.CannotConvert
import pureconfig.{ConfigReader, ConfigSource}

import scala.reflect.ClassTag

object PureConfigStorageBackend {

  implicit val doubleWithInfReader: ConfigReader[Double] = ConfigReader.fromString {
    case "+inf" | "inf" | "∞" | "Infinity" => Right(Double.PositiveInfinity)
    case "-inf" | "-∞" | "-Infinity" => Right(Double.NegativeInfinity)
    case "NaN" => Right(Double.NaN)
    case s =>
      try Right(s.toDouble)
      catch {
        case _: NumberFormatException =>
          Left(CannotConvert(s, "Double", "Invalid format or not supported"))
      }
  }

  def readInternalConfig[T: ClassTag](filepath: String, customPrefix: Option[String] = None)(implicit reader: ConfigReader[T]): T = {
    val configFile: java.io.File = customPrefix match {
      case Some(prefix) => new java.io.File(prefix + filepath)
      case None => new java.io.File(getClass.getClassLoader.getResource(filepath).toURI)
    }
    val resolvedConfig: TypesafeConfig = ConfigFactory.parseFile(configFile).resolve()
    ConfigSource.fromConfig(resolvedConfig).loadOrThrow[T]
  }
}