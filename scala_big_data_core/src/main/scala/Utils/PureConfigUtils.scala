package Utils

import com.typesafe.config.{ConfigFactory, Config => TypesafeConfig}
import pureconfig.error.CannotConvert
import pureconfig.{ConfigReader, ConfigSource}

import scala.reflect.ClassTag

object PureConfigUtils {
  implicit val doubleWithInfReader: ConfigReader[Double] = ConfigReader.fromString {
    case "+inf" | "inf" | "∞" | "Infinity" => Right(Double.PositiveInfinity)
    case "-inf" | "-∞" | "-Infinity" => Right(Double.NegativeInfinity)
    case "NaN" => Right(Double.NaN)
    case s =>
      try Right(s.toDouble)
      catch {
        case _: NumberFormatException =>
          Left(CannotConvert(s, "Double", "Formato inválido o no soportado"))
      }
  }

  def readConfigFromFile[T: ClassTag](filepath: String)(implicit reader: ConfigReader[T]): T = {
    val configFile: java.io.File = new java.io.File(getClass.getClassLoader.getResource(filepath).toURI)
    val resolvedConfig: TypesafeConfig = ConfigFactory.parseFile(configFile).resolve()
    ConfigSource.fromConfig(resolvedConfig).loadOrThrow[T]
  }
}
