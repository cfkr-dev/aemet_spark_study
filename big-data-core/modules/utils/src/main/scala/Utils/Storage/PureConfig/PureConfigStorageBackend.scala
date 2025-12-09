package Utils.Storage.PureConfig

import com.typesafe.config.{ConfigFactory, Config => TypesafeConfig}
import pureconfig.error.CannotConvert
import pureconfig.{ConfigReader, ConfigSource}

import java.io.InputStream
import scala.io.Source
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

  def readInternalConfig[T: ClassTag](
    filepath: String,
    customPrefix: Option[String] = None
  )(implicit reader: ConfigReader[T]): T = {

    val config: TypesafeConfig = customPrefix match {
      case Some(prefix) =>
        val configFile = new java.io.File(prefix, filepath)
        if (!configFile.exists())
          throw new RuntimeException(s"Config file not found: ${configFile.getAbsolutePath}")
        ConfigFactory.parseFile(configFile).resolve()

      case None =>
        val resource: InputStream = Option(getClass.getClassLoader.getResourceAsStream(filepath))
          .getOrElse(throw new RuntimeException(s"Config file '$filepath' not found in classpath"))

        val reader = Source.fromInputStream(resource).bufferedReader()
        try {
          ConfigFactory.parseReader(reader).resolve()
        } finally {
          reader.close()
          resource.close()
        }
    }

    ConfigSource.fromConfig(config).loadOrThrow[T]
  }
}