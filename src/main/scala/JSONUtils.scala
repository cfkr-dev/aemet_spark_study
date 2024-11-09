import ujson.{Value, write}

import java.io.{BufferedWriter, File, FileWriter}

object JSONUtils {

  @deprecated
  object Aemet {
    @deprecated
    def metadataToNamesToTypes(metadata: ujson.Value): Map[String, String] = {
      metadata("campos").arr.map {
        campo => campo("id").str -> campo("tipo_datos").str
      }.toMap
    }
  }


  def writeJSON(file: File, json: ujson.Value, append: Boolean = false): Either[Exception, String] = {
    try {
      val bufferedWriter = new BufferedWriter(new FileWriter(file, append))

      bufferedWriter.write(write(json, 2))
      bufferedWriter.flush()
      bufferedWriter.close()

      Right(file.getPath)
    } catch {
      case exception: Exception => Left(exception)
    }
  }

  @deprecated
  def cast(json: ujson.Value, typesToCaster: Map[String, ujson.Value => ujson.Value], namesToType: Map[String, String]): ujson.Value = {
    json match {
      case obj: ujson.Obj => ujson.Obj.from(
        obj.value.map {
          case (key, value) => key -> cast(typesToCaster.get(namesToType.getOrElse(key.toLowerCase, identity(key))) match {
            case Some(caster) => caster(value)
            case None => value
          }, typesToCaster, namesToType)
        }
      )

      case arr: ujson.Arr => ujson.Arr(
        arr.value.map {
          element => cast(element, typesToCaster, namesToType)
        }
      )

      case other => other
    }
  }

}
