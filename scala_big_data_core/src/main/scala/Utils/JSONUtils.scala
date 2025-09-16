package Utils

import ujson.{Obj, write}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.io.Source

object JSONUtils {

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

  def readJSON(path: String, findHeaviest: Boolean = false): Either[Exception, ujson.Value] = {
    def readFile(filePath: String): Either[Exception, ujson.Value] = {
      val source = Source.fromFile(filePath)
      try {
        Right(ujson.read(source.mkString))
      } catch {
        case exception: Exception => Left(exception)
      } finally {
        source.close()
      }
    }

    val file = new File(path)

    if (file.exists && file.isDirectory && findHeaviest) {
      file.listFiles
        .filter(f => f.isFile && f.getName.toLowerCase.endsWith(".json"))
        .maxByOption(_.length)
        .map(f => readFile(f.getAbsolutePath))
        .getOrElse(Left(new RuntimeException("No JSON files found in directory")))
    } else if (file.exists && file.isFile) {
      readFile(file.getAbsolutePath)
    } else {
      Left(new RuntimeException("Invalid path or not permitted usage"))
    }
  }

  def lowercaseKeys(json: ujson.Value): ujson.Value = {
    json match {
      case obj: ujson.Obj => ujson.Obj.from(
        mutable.LinkedHashMap(obj.value.toSeq: _*).map {
          case (key, value) => key.toLowerCase -> lowercaseKeys(value)
        }
      )

      case arr: ujson.Arr => ujson.Arr(
        arr.value.map(lowercaseKeys)
      )

      case other => other
    }
  }

  def buildJSONFromSchemaAndData(
    schemaJSON: ujson.Value,
    schemaDataRelation: Map[String, ujson.Value]
  ): ujson.Value = {
    schemaJSON match {
      case ujson.Obj(fields) => ujson.Obj.from(
        fields.toList.collect {
          case (key: String, ujson.Obj(value)) if schemaDataRelation.contains(key) =>
            key -> buildJSONFromSchemaAndData(value, schemaDataRelation)
          case (key: String, ujson.Arr(value)) if schemaDataRelation.contains(key) =>
            key -> buildJSONFromSchemaAndData(value, schemaDataRelation)
          case (key: String, _) if schemaDataRelation.contains(key) =>
            key -> schemaDataRelation(key)
        }
      )
      case ujson.Arr(elements) => ujson.Arr(
        elements.map(element => buildJSONFromSchemaAndData(element, schemaDataRelation))
      )
      case other => other
    }
  }

  def transformJSONValues(json: ujson.Value, transformation: Map[String, ujson.Value => ujson.Value]): ujson.Value = {
    json match {
      case ujson.Obj(fields) => ujson.Obj.from(
        fields.toList.collect {
          case (key: String, ujson.Obj(value)) if transformation.contains(key) =>
            key -> transformJSONValues(value, transformation)
          case (key: String, ujson.Arr(value)) if transformation.contains(key) =>
            key -> transformJSONValues(value, transformation)
          case (key: String, value: ujson.Value) if transformation.contains(key) =>
            key -> transformation(key)(value)
        }
      )
      case ujson.Arr(elements) => ujson.Arr(
        elements.map(element => transformJSONValues(element, transformation))
      )
      case other => other
    }
  }

  def removeNullKeys(json: ujson.Value): ujson.Value = json match {
    case obj: ujson.Obj =>
      ujson.Obj.from(
        obj.value.toSeq.collect {
          case (key, value) if value != ujson.Null =>
            key -> removeNullKeys(value)
        }
      )
    case arr: ujson.Arr =>
      ujson.Arr(arr.value.map(removeNullKeys))
    case other => other
  }

}
