package Utils

import ujson.{Obj, write}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.io.Source

object JSONUtils {

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
