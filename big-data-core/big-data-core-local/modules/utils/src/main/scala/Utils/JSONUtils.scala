package Utils

import scala.collection.mutable

/**
 * JSON utility helpers built on `ujson`.
 *
 * Offers functions to lowercase keys, build filtered JSON structures from a
 * schema and data relation, transform values by key and remove null keys.
 */
object JSONUtils {

  /**
   * Recursively lowercase all object keys in the provided `ujson.Value`.
   *
   * @param json the input `ujson.Value` to transform
   * @return a new `ujson.Value` with all object keys lowercased
   */
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

  /**
   * Build a JSON value by selecting fields from a `schemaJSON` according to
   * a `schemaDataRelation` map that provides actual values for leaf keys.
   *
   * @param schemaJSON schema-like `ujson.Value` describing the desired shape
   * @param schemaDataRelation mapping from schema key to concrete `ujson.Value`
   * @return built `ujson.Value` populated with values from `schemaDataRelation`
   */
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

  /**
   * Recursively transform values for keys present in the `transformation` map.
   *
   * @param json the input `ujson.Value`
   * @param transformation mapping from key `String` to a transformation function
   *                       that receives a `ujson.Value` and returns a `ujson.Value`
   * @return transformed `ujson.Value`
   */
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

  /**
   * Remove keys with `null` (`ujson.Null`) values from the provided JSON tree.
   *
   * @param json the input `ujson.Value`
   * @return a `ujson.Value` with `null` entries removed
   */
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
