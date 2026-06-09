/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.smartdatalake.meta.jsonschema

import io.smartdatalake.meta.jsonschema.JsonTypeEnum.JsonTypeEnum
import io.smartdatalake.util.misc.ProductUtil
import org.apache.commons.lang.NotImplementedException
import org.json4s._
import org.json4s.jackson.Serialization

import scala.collection.AbstractMap
import scala.collection.immutable.ListMap

/**
 * Enumeration of JSON schema types
 */
private[smartdatalake] object JsonTypeEnum extends Enumeration {
  type JsonTypeEnum = Value
  val Object: Value = Value("object")
  val Array: Value = Value("array")
  val String: Value = Value("string")
  val Integer: Value = Value("integer")
  val Number: Value = Value("number")
  val Boolean: Value = Value("boolean")
}

/**
 * Supertype of json schema definition elements
 */
private[smartdatalake] sealed trait JsonTypeDef extends JsonExtractor

/**
 * Definition of a JSON object
 * @param properties: ListMap ensures that property ordering is kept. It has to be lazy to break recursive conversion.
 */
private[smartdatalake] case class JsonObjectDef(
                          properties: LazyListMapWrapper[String,JsonTypeDef],
                          title: String,
                          required: Seq[String] = Seq(),
                          additionalProperties: Boolean = false,
                          description: Option[String] = None,
                          deprecated: Option[Boolean] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Object)
}

/**
 * Definition of a JSON array
 */
private[smartdatalake] case class JsonArrayDef(
                         items: JsonTypeDef,
                         description: Option[String],
                         deprecated: Option[Boolean] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Array)
}

/**
 * Definition of a JSON string
 */
private[smartdatalake] case class JsonStringDef(
                          description: Option[String] = None,
                          default: Option[String] = None,
                          `enum`: Option[Seq[String]] = None,
                          existingJavaType: Option[String] = None,
                          deprecated: Option[Boolean] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.String)
}

/**
 * Definition of a JSON number
 */
private[smartdatalake] case class JsonNumberDef(
                          description: Option[String] = None,
                          default: Option[Int] = None,
                          deprecated: Option[Boolean] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Number)
}

/**
 * Definition of a JSON integer
 */
private[smartdatalake] case class JsonIntegerDef(
                          description: Option[String] = None,
                          default: Option[Int] = None,
                          deprecated: Option[Boolean] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Integer)
}

/**
 * Definition of a JSON boolean
 */
private[smartdatalake] case class JsonBooleanDef(
                           description: Option[String] = None,
                           default: Option[Boolean] = None,
                           deprecated: Option[Boolean] = None
                         ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Boolean)
}

/**
 * Definition of a JSON constant
 */
private[smartdatalake] case class JsonConstDef(
                         const: String
                       ) extends JsonTypeDef

/**
 * Definition of a JSON reference, which refers a type in the global definition.
 * @param `$ref` reference to global definition. Example: #/definitions/[typename]
 */
private[smartdatalake] case class JsonRefDef(
                       `$ref`: String,
                       description: Option[String] = None,
                       deprecated: Option[Boolean] = None
                     ) extends JsonTypeDef

/**
 * Definition of a JSON union: this allows one of the defined types.
 */
private[smartdatalake] case class JsonOneOfDef(
                         oneOf: Seq[JsonTypeDef],
                         description: Option[String] = None,
                         deprecated: Option[Boolean] = None
                       ) extends JsonTypeDef

/**
 * A Map is an object with restricted value types.
 * This can be created in JSON schema by limiting the type of additional properties.
 */
private[smartdatalake] case class JsonMapDef(
                       additionalProperties: JsonTypeDef,
                       description: Option[String] = None,
                       deprecated: Option[Boolean] = None
                     ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Object)
}

/**
 * Supertype of JSON schema definition root elements
 */
private[smartdatalake] trait SchemaRootDef extends JsonExtractor

/**
 * JSON schema root element that starts the schema with a JSON object.
 */
private[smartdatalake] case class SchemaRootObjectDef(
                                `$schema`: String,
                                version: String,
                                id: String,
                                definitions: Map[String, ListMap[String,JsonTypeDef]],
                                properties: ListMap[String,JsonTypeDef],
                                required: Seq[String],
                                additionalProperties: Boolean,
                                title: Option[String] = None
                              ) extends SchemaRootDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Object)
}

/**
 * Mixin to convert JSON schema elements to JSON syntax using json4s.
 */
private[smartdatalake] trait JsonExtractor {
  val `type`: Option[JsonTypeEnum] = None

  /**
   * create json4s tree
   */
  def toJson: JValue = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints) + JsonExtractor.jsonTypeDefSerializer()
    Extraction.decompose(this)
  }
}
private[smartdatalake] object JsonExtractor {
  /**
   * Custom serializer adds type-attribute if defined and ignores empty attributes
   */
  private def jsonTypeDefSerializer() = new CustomSerializer[JsonTypeDef](format => {
    val serializer: PartialFunction[Any, JValue] = {
      case obj: JsonExtractor =>
        val attributes = ProductUtil.attributesWithValuesForCaseClass(obj)
          .filter {
            case (_, None) => false
            case (_, v: Iterable[_]) if v.isEmpty
            => false
            case _ => true
          }
          .map { case (k, v) => (k, Extraction.decompose(v)(format)) }
        val jsonObj = if (obj.`type`.isDefined) JObject(("type", JString(obj.`type`.get.toString)) +: attributes)
        else JObject(attributes)
        jsonObj
    }
    val deserializer: PartialFunction[JValue, JsonTypeDef] = {
      case _ => throw new NotImplementedException
    }
    (deserializer, serializer)
  })
}

/**
 * LazyListMapWrapper is used to break recursive conversion.
 *
 * Note that it is difficult to find an implementation that compiles for Scala 2.12 and 2.13.
 * It's possible with collection.AbstractMap, but not collection.immutable.AbstractMap...
 */
private[smartdatalake] class LazyListMapWrapper[A,B](createFn: () => ListMap[A,B]) extends AbstractMap[A,B] with Serializable {
  private lazy val wrappedList: ListMap[A,B] = createFn()
  override def size: Int = wrappedList.size
  def get(key: A): Option[B] = wrappedList.get(key) // removed in 2.9: orElse Some(default(key))
  def iterator: Iterator[(A, B)] = wrappedList.iterator
  override def -(key: A): Map[A, B] = wrappedList -- Iterable(key)
  override def -(key1: A, key2: A, keys: A*): Map[A, B] = wrappedList -- (Seq(key1, key2) ++ keys)
  override def +[V1 >: B](kv: (A, V1)): collection.Map[A, V1] = wrappedList + kv
}