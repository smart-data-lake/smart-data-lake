/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.ParsableFromConfig
import io.smartdatalake.meta.GenericTypeUtil
import io.smartdatalake.meta.jsonschema.JsonTypeEnum.JsonTypeEnum
import org.apache.commons.lang.NotImplementedException
import org.json4s._
import org.json4s.jackson.Serialization

import scala.collection.immutable.ListMap
import scala.reflect.runtime.universe.MethodSymbol

/**
 * Enumeration of json schema types
 */
private[smartdatalake] object JsonTypeEnum extends Enumeration {
  type JsonTypeEnum = Value
  val Object = Value("object")
  val Array = Value("array")
  val String = Value("string")
  val Integer = Value("integer")
  val Number = Value("number")
  val Boolean = Value("boolean")
  val Null = Value("null")
}

/**
 * Supertype of json schema definition elements
 */
private[smartdatalake] sealed trait JsonTypeDef extends JsonExtractor

/**
 * Definition of a json object
 */
private[smartdatalake] case class JsonObjectDef(
                          properties: ListMap[String,JsonTypeDef],
                          required: Seq[String] = Seq(),
                          additionalProperties: Boolean = false,
                          description: Option[String] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Object)
}

/**
 * Definition of a json array
 */
private[smartdatalake] case class JsonArrayDef(
                         items: JsonTypeDef,
                         description: Option[String]
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Array)
}

/**
 * Definition of a json string
 */
private[smartdatalake] case class JsonStringDef(
                          description: Option[String] = None,
                          default: Option[String] = None,
                          enum: Option[Seq[String]] = None,
                          existingJavaType: Option[String] = None
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.String)
}

/**
 * Definition of a json number
 */
private[smartdatalake] case class JsonNumberDef(
                          description: Option[String] = None,
                          default: Option[Int] = None,
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Number)
}

/**
 * Definition of a json integer
 */
private[smartdatalake] case class JsonIntegerDef(
                          description: Option[String] = None,
                          default: Option[Int] = None,
                        ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Integer)
}

/**
 * Definition of a json boolean
 */
private[smartdatalake] case class JsonBooleanDef(
                           description: Option[String] = None,
                           default: Option[Boolean] = None,
                         ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.String)
}

/**
 * Definition of a json empty object
 */
private[smartdatalake] case class JsonNullDef() extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Null)
}

/**
 * Definition of a json constant
 */
private[smartdatalake] case class JsonConstDef(
                         const: String
                       ) extends JsonTypeDef

/**
 * Definition of a json reference, which referes a type in the global definition.
 * @param `$ref` reference to global definition. Example: #/definitions/[typename]
 */
private[smartdatalake] case class JsonRefDef(
                       `$ref`: String,
                       description: Option[String] = None
                     ) extends JsonTypeDef

/**
 * Definition of a json union: this allows one of the defined types.
 */
private[smartdatalake] case class JsonOneOfDef(
                         oneOf: Seq[JsonTypeDef]
                       ) extends JsonTypeDef

/**
 * Definition of a json all of: this requires all of the defined types.
 */
private[smartdatalake] case class JsonAllOfDef(
                         allOf: Seq[JsonTypeDef]
                       ) extends JsonTypeDef

/**
 * Definition of a json any of: this allows one or multiple of the defined types.
 */
private[smartdatalake] case class JsonAnyOfDef(
                         anyOf: Seq[JsonTypeDef]
                       ) extends JsonTypeDef

/**
 * A Map is an object with restricted value types.
 * This can be created in json schema by limiting the type of additional properties.
 */
private[smartdatalake] case class JsonMapDef(
                       additionalProperties: JsonTypeDef,
                       description: Option[String] = None
                     ) extends JsonTypeDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Object)
}

/**
 * Supertype of json schema definition root elements
 */
private[smartdatalake] trait SchemaRootDef extends JsonExtractor

/**
 * Json schema root element that starts the schema with an json object.
 */
private[smartdatalake] case class SchemaRootObjectDef(
                                `$schema`: String,
                                definitions: Map[String,JsonTypeDef],
                                properties: ListMap[String,JsonTypeDef],
                                required: Seq[String],
                                additionalProperties: Boolean,
                                title: Option[String] = None
                              ) extends SchemaRootDef {
  override val `type`: Option[JsonTypeEnum] = Some(JsonTypeEnum.Object)
}

/**
 * Mixin to convert json schema elements to json syntax using json4s.
 */
private[smartdatalake] trait JsonExtractor {
  val `type`: Option[JsonTypeEnum] = None

  /**
   * create json4s tree
   */
  def toJson: JValue = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints) + JsonExtractor.jsonTypeDefSerializer
    Extraction.decompose(this)
  }
}
private[smartdatalake] object JsonExtractor {
  /**
   * Custom serializer adds type-attribute if defined and ignores empty attributes
   */
  def jsonTypeDefSerializer() = new CustomSerializer[JsonTypeDef](format => {
    val serializer: PartialFunction[Any, JValue] = {
      case obj: JsonExtractor =>
        val attributes = GenericTypeUtil.attributesWithValuesForCaseClass(obj)
          .filter{
            case (k, None) => false
            case (k, v: Iterable[_]) if (v.isEmpty) => false
            case _ => true
          }
          .map{ case (k,v) => (k, Extraction.decompose(v)(format))}.toList
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