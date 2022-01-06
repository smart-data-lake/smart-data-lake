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

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.config.{ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.meta.BaseType.BaseType
import io.smartdatalake.meta.{BaseType, GenericAttributeDef, GenericTypeDef, GenericTypeUtil, jsonschema}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods.pretty
import org.reflections.Reflections

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.reflect.runtime.universe.{ClassSymbol, Type, TypeRef, typeOf}

/**
 * Create Json schema elements from generic type definitions.
 */
private[smartdatalake] object JsonSchemaUtil extends SmartDataLakeLogger {

  private val globalKey = "global"
  private val connectionsKey = "connections"
  private val dataObjectsKey = "dataObjects"
  private val actionsKey = "actions"

  /**
   * create generic type definitions and convert to json schema elements.
   */
  def createSdlSchema(): SchemaRootObjectDef = {

    val reflections = GenericTypeUtil.getReflections

    // get generic type definitions
    val typeDefs = GenericTypeUtil.typeDefs(reflections)
      .filter(_.isFinal) // only case classes

    // create and register json types
    // use reverse order of BaseType enum to satisfy dependencies
    val registry = new DefinitionRegistry
    val converter = new JsonTypeConverter(BaseType.values.toSeq, reflections, registry)
    BaseType.values.toSeq.reverse.foreach { currentBaseType =>
      typeDefs.filter(_.baseType.contains(currentBaseType))
        .foreach(typeDef => registry.add(typeDef.baseType.get, typeDef.name, converter.fromGenericTypeDef(typeDef)))
    }

    // create global type
    val globalJsonDef = converter.fromCaseClass(typeOf[GlobalConfig].typeSymbol.asClass)

    // create output schema
    val jsonRootDef = SchemaRootObjectDef(
      `$schema` = "http://json-schema.org/draft-07/schema#",
      properties = ListMap(
        globalKey -> globalJsonDef,
        dataObjectsKey -> JsonMapDef(JsonOneOfDef(registry.getJsonRefDefs(BaseType.DataObject))),
        actionsKey -> JsonMapDef(JsonOneOfDef(registry.getJsonRefDefs(BaseType.Action))),
        connectionsKey -> JsonMapDef(JsonOneOfDef(registry.getJsonRefDefs(BaseType.Connection))),
      ),
      required = Seq(dataObjectsKey, actionsKey),
      additionalProperties = true,
      definitions = registry.getDefinitionMap
    )

    jsonRootDef
  }

  /**
   * Converter of Scala types to json schema element
   */
  class JsonTypeConverter(baseTypes: Seq[BaseType], reflections: Reflections, registry: DefinitionRegistry) {

    def fromGenericTypeDef(typeDef: GenericTypeDef): JsonObjectDef = {
      val properties = ListMap(typeDef.attributes.map(a => (a.name, convertToJsonType(a))):_*)
      val required = typeDef.attributes.filter(_.isRequired).map(_.name)
      jsonschema.JsonObjectDef(properties, required, description = typeDef.description)
    }

    def fromCaseClass(cls: ClassSymbol): JsonObjectDef = {
      convertedCaseClasses.getOrElseUpdate(cls, {
        logger.debug(s"Converting case class ${cls.fullName}")
        val typeDef = GenericTypeUtil.typeDefForClass(cls)
        fromGenericTypeDef(typeDef)
      })
    }
    private val convertedCaseClasses: mutable.Map[ClassSymbol, JsonObjectDef] = mutable.Map()

    def convertToJsonType(attr: GenericAttributeDef): JsonTypeDef = {
      convertToJsonType(attr.tpe, attr.description)
    }

    def convertToJsonType(tpe: Type, description: Option[String] = None): JsonTypeDef = {
      tpe match {
        case t if t =:= typeOf[String] => JsonStringDef(description)
        case t if t =:= typeOf[Long] => JsonIntegerDef(description)
        case t if t =:= typeOf[Int] => JsonIntegerDef(description)
        case t if t =:= typeOf[Float] => JsonNumberDef(description)
        case t if t =:= typeOf[Double] => JsonNumberDef(description)
        case t if t =:= typeOf[Boolean] => JsonBooleanDef(description)
        case t if t.typeSymbol.asType.toType <:< typeOf[ConfigObjectId] => JsonStringDef(description) // map DataObjectId as string
        case t if t =:= typeOf[StructType] => JsonStringDef(description) // map StructType (Spark schema) as string
        case t if t =:= typeOf[OutputMode] => JsonStringDef(description, enum = Some(Seq("Append", "Complete", "Update"))) // OutputMode is not an ordinary enum...
        case t if baseTypes.exists(_.toString == t.typeSymbol.fullName) => JsonOneOfDef(registry.getJsonRefDefs(baseTypes.find(_.toString == t.typeSymbol.fullName).get)) // this needs to be done before ParsableFromConfig type
        case t if t <:< typeOf[Product] => fromCaseClass(t.typeSymbol.asClass)
        case t if t <:< typeOf[ParsableFromConfig[_]] =>
          val baseCls = getClass.getClassLoader.loadClass(t.typeSymbol.fullName)
          val subTypeClss = reflections.getSubTypesOf(baseCls).asScala
            .map(mirror.classSymbol)
          logger.debug(s"ParsableFromConfig ${baseCls.getName} has sub types ${subTypeClss.map(_.name.toString)}")
          JsonOneOfDef(subTypeClss.map(c => addTypeProperty(fromCaseClass(c.asClass), c.fullName)).toSeq)
        case t if t.typeSymbol.asClass.isSealed =>
          val subTypeClss = t.typeSymbol.asClass.knownDirectSubclasses
          logger.debug(s"Sealed trait ${t.typeSymbol.fullName} has sub types ${subTypeClss.map(_.name.toString)}")
          JsonOneOfDef(subTypeClss.map(c => addTypeProperty(fromCaseClass(c.asClass), c.fullName)).toSeq)
        case t if t <:< typeOf[Iterable[_]] || t <:< typeOf[Array[_]] =>
          t.typeArgs.size match {
            // simple list
            case 1 =>
              val subTypeCls = t.typeArgs.head.typeSymbol.asClass
              JsonArrayDef(convertToJsonType(subTypeCls.toType), description)
            // map with key=String
            case 2 if t.typeArgs.head.typeSymbol.asType.toType =:= typeOf[String] || t.typeArgs.head.typeSymbol.asType.toType <:< typeOf[ConfigObjectId] =>
              val valueCls = t.typeArgs.last.typeSymbol.asClass
              JsonMapDef(additionalProperties = convertToJsonType(valueCls.toType), description = description)
            case 2 => throw new IllegalStateException(s"Key type for Map must be String, but is ${t.typeArgs.head.typeSymbol.fullName}")
            case _ => throw new IllegalStateException(s"Can not handle List with elements of type ${t.typeArgs.map(_.typeSymbol.fullName).mkString(",")}")
          }
        case t: TypeRef if t.pre <:< typeOf[Enumeration] =>
          val enumValues = t.pre.members.filter(m => !m.isMethod && !m.isType  && m.typeSignature.typeSymbol.name.toString == "Value")
          assert(enumValues.nonEmpty, s"Enumeration values for ${t.typeSymbol.fullName} not found")
          JsonStringDef(description, enum = Some(enumValues.map(_.name.toString).toSeq))
        case t =>
          logger.error(s"Json schema creator for ${t.typeSymbol.fullName} missing")
          JsonStringDef(description, existingJavaType = Some(t.typeSymbol.fullName))
      }
    }

    def addTypeProperty(jsonDef: JsonObjectDef, className: String): JsonObjectDef = {
      val typeName = if (className.startsWith("io.smartdatalake")) className.split('.').last else className
      jsonDef.copy(properties = ListMap("type" -> JsonConstDef(typeName)) ++ jsonDef.properties, required = jsonDef.required :+ "type")
    }

    private val mirror = scala.reflect.runtime.currentMirror
  }

}

/**
 * Registry for global json schema element definitions
 */
private[smartdatalake] class DefinitionRegistry() {
  val entries: mutable.Map[BaseType, mutable.Map[String,JsonTypeDef]] = mutable.Map()
  def add(baseType: BaseType, name: String, jsonType: JsonTypeDef): Unit = {
    val baseTypeEntries = entries.getOrElseUpdate(baseType, mutable.Map())
    baseTypeEntries.put(name, jsonType)
  }
  def getJsonRefDefs(baseType: BaseType): Seq[JsonRefDef] = {
    val baseTypeEntries = entries.getOrElse(baseType, Map())
    baseTypeEntries.map{ case (name, tpe) => JsonRefDef(s"#/definitions/$name")}.toSeq
  }
  def getDefinitionMap: Map[String, JsonTypeDef] = {
    val allEntries = entries.values.reduceLeft(_ ++ _)
    allEntries.map{ case (name, tpe) => (name, tpe)}.toMap
  }
}

