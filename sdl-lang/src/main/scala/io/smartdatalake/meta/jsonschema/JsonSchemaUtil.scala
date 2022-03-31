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
import io.smartdatalake.meta.{GenericAttributeDef, GenericTypeDef, GenericTypeUtil, jsonschema}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.DataObject
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
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
  def createSdlSchema(version: String): SchemaRootObjectDef = {

    val reflections = GenericTypeUtil.getReflections

    // get generic type definitions
    val typeDefs = GenericTypeUtil.typeDefs(reflections)
      .filter(_.isFinal) // only case classes

    // define registry and converter
    val registry = new DefinitionRegistry
    val converter = new JsonTypeConverter(reflections, registry)

    // create and add type definitions without base type
    typeDefs.filter(_.baseTpe.isEmpty).foreach(typeDef => registry.add(typeDef.baseTpe, typeDef.tpe, converter.fromGenericTypeDef(typeDef)))

    // create and add type definitions with base type
    // use reverse order of BaseType enum to satisfy dependencies
    GenericTypeUtil.baseTypes.reverse.foreach { currentBaseType =>
      typeDefs.filter(_.baseTpe.contains(currentBaseType))
        .foreach(typeDef =>
          registry.add(typeDef.baseTpe, typeDef.tpe, converter.fromGenericTypeDef(typeDef))
        )
    }

    // create global type
    val globalJsonDef = converter.fromCaseClass(typeOf[GlobalConfig].typeSymbol.asClass)

    // create output schema
    val jsonRootDef = SchemaRootObjectDef(
      `$schema` = "http://json-schema.org/draft-07/schema#",
      version = version,
      id = "sdl-schema.json#",
      properties = ListMap(
        globalKey -> globalJsonDef,
        connectionsKey -> JsonMapDef(JsonOneOfDef(registry.getJsonRefDefs(typeOf[Connection]), Some("Map Connection name : definition"))),
        dataObjectsKey -> JsonMapDef(JsonOneOfDef(registry.getJsonRefDefs(typeOf[DataObject]), Some("Map of DataObject name and definition"))),
        actionsKey -> JsonMapDef(JsonOneOfDef(registry.getJsonRefDefs(typeOf[Action]), Some("Map of Action name and definition"))),
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
  class JsonTypeConverter(reflections: Reflections, registry: DefinitionRegistry) {

    def fromGenericTypeDef(typeDef: GenericTypeDef): JsonObjectDef = {
      val typeProperty = if(typeDef.baseTpe.isDefined) Seq(("type", JsonConstDef(typeDef.name))) else Seq()
      val jsonAttributes = typeProperty ++ typeDef.attributes.map(a => (a.name, convertToJsonType(a)))
      val properties = ListMap(jsonAttributes:_*)
      val required = typeDef.attributes.filter(_.isRequired).map(_.name)
      jsonschema.JsonObjectDef(properties, required = required, title = typeDef.name, description = typeDef.description)
    }

    def fromCaseClass(cls: ClassSymbol): JsonObjectDef = {
      convertedCaseClasses.getOrElseUpdate(cls, {
        logger.debug(s"Converting case class ${cls.fullName}")
        val typeDef = GenericTypeUtil.typeDefForClass(cls.toType)
        fromGenericTypeDef(typeDef)
      })
    }
    private val convertedCaseClasses: mutable.Map[ClassSymbol, JsonObjectDef] = mutable.Map()

    def convertToJsonType(attr: GenericAttributeDef): JsonTypeDef = {
      convertToJsonType(attr.tpe, attr.description, if (attr.isDeprecated) Some(true) else None)
    }

    def convertToJsonType(tpe: Type, description: Option[String] = None, isDeprecated: Option[Boolean] = None): JsonTypeDef = {
      tpe match {
        case t if t =:= typeOf[String] => JsonStringDef(description, deprecated = isDeprecated)
        case t if t =:= typeOf[Long] => JsonIntegerDef(description, deprecated = isDeprecated)
        case t if t =:= typeOf[Int] => JsonIntegerDef(description, deprecated = isDeprecated)
        case t if t =:= typeOf[Float] => JsonNumberDef(description, deprecated = isDeprecated)
        case t if t =:= typeOf[Double] => JsonNumberDef(description, deprecated = isDeprecated)
        case t if t =:= typeOf[Boolean] => JsonBooleanDef(description, deprecated = isDeprecated)
        case t if t.typeSymbol.asType.toType <:< typeOf[ConfigObjectId] => JsonStringDef(description, deprecated = isDeprecated) // map DataObjectId as string
        case t if t =:= typeOf[StructType] => JsonStringDef(description, deprecated = isDeprecated) // map StructType (Spark schema) as string
        case t if t =:= typeOf[GenericSchema] => JsonStringDef(description, deprecated = isDeprecated) // map GenericSchema as string
        case t if t =:= typeOf[OutputMode] => JsonStringDef(description, enum = Some(Seq("Append", "Complete", "Update")), deprecated = isDeprecated) // OutputMode is not an ordinary enum...
        // BaseTypes needs to be handled before ParsableFromConfig type
        case t if registry.baseTypeExists(t) => {
          val refDefs = registry.getJsonRefDefs(t)
          if (refDefs.size > 1) JsonOneOfDef(refDefs, description, deprecated = isDeprecated)
          else refDefs.head
        }
        case t if registry.typeExists(t) => registry.getJsonRefDef(t)
        case t if t <:< typeOf[Product] => fromCaseClass(t.typeSymbol.asClass)
        case t if t <:< typeOf[ParsableFromConfig[_]] =>
          val baseCls = getClass.getClassLoader.loadClass(t.typeSymbol.fullName)
          val subTypeClssSym = reflections.getSubTypesOf(baseCls).asScala
            .map(mirror.classSymbol)
          logger.debug(s"ParsableFromConfig ${baseCls.getSimpleName} has sub types ${subTypeClssSym.map(_.name.toString)}")
          JsonOneOfDef(subTypeClssSym.map(c => addTypeProperty(fromCaseClass(c), c.fullName)).toSeq, description, deprecated = isDeprecated)
        case t if t.typeSymbol.asClass.isSealed =>
          val subTypeClss = t.typeSymbol.asClass.knownDirectSubclasses
          logger.debug(s"Sealed trait ${t.typeSymbol.fullName} has sub types ${subTypeClss.map(_.name.toString)}")
          JsonOneOfDef(subTypeClss.map(c => addTypeProperty(fromCaseClass(c.asClass), c.fullName)).toSeq, description, deprecated = isDeprecated)
        case t if t <:< typeOf[Iterable[_]] || t <:< typeOf[Array[_]] =>
          t.typeArgs.size match {
            // simple list
            case 1 =>
              val subTypeCls = t.typeArgs.head.typeSymbol.asClass
              JsonArrayDef(convertToJsonType(subTypeCls.toType), description, deprecated = isDeprecated)
            // map with key=String
            case 2 if t.typeArgs.head.typeSymbol.asType.toType =:= typeOf[String] || t.typeArgs.head.typeSymbol.asType.toType <:< typeOf[ConfigObjectId] =>
              val valueCls = t.typeArgs.last.typeSymbol.asClass
              JsonMapDef(additionalProperties = convertToJsonType(valueCls.toType), description = description, deprecated = isDeprecated)
            case 2 => throw new IllegalStateException(s"Key type for Map must be String, but is ${t.typeArgs.head.typeSymbol.fullName}")
            case _ => throw new IllegalStateException(s"Can not handle List with elements of type ${t.typeArgs.map(_.typeSymbol.fullName).mkString(",")}")
          }
        case t: TypeRef if t.pre <:< typeOf[Enumeration] =>
          val enumValues = t.pre.members.filter(m => !m.isMethod && !m.isType  && m.typeSignature.typeSymbol.name.toString == "Value")
          assert(enumValues.nonEmpty, s"Enumeration values for ${t.typeSymbol.fullName} not found")
          JsonStringDef(description, enum = Some(enumValues.map(_.name.toString).toSeq), deprecated = isDeprecated)
        case t =>
          logger.warn(s"Json schema creator for ${t.typeSymbol.fullName} missing. Creating type as existingJavaType.")
          JsonStringDef(description, existingJavaType = Some(t.typeSymbol.fullName), deprecated = isDeprecated)
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
  private val defaultBaseTypeName = "Others"
  private val entries: mutable.Map[Option[Type], mutable.Map[Type,JsonTypeDef]] = mutable.Map()
  def add(baseType: Option[Type], tpe: Type, jsonType: JsonTypeDef): Unit = {
    val baseTypeEntries = entries.getOrElseUpdate(baseType, mutable.Map())
    baseTypeEntries.put(tpe, jsonType)
  }
  def getJsonRefDefs(baseType: Type): Seq[JsonRefDef] = {
    val baseTypeEntries = entries.getOrElse(Some(baseType), Map())
    baseTypeEntries.keys.map( tpe => getJsonRefDef(Some(baseType), tpe)).toSeq.sortBy(_.`$ref`)
  }
  def getJsonRefDef(baseType: Option[Type], tpe: Type): JsonRefDef = {
    JsonRefDef(s"#/definitions/${getDefinitionName(baseType, tpe.typeSymbol.name.toString)}")
  }
  def getJsonRefDef(tpe: Type): JsonRefDef = {
    val baseType = entries.flatMap { case (baseType, typeDefs) => typeDefs.keys.map( typeDef => (typeDef, baseType))}
      .find(_._1 == tpe).get._2
    JsonRefDef(s"#/definitions/${getDefinitionName(baseType, tpe.typeSymbol.name.toString)}")
  }
  def getDefinitionMap: Map[String, ListMap[String, JsonTypeDef]] = {
    entries.map {
      case (baseType, typeDefs) =>
        val baseTypeName = baseType.map(_.typeSymbol.name.toString).getOrElse(defaultBaseTypeName)
        val typeDefsSorted = typeDefs.map {
          case (tpe, typeDef) => (tpe.typeSymbol.name.toString, typeDef)
        }.toSeq.sortBy(_._1)
        (baseTypeName, ListMap(typeDefsSorted:_*))
    }.toMap
  }
  def baseTypeExists(tpe: Type): Boolean = entries.isDefinedAt(Some(tpe))
  def typeExists(tpe: Type): Boolean = entries.values.flatMap(_.keys).toSeq.contains(tpe)
  private def getDefinitionName(baseType: Option[Type], name: String) = s"${baseType.map(_.typeSymbol.name.toString).getOrElse(defaultBaseTypeName)}/$name"
}

