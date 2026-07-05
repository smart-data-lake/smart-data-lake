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
package io.smartdatalake.util.spark

import io.smartdatalake.config.ConfigUtil
import io.smartdatalake.util.misc.FileUtil.readFromPath
import io.smartdatalake.util.misc.{ProductUtil, ScaladocUtil, SdlbXsdURIResolver, SchemaProviderType}
import io.smartdatalake.util.spark.SparkProductUtil.getSchemaFromCaseClass
import io.smartdatalake.util.webservice.OpenApiUtil
import io.smartdatalake.util.webservice.OpenApiUtil.defaultResponseContentType
import io.smartdatalake.workflow.dataframe.{GenericSchema, LazyGenericSchema}
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.avro
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.confluent.json.JsonSchemaConverter
import org.apache.spark.sql.types._
import scaladoc.Tag

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.reflect.runtime.universe.{Type, typeOf}

object SparkSchemaUtil {

  def enrichSchemaCommentsFromCaseClass(schema: StructType, tpe: Type): StructType = {
    if (tpe <:< typeOf[Product]) {
      val tpeAccessors = ProductUtil.classAccessors(tpe)
      val scaladocParamTags = ScaladocUtil.extractScalaDoc(tpe.typeSymbol.annotations)
        .toSeq.flatMap(_.tags.collect { case x: Tag.Param => x })
      val newFields = schema.fields.map { field =>
        var newField = field
        newField.dataType match {
          case dt: StructType =>
            val accessor = tpeAccessors.find(a => a.name.toString == field.name)
            accessor.foreach { a =>
              newField = newField.copy(dataType = enrichSchemaCommentsFromCaseClass(dt, a.returnType))
            }
          case dt: ArrayType if dt.elementType.isInstanceOf[StructType] =>
            val accessor = tpeAccessors.find(a => a.name.toString == field.name)
            accessor.foreach { a =>
              val elementTpe = a.returnType.typeArgs.head
              newField = newField.copy(dataType = dt.copy(elementType = enrichSchemaCommentsFromCaseClass(dt.elementType.asInstanceOf[StructType], elementTpe)))
            }
          case _ => ()
        }
        val comment = scaladocParamTags.find(p => p.name == field.name)
        comment.foreach(c => newField = newField.withComment(ScaladocUtil.formatScaladocMarkup(c.markup)))
        newField
      }
      StructType(newFields)
    } else schema
  }

  def mergeSchemaMetadata(from: StructType, to: StructType): StructType = {

    def replaceField(struct: StructType, newField: StructField): StructType = {
      val structWithoutField = StructType(struct.filterNot(_.name == newField.name))
      structWithoutField.add(newField)
    }

    @tailrec
    def handleArrays(from: ArrayType, to: ArrayType): DataType = {
      from.elementType match {
        case struct: StructType => mergeSchemaMetadata(struct, to.elementType.asInstanceOf[StructType])
        case arr: ArrayType => handleArrays(arr, to.elementType.asInstanceOf[ArrayType])
        case _ => from.elementType
      }
    }

    from.fields.foldLeft(to)((struc, field) => field.dataType match {
      case inner: StructType =>
        val newField = StructField(field.name, mergeSchemaMetadata(inner, struc(field.name).dataType.asInstanceOf[StructType]), struc(field.name).nullable, field.metadata)
        replaceField(struc, newField)
      case arr: ArrayType =>
        val mergedType = handleArrays(arr, struc(field.name).dataType.asInstanceOf[ArrayType])
        val newField = StructField(field.name, ArrayType(mergedType, struc(field.name).nullable), struc(field.name).nullable, field.metadata)
        replaceField(struc, newField)
      case _ => replaceField(struc, struc(field.name).copy(metadata = field.metadata))
    })
  }

  def identifyMissingComments(from: StructType, to: StructType, parents: Seq[String] = Queue()): Map[Seq[String], String] = {

    @tailrec
    def handleArrays(from: ArrayType, to: ArrayType, parents: Seq[String]): Map[Seq[String], String] = {
      (from.elementType, to.elementType) match {
        case (f: StructType, t: StructType) => identifyMissingComments(f, t, parents)
        case (f: ArrayType, t: ArrayType) => handleArrays(f, t, parents)
        case _ => Map()
      }
    }

    val toFields = to.fieldNames
    from.fields.foldLeft(Map(): Map[Seq[String], String])((map, field) => {
      val comment = field.getComment()
      val additionalComments = if (toFields.contains(field.name)) {
        val toField = to(field.name)
        val newParents = parents :+ field.name
        val localComment = if (comment.isDefined && !toField.getComment().equals(comment)) {
          Map(newParents -> comment.get)
        } else Map()
        val nestedComments: Map[Seq[String], String] = (field.dataType, toField.dataType) match {
          case (f: StructType, t: StructType) => identifyMissingComments(f, t, newParents)
          case (f: ArrayType, t: ArrayType) => handleArrays(f, t, newParents)
          case _ => Map()
        }
        localComment ++ nestedComments
      } else Map()
      map ++ additionalComments
    })
  }

  def columnsComments(schema: StructType, parents: Seq[String] = Queue()): Map[Seq[String], String] = {

    @tailrec
    def handleArrays(a: ArrayType, parents: Seq[String]): Map[Seq[String], String] = {
      a.elementType match {
        case s: StructType => columnsComments(s, parents)
        case a: ArrayType => handleArrays(a, parents)
        case _ => Map()
      }
    }

    schema.fields.foldLeft(Map(): Map[Seq[String], String])((map, field) => {
      val newParents = parents :+ field.name
      val singlecomment = if (field.getComment().isDefined) Map(newParents -> field.getComment().get) else Map()
      val nestedComments: Map[Seq[String], String] = field.dataType match {
        case s: StructType => columnsComments(s, newParents)
        case a: ArrayType => handleArrays(a, newParents)
        case _ => Map()
      }
      map ++ singlecomment ++ nestedComments
    })
  }

  def getSchemaFromJavaBean(beanClass: Class[_]): StructType = {
    JavaTypeInference.inferDataType(beanClass)._1.asInstanceOf[StructType]
  }

  def getSchemaFromJsonSchema(jsonSchemaContent: String, strictTyping: Boolean, additionalPropertiesDefault: Boolean): StructType = {
    JsonSchemaConverter.convertToSpark(jsonSchemaContent, strictTyping, additionalPropertiesDefault)
  }

  def getSchemaFromAvroSchema(avroSchemaContent: String): StructType = {
    avro.SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchemaContent)).dataType.asInstanceOf[StructType]
  }

  def getSchemaFromXsd(xsdFile: Path, maxRecursion: Option[Int] = None)
                      (implicit hadoopConfiguration: Configuration): StructType = {
    SdlbXsdURIResolver.readXsd(xsdFile, maxRecursion.getOrElse(10))
  }

  def getSchemaFromDdl(ddl: String): StructType = StructType.fromDDL(ddl)

  def getSchemaFromOpenApi(specUrl: String, operationId: String, responseContentType: String = defaultResponseContentType)
                          (implicit hadoopConfiguration: Configuration): StructType = {
    OpenApiUtil.queryOperationSchema(specUrl, operationId, responseContentType) match {
      case (_, x: StructType) => x
      case (_, dataType) => throw new IllegalStateException(s"Got ${dataType.typeName} as schema" +
        s" for $operationId, but needs StructType ($specUrl)")
    }
  }

  /**
   * Parses a Spark [[StructType]] by using the desired schema provider.
   * The schema provider is included in the configuration value as prefix terminated by '#'.
   */
  def readSchemaFromConfigValue(schemaConfig: String, lazyFileReading: Boolean = true): GenericSchema = {
    import io.smartdatalake.util.misc.SchemaProviderType._
    implicit lazy val defaultHadoopConf: Configuration = new Configuration()
    val (providerId, value) = ConfigUtil.parseProviderConfigValue(schemaConfig, Some(DDL.toString))
    SchemaProviderType.withName(providerId.toLowerCase) match {
      case DDL =>
        SparkSchema(getSchemaFromDdl(value))
      case DDLFile =>
        val valueElements = value.split(";")
        assert(valueElements.size == 1, s"DDL schema provider configuration error. Configuration format is '<path-to-ddl-file>', but received $value.")
        val content = readFromPath(new Path(valueElements.head))
        SparkSchema(getSchemaFromDdl(content))
      case CaseClass =>
        val clazz = this.getClass.getClassLoader.loadClass(value)
        val mirror = scala.reflect.runtime.currentMirror
        val tpe = mirror.classSymbol(clazz).toType
        SparkSchema(getSchemaFromCaseClass(tpe))
      case JavaBean =>
        val clazz = this.getClass.getClassLoader.loadClass(value)
        SparkSchema(getSchemaFromJavaBean(clazz))
      case XsdFile =>
        val valueElements = value.split(";")
        assert(valueElements.size <= 4, s"XSD schema provider configuration error. Configuration format is '<path-to-xsd-file>;<row-tag>;<maxRecursion:Int>;<jsonCompatibility:Boolean>', but received $value.")
        val path = valueElements.head
        val rowTag = if (valueElements.size >= 2) Some(valueElements(1)).filter(_.nonEmpty) else None
        val maxRecursion = if (valueElements.size >= 3) Some(valueElements(2).toInt) else None
        val jsonCompatibility = if (valueElements.size >= 4) Some(valueElements(3).toBoolean) else None
        if (!lazyFileReading) {
          val schema = getSchemaFromXsd(new Path(path), maxRecursion)
          val sparkSchema = SparkSchema(rowTag.map(t => extractRowTag(schema, t)).getOrElse(schema))
          if (jsonCompatibility.getOrElse(false)) makeXsdJsonCompatible(sparkSchema)
          else sparkSchema
        } else LazyGenericSchema(schemaConfig)
      case JsonSchemaFile =>
        val valueElements = value.split(";")
        assert(valueElements.size <= 4, s"Json schema provider configuration error." +
          s" Configuration format is '<path-to-json-file>;<row-tag>;<strictTyping:Boolean>;" +
          s"<additionalPropertiesDefault:Boolean>', but received $value.")
        val path = valueElements.head
        val rowTag = if (valueElements.size >= 2) Some(valueElements(1)).filter(_.nonEmpty) else None
        val strictTyping = if (valueElements.size >= 3) Some(valueElements(2).toBoolean) else None
        val additionalPropertiesDefault = if (valueElements.size >= 4) Some(valueElements(3).toBoolean) else None
        if (!lazyFileReading) {
          val content = readFromPath(new Path(path))
          val schema = getSchemaFromJsonSchema(content, strictTyping.getOrElse(false), additionalPropertiesDefault.getOrElse(false))
          SparkSchema(rowTag.map(t => extractRowTag(schema, t)).getOrElse(schema))
        } else LazyGenericSchema(schemaConfig)
      case AvroSchemaFile =>
        val valueElements = value.split(";")
        assert(valueElements.size <= 2, s"Avro schema provider configuration error. Configuration format is '<path-to-avsc-file>;<row-tag>', but received $value.")
        val path = valueElements.head
        val rowTag = valueElements.drop(1).headOption
        if (!lazyFileReading) {
          val content = readFromPath(new Path(path))
          val schema = getSchemaFromAvroSchema(content)
          SparkSchema(rowTag.map(t => extractRowTag(schema, t)).getOrElse(schema))
        } else LazyGenericSchema(schemaConfig)
      case OpenApi =>
        val valueElements = value.split(";")
        assert(2 <= valueElements.size && valueElements.size <= 3, s"OpenApi schema provider configuration error. Configuration format is '<apiDocsUrl>;<operationId>;<responseContentType>', but received $value.")
        val apiDocsUrl = valueElements(1)
        val operationId = valueElements(2)
        val responseContentType = if (valueElements.size >= 3) valueElements(3) else defaultResponseContentType
        if (!lazyFileReading) {
          val (_, dataType) = OpenApiUtil.queryOperationSchema(apiDocsUrl, operationId, responseContentType)
          dataType match {
            case schema: StructType => SparkSchema(schema)
            case _ => throw new IllegalStateException(s"'object' type (e.g. Spark StructType) needed," +
              s" but got dataType $dataType for operation $operationId")
          }
        } else LazyGenericSchema(schemaConfig)
    }
  }

  private[smartdatalake] def extractRowTag(schema: StructType, rowTag: String): StructType = {
    val schemas = rowTag.split(",").map(extractSingleRowTag(schema, _))
    schemas.reduceLeft(unifySchemas)
  }

  private[smartdatalake] def extractSingleRowTag(schema: StructType, rowTag: String): StructType = {
    rowTag.split("/").filter(_.nonEmpty).foldLeft(schema) {
      case (schema, element) =>
        val schemaElement = schema.fields.find(_.name == element)
        assert(schemaElement.isDefined, s"Schema element $element not found while extracting rowTag. Available fields are ${schema.fieldNames.mkString(", ")}")
        var elementDataType = schemaElement.get.dataType
        elementDataType match {
          case arrayType: ArrayType => elementDataType = arrayType.elementType
          case _ =>
        }
        assert(elementDataType.isInstanceOf[StructType], s"Schema element $element dataType is ${elementDataType.typeName}, but must be a StructType.")
        elementDataType.asInstanceOf[StructType]
    }
  }

  private def unifySchemas(schema1: StructType, schema2: StructType): StructType = {
    val (fields1Common, fields1Only) = schema1.partition(f => schema2.fieldNames.contains(f.name))
    val (fields2Common, fields2Only) = schema2.partition(f => schema1.fieldNames.contains(f.name))
    val fields2CommonMap = fields2Common.map(f => (f.name, f)).toMap
    val commonDifferentType = fields1Common.filter(f => f.dataType != fields2CommonMap(f.name).dataType)
    assert(commonDifferentType.isEmpty, s"Cannot unify schemas. Fields ${commonDifferentType.map(_.name).mkString(",")} have different dataType.")
    val fieldsMap = (fields1Common.map(f => f.copy(nullable = f.nullable || fields2CommonMap(f.name).nullable)) ++
      fields1Only.map(_.copy(nullable = true)) ++
      fields2Only.map(_.copy(nullable = true)))
      .map(f => (f.name, f)).toMap
    StructType(schema1.fields.map(f => fieldsMap(f.name)) ++ fields2Only.map(f => fieldsMap(f.name)))
  }

  private def makeXsdJsonCompatible(sparkSchema: SparkSchema): SparkSchema = {
    def renameArrayToPluralForm(field: StructField): StructField = {
      val newName = field.dataType match {
        case _: ArrayType => field.name + "s"
        case _ => field.name
      }
      field.copy(name = newName)
    }
    transformSchemaFields(sparkSchema, renameArrayToPluralForm)
  }

  def transformSchemaFields(sparkSchema: SparkSchema, fieldTransformer: StructField => StructField): SparkSchema = {
    def visitField(field: StructField): StructField = {
      val transformedField = fieldTransformer(field)
      val newType = visitType(transformedField.dataType)
      transformedField.copy(dataType = newType)
    }

    def visitType(dataType: DataType): DataType = {
      dataType match {
        case structType: StructType => structType.copy(fields = structType.fields.map(f => visitField(f)))
        case arrType: ArrayType => arrType.copy(elementType = visitType(arrType.elementType))
        case mapType: MapType => MapType(visitType(mapType.keyType), visitType(mapType.valueType))
        case x => x
      }
    }

    val newFields = sparkSchema.inner.fields.map(visitField)
    SparkSchema(sparkSchema.inner.copy(fields = newFields))
  }
}
