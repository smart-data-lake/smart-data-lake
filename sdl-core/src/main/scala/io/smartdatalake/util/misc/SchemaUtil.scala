/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.config.ConfigUtil
import io.smartdatalake.util.misc.FileUtil.readFromPath
import io.smartdatalake.util.webservice.OpenApiUtil
import io.smartdatalake.util.webservice.OpenApiUtil.defaultResponseContentType
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.confluent.avro.AvroSchemaConverter
import org.apache.spark.sql.confluent.json.JsonSchemaConverter
import org.apache.spark.sql.types._
import scaladoc.Tag

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

// TODO: Merge with package io.smartdatalake.util.spark.dataset
object SchemaUtil {

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight`: `Set(schemaLeft)` \ `Set(schemaRight)`.
   *
   * @param schemaLeft     schema used as minuend.
   * @param schemaRight    schema used as subtrahend.
   * @param ignoreNullable if `true`, columns that only differ in their `nullable` property are considered equal.
   * @return the set of columns contained in `schemaRight` but not in `schemaLeft`.
   */
  def schemaDiff(schemaLeft: GenericSchema, schemaRight: GenericSchema, ignoreNullable: Boolean = false, caseSensitive: Boolean = false, deep: Boolean = false): Set[GenericField] = {
    if (deep) {
      deepPartialMatchDiffFields(schemaLeft.fields, schemaRight.fields, ignoreNullable, caseSensitive)
    } else {
      val left = prepareSchemaForDiff(schemaLeft.fields, ignoreNullable, caseSensitive)
      val right = prepareSchemaForDiff(schemaRight.fields, ignoreNullable, caseSensitive)
      left.toSet.diff(right.toSet)
    }
  }

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight` in both directions:
   * 1st return value is `Set(schemaLeft) \ Set(schemaRight)`, 2nd return value is `Set(schemaRight) \ Set(schemaLeft)`.
   *
   * @return Tuple `Set(schemaLeft) \ Set(schemaRight), `Set(schemaRight) \ Set(schemaLeft)`
   */
  def schemaDiff2(schemaLeft: Seq[GenericField], schemaRight: Seq[GenericField], ignoreNullable: Boolean = false, caseSensitive: Boolean = false, deep: Boolean = false): (Set[GenericField], Set[GenericField]) = {
    if (deep) {
      (
        deepPartialMatchDiffFields(schemaLeft, schemaRight, ignoreNullable, caseSensitive),
        deepPartialMatchDiffFields(schemaRight, schemaLeft, ignoreNullable, caseSensitive),
      )
    } else {
      val left = prepareSchemaForDiff(schemaLeft, ignoreNullable, caseSensitive).toSet
      val right = prepareSchemaForDiff(schemaRight, ignoreNullable, caseSensitive).toSet
      (left.diff(right), right.diff(left))
    }
  }

  def prepareSchemaForDiff(schemaIn: Seq[GenericField], ignoreNullable: Boolean, caseSensitive: Boolean, ignoreMetadata: Boolean = true): Seq[GenericField] = {
    var schema = schemaIn
    if (ignoreNullable) schema = schema.map(_.makeNullable)
    if (!caseSensitive) schema = schema.map(_.toLowerCase)
    if (ignoreMetadata) schema = schema.map(_.removeMetadata)
    schema
  }

  /**
   * Computes the set difference of `left` minus `right`, i.e: `Set(left)` \ `Set(right)`.
   *
   * StructField equality is defined by exact matching of the field name and partial (subset) matching of field
   * data type as computed by `deepIsTypeSubset`.
   *
   * @param ignoreNullable whether to ignore differences in nullability.
   * @return The set of fields in `left` that are not contained in `right`.
   *
   *         TODO #935: probably doesnt work for structs nested in arrays...
   */
  private[smartdatalake] def deepPartialMatchDiffFields(left: Seq[GenericField],
                                                        right: Seq[GenericField],
                                                        ignoreNullable: Boolean = false,
                                                        caseSensitive: Boolean = false): Set[GenericField] = {
    val rightNamesIndex = right.groupBy(f => if (caseSensitive) f.name else f.name.toLowerCase)
    left.map { leftField: GenericField =>
      val leftName = if (caseSensitive) leftField.name else leftField.name.toLowerCase
      rightNamesIndex.get(leftName) match {
        case Some(rightFieldsWithSameName) if rightFieldsWithSameName.foldLeft(false) {
          (hasPreviousSubset, rightField) =>
            hasPreviousSubset || (//if no previous match found check this rightField
              (ignoreNullable || leftField.nullable == rightField.nullable) //either nullability is ignored or nullability must match
                && deepIsTypeSubset(leftField.dataType, rightField.dataType, ignoreNullable, caseSensitive) //left field must be a subset of right field
              )
        } => Set.empty[GenericField] //found a match
        case _ => Set(leftField) //left field is not contained in right
      }
    }.toSet.flatten
  }

  /**
   * Check if a type is a subset of another type with deep comparison.
   *
   * - For simple types (e.g. String) it checks if the type names are equal.
   * - For array types it checks recursively whether the element types are subsets and optionally the containsNull property.
   * - For map types it checks recursively whether the key types and value types are subsets and optionally the valueContainsNull property.
   * - For struct types it checks whether all fields is a subset with `deepPartialMatchDiffFields`.
   *
   * @param ignoreNullable whether to ignore differences in nullability.
   * @return `true` iff `leftType` is a subset of `rightType`. `false` otherwise.
   */
  private def deepIsTypeSubset(leftType: GenericDataType, rightType: GenericDataType, ignoreNullable: Boolean, caseSensitive: Boolean): Boolean = {
    if (leftType.typeName != rightType.typeName) false /*fail fast*/
    else {
      (leftType, rightType) match {
        case (structL: GenericStructDataType, structR: GenericStructDataType) =>
          structL.withOtherFields(structR, (l, r) => deepPartialMatchDiffFields(l, r, ignoreNullable, caseSensitive).isEmpty)
        case (arrayL: GenericArrayDataType, arrayR: GenericArrayDataType) =>
          if (!ignoreNullable && (arrayL.containsNull != arrayR.containsNull)) false
          else arrayL.withOtherElementType(arrayR, (l, r) => deepIsTypeSubset(l, r, ignoreNullable, caseSensitive: Boolean))
        case (mapL: GenericMapDataType, mapR: GenericMapDataType) =>
          if (!ignoreNullable && (mapL.valueContainsNull != mapR.valueContainsNull)) false
          else mapL.withOtherKeyType(mapR, (l, r) => deepIsTypeSubset(l, r, ignoreNullable, caseSensitive)) && mapL.withOtherValueType(mapR, (l, r) => deepIsTypeSubset(l, r, ignoreNullable, caseSensitive))
        case _ => true //typeNames are equal
      }
    }
  }

  def getSchemaFromCaseClass[T <: Product : TypeTag]: StructType = {
    Encoders.product[T].schema
  }

  def getSchemaFromCaseClass(tpe: Type): StructType = {
    val schema = ProductUtil.createSchema(tpe)
    enrichSchemaCommentsFromCaseClass(schema, tpe)
  }

  def enrichSchemaCommentsFromCaseClass(schema: StructType, tpe: Type): StructType = {
    if (tpe <:< typeOf[Product]) {
      val tpeAccessors = ProductUtil.classAccessors(tpe)
      val scaladocParamTags = ScaladocUtil.extractScalaDoc(tpe.typeSymbol.annotations)
        .toSeq.flatMap(_.tags.collect { case x: Tag.Param => x })
      val newFields = schema.fields.map { field =>
        var newField = field
        // enrich complex type
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
          case _ => () // nothing to do otherwise
        }
        // enrich comment
        val comment = scaladocParamTags.find(p => p.name == field.name)
        comment.foreach(c => newField = newField.withComment(ScaladocUtil.formatScaladocMarkup(c.markup)))
        // return
        newField
      }
      StructType(newFields)
    } else schema
  }


  /**
   * Merges the metadata from this schema into another one. This method should only be used if the used schema ("from")
   * is a subset of the schema it's being merged into ("to"). For this, we should use methods such as the ones defined in the [[io.smartdatalake.workflow.dataobject.SchemaValidation]] trait first.
   *
   * @param from the schema from which the metadata is read. It should be a subset of "to".
   * @param to   The schema in which the metadata is being merged into. Superset of "from".
   */
  def mergeSchemaMetadata(from: StructType, to: StructType): StructType = {

    def replaceField(struct: StructType, newField: StructField): StructType = {
      val structWithoutField = StructType(struct.filterNot(_.name == newField.name)) //Field has to be deleted before since add() does not replace it
      structWithoutField.add(newField)
    }

    @tailrec
    def handleArrays(from: ArrayType, to: ArrayType): DataType = {
      from.elementType match {
        case struct: StructType => mergeSchemaMetadata(struct, to.elementType.asInstanceOf[StructType]) //casting can be done since to is a superset of from
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

  /**
   * This method compares two Schemas and finds existing columns in schema "to"
   * that have a different comment than theones in schema "from".
   * It returns these columns with their new comments.
   * Note that only columns that are present in both schemas (and with the same types) are considered.
   *
   * @param from The schema with the updated column comments
   * @param to   The schema which already exists and is compared to
   * @return A map of the type [Queue[String] -> String],
   *         where the key represents the parents / path of a nested column, and the value the comment of that column.
   *         E.g. a result of Queue("myCol", "mySubCol") -> ("a comment")
   *         represents a nested column "tableName.myCol.mySubCol" which has a comment.
   */
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
      val additionalComments = if (toFields.contains(field.name)) { //only columns that already exist
        val toField = to(field.name)
        val newParents = parents :+ field.name
        val localComment = if (comment.isDefined && !toField.getComment().equals(comment)) { //only if comments are different
          Map(newParents -> comment.get)
        } else Map()

        //only look for identical structures; in the other cases the comments will be updated automatically when writing
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


  /**
   * Returns a Map of columns and comments based on the metadata of a Spark schema.
   * The columns are represented as a Seq of fields (for nested schemas).
   * E.g. a key-value pair Queue("myCol", "mySubCol") -> ("a comment")
   * represents a nested column "tableName.myCol.mySubCol" which has a comment.
   *
   * @param schema The schema containing the metadata
   */
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
      //only look for identical structures; in the other cases the comments will be updated automatically when writing
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
    AvroSchemaConverter.toSqlType(new Schema.Parser().parse(avroSchemaContent)).dataType.asInstanceOf[StructType]
  }

  def getSchemaFromXsd(xsdFile: Path, maxRecursion: Option[Int] = None)(implicit hadoopConfiguration: Configuration): StructType = {
    SdlbXsdURIResolver.readXsd(xsdFile, maxRecursion.getOrElse(10)) // default is maxRecursion=10
  }

  def getSchemaFromDdl(ddl: String): StructType = {
    StructType.fromDDL(ddl)
  }

  def getSchemaFromOpenApi(specUrl: String, operationId: String, responseContentType: String = "application/json")(implicit hadoopConfiguration: Configuration): StructType = {
    OpenApiUtil.queryOperationSchema(specUrl, operationId, responseContentType) match {
      case (contentType, x: StructType) => x
      case (contentType, dataType) => throw new IllegalStateException(s"Got ${dataType.typeName} as schema for $operationId, but needs StructType ($specUrl)")
    }
  }

  def checkMissingCols(colsLeft: Seq[String], colsRight: Seq[String], caseSensitive: Boolean): Seq[String] = {
    if (caseSensitive) colsLeft.diff(colsRight)
    else colsLeft.map(_.toLowerCase).diff(colsRight.map(_.toLowerCase))
  }

  def checkPartitionMatch(configuredPartitions: Seq[String], existingPartitions: Seq[String], caseSensitive: Boolean): (Boolean, Set[String], Set[String]) = {
    val (confPartitions, existPartitions) = if (caseSensitive) (configuredPartitions.toSet, existingPartitions.toSet)
    else (configuredPartitions.map(_.toLowerCase()).toSet, existingPartitions.map(_.toLowerCase()).toSet)
    (confPartitions == existPartitions, confPartitions, existPartitions)
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
          val (contentType, dataType) = OpenApiUtil.queryOperationSchema(apiDocsUrl, operationId, responseContentType)
          dataType match {
            case schema: StructType => SparkSchema(schema)
            case _ => throw new IllegalStateException(s"'object' type (e.g. Spark StructType) needed, but got dataType $dataType for operation $operationId")
          }
        } else LazyGenericSchema(schemaConfig)
    }
  }

  /**
   * Extract nested schema element according to row tag.
   *
   * An undocumented feature allows to specify multiple comma-separated rowTags.
   * extractRowTag will extract both schemas and try to build a superset of it.
   * A use case for this is to extract nodes with same name
   * but different type of different branches of an XML-file, as spark-xml cannot discern those...
   */
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
    // check common fields for same dataType
    val commonDifferentType = fields1Common.filter(f => f.dataType != fields2CommonMap(f.name).dataType)
    assert(commonDifferentType.isEmpty, s"Cannot unify schemas. Fields ${commonDifferentType.map(_.name).mkString(",")} have different dataType.")
    // unify fields, adapting nullable definition
    val fieldsMap = (fields1Common.map(f => f.copy(nullable = f.nullable || fields2CommonMap(f.name).nullable)) ++
      fields1Only.map(_.copy(nullable = true)) ++
      fields2Only.map(_.copy(nullable = true)))
      .map(f => (f.name, f)).toMap
    // order fields according to schema1
    StructType(schema1.fields.map(f => fieldsMap(f.name)) ++ fields2Only.map(f => fieldsMap(f.name)))
  }

  /**
   * In XML array elements are modeled with their own tag named with singular name.
   * In JSON an array attribute has unnamed array entries, but the array attribute has a plural name.
   *
   * Often if you get an XSD file for JSON data (because the data is published as XML and JSON),
   * the singular name of the array element in the XSD has to be converted to a plural name by adding an 's'.
   * Thats what this method does.
   */
  private def makeXsdJsonCompatible(sparkSchema: SparkSchema): SparkSchema = {
    def renameArrayToPluralForm(field: StructField): StructField = {
      val newName = field.dataType match {
        // add final 's' to singular name of XML array field
        case _: ArrayType => field.name + "s"
        case _ => field.name
      }
      field.copy(name = newName)
    }
    transformSchemaFields(sparkSchema, renameArrayToPluralForm)
  }

  /**
   * A function to transform recursively the fields of a schema.
   */
  private def transformSchemaFields(sparkSchema: SparkSchema, fieldTransformer: StructField => StructField): SparkSchema = {
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

object SchemaProviderType extends Enumeration {
  type SchemaProviderType = Value

  /**
   * Parse SQL DDL (data definition language) using Spark.
   * Parameter: A DDL-formatted string. This is a comma separated list of field definitions, e.g. 'a INT, b STRING'.
   */
  val DDL: SchemaProviderType.Value = Value("ddl")

  /**
   * Parse SQL DDL (data definition language) using Spark from a file.
   * Parameter: the hadoop path of the file with a DDL-formatted string as content, see also DDL.
   */
  val DDLFile: SchemaProviderType.Value = Value("ddlfile")

  /**
   * Get schema from a case class using Spark Encoders.
   * Parameter: the class name of the case class.
   */
  val CaseClass: SchemaProviderType.Value = Value("caseclass")

  /**
   * Get schema from a java bean using Sparks java type inference.
   * Parameter: the class name of the java bean.
   */
  val JavaBean: SchemaProviderType.Value = Value("javabean")

  /**
   * Get schema from an XSD file (XML schema definition).
   * This is using a customized version of spark-xml's XSD support:
   * [[https://github.com/databricks/spark-xml#xsd-support]]
   * Parameters (semicolon separated):
   * - the hadoop path of the XSD file.
   * - row tag to extract a subpart from the schema, see also XML source rowTag option.
   * Put an emtpy string to use root tag.
   * To extract a nested row tag, split the elements by slash (/).
   */
  val XsdFile: SchemaProviderType.Value = Value("xsdfile")

  /**
   * Get schema from an Json Schema file, using an adapted verion of zalando-incubator/spark-json-schema library,
   * see also [[JsonSchemaConverter]]
   * Parameters (semicolon separated):
   * - the hadoop path of the Json schema file.
   * - row tag to extract a subpart from the schema, this is similar to XML source rowTag option.
   * Put an emtpy string to use root tag.
   * To extract a nested row tag, split the elements by slash (/).
   */
  val JsonSchemaFile: SchemaProviderType.Value = Value("jsonschemafile")

  /**
   * Get schema from an Avro Schema file using methods from spark-avro
   * Parameters (semicolon separated):
   * - the hadoop path of the Avro schema file.
   * - row tag to extract a subpart from the schema, this is similar to XML source rowTag option.
   * Put an emtpy string to use root tag.
   * To extract a nested row tag, split the elements by slash (/).
   */
  val AvroSchemaFile: SchemaProviderType.Value = Value("avroschemafile")

  /**
   * Get schema from OpenApi specification operation
   * Parameters (semicolon separated):
   * - baseUrl
   * - operationId
   * - optional apiDocsPath, default is v3/api-docs
   * - optional responseContentType, default is application/json
   */
  val OpenApi: SchemaProviderType.Value = Value("openapi")

}