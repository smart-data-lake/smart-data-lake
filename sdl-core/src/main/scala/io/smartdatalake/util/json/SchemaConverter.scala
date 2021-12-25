package io.smartdatalake.util.json

import org.apache.spark.sql.types._
import org.json4s._

import scala.annotation.tailrec

/**
 * This code originates from https://github.com/zalando-incubator/spark-json-schema and is protected by its corresponding MIT license.
 * We added support for date-time datatype, and changed from using play-json library to json4s to minimize SDL dependencies.
 *
 * Motivation for copying vs creating a PR: It seems that the original version isn't maintained very well. An open PR exists for
 * timestamp and decimal datatype support. It is open for nearly one year, see https://github.com/zalando-incubator/spark-json-schema/pull/49.
 * Also to change from play-json to json4s would be hard to argument on the original library.
 */

/**
 * Schema Converter for getting schema in json format into a spark Structure
 *
 * The given schema for spark has almost no validity checks, so it will make sense
 * to combine this with the schema-validator. For loading data with schema, data is converted
 * to the type given in the schema. If this is not possible the whole row will be null (!).
 * A field can be null if its type is a 2-element array, one of which is "null". The converted
 * schema doesn't check for 'enum' fields, i.e. fields which are limited to a given set.
 * It also doesn't check for required fields or if additional properties are set to true
 * or false. If a field is specified in the schema, than you can select it and it will
 * be null if missing. If a field is not in the schema, it cannot be selected even if
 * given in the dataset.
 */
private[smartdatalake] class SchemaConverter(inputSchema: JObject, isStrictTypingEnabled: Boolean) {
  import io.smartdatalake.util.json.SchemaConverter._
  implicit val format: Formats = DefaultFormats

  lazy val definitions: JObject = (inputSchema \ Definitions).extractOpt[JObject].getOrElse(definitions)

  def convert(): StructType = {
    val name = getJsonName(inputSchema).getOrElse(SchemaRoot)
    val typeName = getJsonType(inputSchema, name).typeName
    if (name == SchemaRoot && typeName == "object") {
      val properties = (inputSchema \ SchemaStructContents).extractOpt[JObject].getOrElse(
        throw new NoSuchElementException(
          s"Root level of schema needs to have a [$SchemaStructContents]-field"
        )
      )
      convertJsonStruct(new StructType, properties, properties.obj.map(_._1))
    } else {
      throw new IllegalArgumentException(
        s"schema needs root level called <$SchemaRoot> and root type <object>. " +
          s"Current root is <$name> and type is <$typeName>"
      )
    }
  }

  private def getJsonName(json: JValue): Option[String] = (json \ SchemaFieldName).extractOpt[String]

  private def getJsonId(json: JValue): Option[String] = (json \ SchemaFieldId).extractOpt[String]

  private def getJsonType(json: JObject, name: String): SchemaType = {
    val id = getJsonId(json).getOrElse(name)

    (json \ SchemaFieldType) match {
      case JString(s) => SchemaType(s.trim, nullable = false)
      case JArray(array) =>
        val nullable = array.contains(JString("null"))
        array.size match {
          case 1 if nullable =>
            throw new IllegalArgumentException("Null type only is not supported")
          case 1 =>
            SchemaType(array.head.extract[String], nullable = nullable)
          case 2 if nullable =>
            array.find(_ != JString("null"))
              .map(i => SchemaType(i.extract[String], nullable = nullable))
              .getOrElse(throw new IllegalArgumentException(s"Incorrect definition of a nullable parameter at <$id>"))
          case _ if isStrictTypingEnabled =>
            throw new IllegalArgumentException(s"Unsupported type definition <${array.toString}> in schema at <$id>")
          case _ => // Default to string as it is the "safest" type
            SchemaType("string", nullable = nullable)
        }
      case JNull =>
        throw new IllegalArgumentException(s"No <$SchemaFieldType>-field in schema at <$id>")
      case t =>
        throw new IllegalArgumentException(s"Unsupported type <${t.toString}> in schema at <$id>")
    }
  }

  @tailrec
  private def convertJsonStruct(schema: StructType, json: JObject, jsonKeys: List[String]): StructType = {
    jsonKeys match {
      case Nil => schema
      case head :: tail =>
        val enrichedSchema = addJsonField(schema, (json \ head).extract[JObject], head)
        convertJsonStruct(enrichedSchema, json, tail)
    }
  }

  private def checkRefs(inputJson: JObject): JObject = {
    val schemaRef = (inputJson \ Reference).extractOpt[String]
    schemaRef match {
      case Some(loc) =>
        val searchDefinitions = Definitions + "/"
        val defIndex = loc.indexOf(searchDefinitions) match {
          case -1 => throw new NoSuchElementException(
            s"Field with name [$Reference] requires path with [$searchDefinitions]"
          )
          case i: Int => i + searchDefinitions.length
        }
        val pathNodes = loc.drop(defIndex).split("/").toList
        val definition = pathNodes.foldLeft(definitions: JValue){ case (obj, node) => obj \ node} match {
          case obj: JObject => obj
          case JNothing => throw new NoSuchElementException(s"Path [$loc] not found in $Definitions")
          case _ => throw new NoSuchElementException(s"Path [$loc] in $Definitions is not of type object")
        }
        definition
      case None => inputJson
    }
  }

  private def addJsonField(schema: StructType, inputJson: JObject, name: String): StructType = {
    val json = checkRefs(inputJson)
    val fieldType = getFieldType(json, name)
    schema.add(getJsonName(json).getOrElse(name), fieldType.dataType, nullable = fieldType.nullable)
  }

  private def getFieldType(json: JObject, name: String): NullableDataType = {
    val fieldType = getJsonType(json, name)
    TypeMap(fieldType.typeName) match {

      case dataType: DataType =>
        NullableDataType(dataType, fieldType.nullable)

      case ArrayType =>
        val innerJson = checkRefs((json \ SchemaArrayContents).extract[JObject])
        val innerJsonType = getFieldType(innerJson, "")
        val dataType = ArrayType(innerJsonType.dataType, innerJsonType.nullable)
        NullableDataType(dataType, fieldType.nullable)

      case StructType =>
        val dataType = getDataType(json, SchemaStructContents)
        NullableDataType(dataType, fieldType.nullable)
    }
  }

  private def getDataType(inputJson: JObject, contentPath: String): DataType = {
    val json = checkRefs(inputJson)

    val content = (json \ contentPath).toOption.collect{ case x: JObject => x }.getOrElse(JObject())

    convertJsonStruct(new StructType, content, content.obj.map(_._1))
  }
}

case class SchemaType(typeName: String, nullable: Boolean)
private case class NullableDataType(dataType: DataType, nullable: Boolean)


object SchemaConverter {

  private val SchemaFieldName = "name"
  private val SchemaFieldType = "type"
  private val SchemaFieldId = "id"
  private val SchemaStructContents = "properties"
  private val SchemaArrayContents = "items"
  private val SchemaRoot = "/"
  private val Definitions = "definitions"
  private val Reference = "$ref"
  private val TypeMap = Map(
    "string" -> StringType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "date-time" -> TimestampType,
    "object" -> StructType,
    "array" -> ArrayType
  )

  def convertStrict(schemaContent: String): StructType = {
    convert(schemaContent, true)
  }

  def convert(schemaContent: String, isStrictTypingEnabled: Boolean = false): StructType = {
    import org.json4s.jackson.JsonMethods._
    implicit val format: Formats = DefaultFormats
    val schema = parse(schemaContent).extract[JObject]
    new SchemaConverter(schema, isStrictTypingEnabled).convert()
  }
}
