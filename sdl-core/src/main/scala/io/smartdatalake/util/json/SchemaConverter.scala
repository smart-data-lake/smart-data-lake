package io.smartdatalake.util.json

import org.apache.spark.sql.types._
import org.json4s._
import io.smartdatalake.util.json.SchemaConverter._
import org.json4s

import scala.annotation.tailrec

/**
 * This code originates from https://github.com/zalando-incubator/spark-json-schema and is protected by its corresponding MIT license.
 * We added support for date-time datatype, and changed from using play-json library to json4s to minimize SDL dependencies.
 * There is some support for additionalProperties, required properties and oneOf.
 *
 * Motivation for copying vs creating a PR: It seems that the original version isn't maintained very well. An open PR exists for
 * timestamp and decimal datatype support. It is open for nearly one year, see https://github.com/zalando-incubator/spark-json-schema/pull/49.
 * Also to change from play-json to json4s would be hard to argument on the original library.
 */

/**
 * Schema Converter for converting schema in json format into a spark schema
 *
 * The given schema for spark has almost no validity checks, so it will make sense
 * to combine this with the schema-validator. For loading data with schema, data is converted
 * to the type given in the schema. If this is not possible the whole row will be null (!).
 * A field can be null if its type is a 2-element array, one of which is "null". The converted
 * schema doesn't check for 'enum' fields, i.e. fields which are limited to a given set.
 * It also doesn't check for required fields.
 * If a field is specified in the schema, than you can select it and it will
 * be null if missing. If a field is not in the schema, it cannot be selected even if
 * given in the dataset.
 *
 * @param inputSchema The Json schema to convert
 * @param isStrictTypingEnabled if isStrictTypingEnabled=true:
 *                              - union types (oneOf) are merged if rational, otherwise they are simply mapped to StringType
 *                              - additional properties are ignored, otherwise the corresponding schema object is mapped to MapType(String,String)
 * @param additionalPropertiesDefault This is the default value for 'additionalProperties'-field if it is missing in a schema with type='object'.
 *                                    Default value is additionalPropertiesDefault=true, as this is conform with the specification.
 *
 * TODO: consolidate with JsonToSparkSchemaConverter of spark-extensions...
 */
class SchemaConverter(inputSchema: JObject,
                      isStrictTypingEnabled: Boolean = true,
                      additionalPropertiesDefault: Boolean = true
                     ) {
  implicit val format: Formats = DefaultFormats

  lazy val definitions: JObject = (inputSchema \ Definitions).extractOpt[JObject].getOrElse(definitions)

  def convert(): StructType = {
    val name = getJsonName(inputSchema).getOrElse(SchemaRoot)
    val schemaType = convertAnyType(inputSchema, name)
    schemaType.dataType.asInstanceOf[StructType]
  }

  private def getJsonName(json: JValue): Option[String] = (json \ SchemaFieldName).extractOpt[String]

  private def getJsonId(json: JValue): Option[String] = (json \ SchemaFieldId).extractOpt[String]

  private def getJsonDescription(json: JValue): Option[String] = (json \ SchemaFieldDescription).extractOpt[String]

  private def getJsonAdditionalProperties(json: JValue): Boolean = {
    (json \ SchemaFieldAdditionalProperties).extractOpt[Boolean].getOrElse(additionalPropertiesDefault)
  }

  private def convertJsonArray(obj: JObject, name: String, nullable: Boolean): SchemaType = {
    val resolvedObj = resolveRefs(obj)
    val resolvedName = getJsonId(resolvedObj).getOrElse(name)
    // parse items
    val items = resolvedObj \ SchemaFieldItems
    val arrType = items match {
      case JNothing => throw new IllegalStateException(s"No 'items'-field found in schema at $resolvedName")
      case v => convertAnyType(v, resolvedName)
    }
    SchemaType(ArrayType(arrType.dataType, arrType.nullable), nullable)
  }

  private def convertJsonObject(obj: JObject, name: String, nullable: Boolean): SchemaType = {
    val resolvedObj = resolveRefs(obj)
    val resolvedName = getJsonId(resolvedObj).getOrElse(name)
    // parse properties
    val properties = resolvedObj \ SchemaFieldProperties
    val required = (resolvedObj \ SchemaFieldRequired).extractOpt[JArray]
    val requiredFields = required.map(_.arr.collect{ case str:JString => str.s }).getOrElse(Seq())
    val additionalProperties = getJsonAdditionalProperties(resolvedObj)
    val structType = properties match {
      case _: JObject if additionalProperties && !isStrictTypingEnabled => MapType(StringType, StringType)
      case v: JObject => convertJsonProperties(v, requiredFields)
      case JNothing if additionalProperties => MapType(StringType, StringType)
      case JNothing => throw new IllegalStateException(s"No 'properties'-field found in schema at $resolvedName")
      case x => throw new IllegalStateException(s"Converting properties for $resolvedName but properties type is $x instead of object")
    }
    SchemaType(structType, nullable)
  }

  private def convertJsonProperties(properties: JObject, required: Seq[String]): StructType = {
    val fields = properties.obj.map { case (k, v) =>
      val fieldType = convertAnyType(v, k)
      val field = StructField(getJsonName(v).getOrElse(k), fieldType.dataType, nullable = fieldType.nullable || !required.contains(k))
      // add description as metadata if defined
      val description = getJsonDescription(v)
      description.map(field.withComment).getOrElse(field)
    }
    StructType(fields)
  }

  private def mergeObjectTypes(obj1: JObject, obj2: JObject, name: String): JObject = {
    val type1 = extractType(obj1, name)
    val type2 = extractType(obj2, name)
    assert(type1.json == JString("object"), "type1 must be object")
    assert(type2.json == JString("object"), "type2 must be object")
    val mergedType = if (type1.nullable || type2.nullable) JArray(List(JString("object"),JString("null"))) else JString("object")
    JObject(List(
      JField(SchemaFieldType,mergedType),
      JField(SchemaFieldProperties,(obj1 \ SchemaFieldProperties).merge(obj2 \ SchemaFieldProperties)),
      JField(SchemaFieldRequired, JArray(List((obj1 \ SchemaFieldRequired).extractOpt[Seq[String]], (obj2 \ SchemaFieldRequired).extractOpt[Seq[String]]).flatten.reduce(_ intersect _).map(JString).toList)),
      JField(SchemaFieldAdditionalProperties, JBool(Seq((obj1 \ SchemaFieldAdditionalProperties).extractOpt[Boolean], (obj2 \ SchemaFieldAdditionalProperties).extractOpt[Boolean], Some(false)).flatten.reduce(_ || _))),
    ))
  }

  private def mergeArrayTypes(obj1: JObject, obj2: JObject, name: String): JObject = {
    val type1 = extractType(obj1, name)
    val type2 = extractType(obj2, name)
    assert(type1.json == JString("array"), "type1 must be array")
    assert(type2.json == JString("array"), "type2 must be array")
    val items1 = type1.json match {
      case JArray(arr) => arr
      case x => Seq(x)
    }
    val items2 = type2.json match {
      case JArray(arr) => arr
      case x => Seq(x)
    }
    val mergedItems = (items1 ++ items2).reduce(mergeTypes(name))
    val mergedType = if (type1.nullable || type2.nullable) JArray(List(JString("array"),JString("null"))) else JString("array")
    JObject(List(
      JField(SchemaFieldType, mergedType),
      JField(SchemaFieldItems, mergedItems),
    ))
  }

  private def mergeTypes(name: String)(jsonType1: JValue, jsonType2: JValue): JValue = {
    assert(jsonType1.toString < jsonType2.toString, "input must be sorted and unique")
    val type1 = extractType(jsonType1, name).json
    val type2 = extractType(jsonType2, name).json
    (type1,type2) match {
      case (JString("object"), JString("object")) =>
        mergeObjectTypes(jsonType1.asInstanceOf[JObject], jsonType2.asInstanceOf[JObject], name)
      case (JString("object"), _) | (_, JString("object")) =>
        if (isStrictTypingEnabled) throw new IllegalArgumentException(s"Cannot unify types <$jsonType1> and <$type2> in schema at <$name>")
        else JString("string")
      case (JString("array"), JString("array")) =>
        mergeArrayTypes(jsonType1.asInstanceOf[JObject], jsonType2.asInstanceOf[JObject], name)
      case (JString("array"), _) | (_, JString("array")) =>
        if (isStrictTypingEnabled) throw new IllegalArgumentException(s"Cannot unify types <$jsonType1> and <$type2> in schema at <$name>")
        else JString("string")
      case (JString("string"), _) | (_, JString("string")) => JString("string")
      case (JString("float"), JString("number")) => JString("number")
      case (JString("integer"), JString("number")) => JString("number")
      case (JString("integer"), JString("float")) => JString("number")
      case _ =>
        if (isStrictTypingEnabled) throw new IllegalArgumentException(s"Cannot unify types <$jsonType1> and <$jsonType2> in schema at <$name>")
        else JString("string")
    }
  }

  private case class NullableType(json: JValue, nullable: Boolean)

  private def extractArrayType(array: Seq[JValue], name: String): NullableType = {
    val nullable = array.contains(JString("null"))
    array.size match {
      case 1 if nullable =>
        throw new IllegalArgumentException(s"Null type only is not supported at <$name>")
      case 1 =>
        NullableType(array.head, nullable = false)
      case 2 if nullable =>
        array.find(_ != JString("null"))
          .map(jsonType => NullableType(jsonType, nullable = true))
          .getOrElse(throw new IllegalArgumentException(s"Incorrect definition of a nullable parameter at <$name>"))
      case _ =>
        NullableType(array.filter(_ != JString("null")).distinct.sortBy(_.toString).reduce(mergeTypes(name)), nullable)
    }
  }

  @tailrec
  private def extractType(json: JValue, name: String): NullableType = json match {
    case str: JString => NullableType(str, nullable = false)
    case JObject(entries) if entries.isEmpty && isStrictTypingEnabled =>
      throw new IllegalStateException(s"type is empty in schema at $name")
    case jsonObj: JObject =>
      val resolvedJsonObj = resolveRefs(jsonObj)
      val jsonType = (resolvedJsonObj \ SchemaFieldFormat).toOption
        .orElse((resolvedJsonObj \ SchemaFieldType).toOption)
        .orElse((resolvedJsonObj \ SchemaFieldOneOf).toOption)
      assert(jsonType.isDefined, throw new IllegalArgumentException(s"No 'type'-field in schema at <$name>"))
      extractType(jsonType.get, name)
    case JArray(arr) => extractArrayType(arr, name)
    case JNothing =>
      throw new IllegalArgumentException(s"No 'type'-field in schema at <$name>")
    case t =>
      throw new IllegalArgumentException(s"Unsupported type <${t.toString}> in schema at <$name>")
  }

  def convertObjWithType(jsonObj: JObject, jsonType: JValue, name: String, nullable: Boolean): SchemaType = {
    jsonType match {
      case JString("object") => convertJsonObject(jsonObj, name, nullable)
      case JString("array") => convertJsonArray(jsonObj, name, nullable)
      case JString(str) => SchemaType(SimpleTypeMap(str.trim), nullable)
    }
  }

  def convertAnyType(json: JValue, name: String, nullable: Boolean = false): SchemaType = {
    val resolvedName = getJsonId(json).getOrElse(name)
    val jsonType = extractType(json, resolvedName)
    json match {
      case JString(str) => SchemaType(SimpleTypeMap(str.trim), jsonType.nullable || nullable)
      case JObject(entries) if entries.isEmpty && isStrictTypingEnabled =>
        throw new IllegalStateException(s"type is empty in schema at $resolvedName")
      case _: JArray => convertAnyType(jsonType.json, name, jsonType.nullable || nullable)
      case _ if jsonType.json.isInstanceOf[JObject] => convertAnyType(jsonType.json, name, jsonType.nullable || nullable)
      case jsonObj: JObject =>
        val resolvedJsonObj = resolveRefs(jsonObj)
        convertObjWithType(resolvedJsonObj, jsonType.json, resolvedName, jsonType.nullable || nullable)
      case JNothing =>
        throw new IllegalArgumentException(s"No 'type'-field in schema at <$resolvedName>")
      case t =>
        throw new IllegalArgumentException(s"Unsupported type <${t.toString}> in schema at <$resolvedName>")
    }
  }

  private def resolveRefs(inputJson: JObject): JObject = {
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
}

case class SchemaType(dataType: DataType, nullable: Boolean)

object SchemaConverter {

  private val SchemaFieldName = "name"
  private val SchemaFieldType = "type"
  private val SchemaFieldFormat = "format"
  private val SchemaFieldOneOf = "oneOf"
  private val SchemaFieldId = "id"
  private val SchemaFieldProperties = "properties"
  private val SchemaFieldItems = "items"
  private val SchemaFieldAdditionalProperties = "additionalProperties"
  private val SchemaFieldDescription = "description"
  private val SchemaFieldRequired = "required"
  private val SchemaRoot = "/"
  private val Definitions = "definitions"
  private val Reference = "$ref"
  private val SimpleTypeMap = Map(
    "string" -> StringType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "date-time" -> TimestampType
  )

  def convertStrict(schemaContent: String): StructType = {
    convert(schemaContent, isStrictTypingEnabled = true)
  }

  def convert(schemaContent: String, isStrictTypingEnabled: Boolean = false, additionalPropertiesDefault: Boolean = false): StructType = {
    import org.json4s.jackson.JsonMethods._
    implicit val format: Formats = DefaultFormats
    val schema = parse(schemaContent).extract[JObject]
    new SchemaConverter(schema, isStrictTypingEnabled, additionalPropertiesDefault).convert()
  }
}
