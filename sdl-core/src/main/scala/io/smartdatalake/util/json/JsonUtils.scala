package io.smartdatalake.util.json

import com.typesafe.config.{Config, ConfigRenderOptions}
import io.smartdatalake.util.misc.ProductUtil
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.json4s.JsonAST._
import org.json4s.jackson.{JsonMethods, Serialization, compactJson}
import org.json4s.{Formats, JValue}

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
import scala.util.Try

object JsonUtils {

  /**
   * Convert a Json value to Json String
   */
  def jsonToString(json: JValue)(implicit formats: Formats): String = {
    JsonMethods.compact(json)
  }

  /**
   * Convert a case class to a Json String
   */
  def caseClassToJsonString(instance: AnyRef)(implicit formats: Formats): String = {
    Serialization.write(instance)
  }

  /**
   * Convert a Hocon config to a Json String
   */
  def configToJsonString(config: Config): String = {
    config.root().render(ConfigRenderOptions.concise())
  }

  /**
   * Convert a Json4s JObject into a Scala Map using a given MapType.
   */
  def convertObjectToMap(obj: JObject, dataType: MapType): Map[String, _] = {
    obj.obj.map {
      case (k, v) => (k, convertToCatalyst(v, dataType.valueType))
    }.toMap
  }

  /**
   * Convert a Json4s JObject into InternalRows using a given Schema.
   * InternalRows can be used to create an RDD/DataFrame very efficiently.
   */
  def convertObjectToCatalyst(obj: JObject, schema: StructType): InternalRow = {
    val row = new GenericInternalRow(schema.length)
    schema.zipWithIndex.foreach {
      case (field,idx) => row.update(idx, convertToCatalyst(obj \ field.name, field.dataType))
    }
    row
  }

  /**
   * Convert a Product (including nested JValues) into InternalRows using a given Schema.
   * InternalRows can be used to create an RDD/DataFrame very efficiently.
   */
  def convertProductToCatalyst(x: Product, schema: StructType): InternalRow = {
    val row = new GenericInternalRow(schema.length)
    val values = ProductUtil.attributesWithValuesForCaseClass(x).toMap
    schema.zipWithIndex.foreach {
      case (field,idx) => row.update(idx, convertToCatalyst(values.get(field.name).orElse(null), field.dataType))
    }
    row
  }

  private def convertToCatalyst(value: Any, dataType: DataType): Any = {
    val scalaValue = (value, dataType) match {
      case (json: JObject, tpe: StructType) => convertObjectToCatalyst(json, tpe)
      case (json: JObject, tpe: MapType) => convertObjectToMap(json, tpe)
      case (json: JArray, tpe: ArrayType) => json.arr.map(convertToCatalyst(_, tpe.elementType))
      case (json: JString, StringType) => json.s
      case (json: JLong, LongType) => json.num
      case (json: JLong, IntegerType) => json.num.toInt
      case (json: JInt, LongType) => json.num.toLong
      case (json: JInt, IntegerType) => json.num.toInt
      case (json: JInt, _: DecimalType) => BigDecimal(json.num)
      case (json: JDecimal, _: DecimalType) => json.num
      case (json: JDecimal, DoubleType) => json.num.toDouble
      case (json: JDouble, _: DecimalType) => BigDecimal(json.num)
      case (json: JDouble, DoubleType) => json.num
      case (json: JDouble, FloatType) => json.num.toFloat
      case (json: JBool, BooleanType) => json.value
      case (json: JString, TimestampType) =>
        Try(OffsetDateTime.parse(json.s).toInstant).toOption
          .getOrElse(Timestamp.valueOf(LocalDateTime.parse(json.s)))
      case (json: JString, TimestampNTZType) => LocalDateTime.parse(json.s)
      case (json: JString, DateType) => LocalDate.parse(json.s)
      case (x: Product, tpe: StructType) => convertProductToCatalyst(x, tpe)
      case (x: JValue, StringType) => compactJson(x)
      case (x: Any, StringType) => x.toString
      case (JNothing | JNull | null, _) => null
    }
    CatalystTypeConverters.convertToCatalyst(scalaValue)
  }

}
