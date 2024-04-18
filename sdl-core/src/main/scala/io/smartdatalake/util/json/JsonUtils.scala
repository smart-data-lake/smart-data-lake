package io.smartdatalake.util.json

import com.typesafe.config.{Config, ConfigRenderOptions}
import io.smartdatalake.util.misc.ProductUtil
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.JsonAST._
import org.json4s.jackson.{JsonMethods, Serialization}
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
      case (x: Product, tpe: StructType) => convertProductToCatalyst(x, tpe)
      case (json: JObject, tpe: StructType) => convertObjectToCatalyst(json, tpe)
      case (json: JArray, tpe: ArrayType) => json.arr.map(convertToCatalyst(_, tpe.elementType))
      case (json: JString, tpe: StringType) => json.s
      case (json: JLong, tpe: LongType) => json.num
      case (json: JLong, tpe: IntegerType) => json.num.toInt
      case (json: JInt, tpe: LongType) => json.num.toLong
      case (json: JInt, tpe: IntegerType) => json.num.toInt
      case (json: JInt, tpe: DecimalType) => BigDecimal(json.num)
      case (json: JDecimal, tpe: DecimalType) => json.num
      case (json: JDecimal, tpe: DoubleType) => json.num.toDouble
      case (json: JDouble, tpe: DecimalType) => BigDecimal(json.num)
      case (json: JDouble, tpe: DoubleType) => json.num
      case (json: JDouble, tpe: FloatType) => json.num.toFloat
      case (json: JBool, tpe: BooleanType) => json.value
      case (json: JString, tpe: TimestampType) =>
        Try(OffsetDateTime.parse(json.s).toInstant).toOption
          .getOrElse(Timestamp.valueOf(LocalDateTime.parse(json.s)))
      case (json: JString, tpe: TimestampNTZType) => LocalDateTime.parse(json.s)
      case (json: JString, tpe: DateType) => LocalDate.parse(json.s)
      case (JNothing | JNull, _) => null
    }
    CatalystTypeConverters.convertToCatalyst(scalaValue)
  }

}
