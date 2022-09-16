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

package io.smartdatalake.workflow.dataframe.snowflake

import io.smartdatalake.workflow.dataframe.{GenericDataType, GenericField, SchemaConverter}
import io.smartdatalake.workflow.dataframe.spark.{SparkArrayDataType, SparkDataType, SparkField, SparkMapDataType, SparkSimpleDataType, SparkStructDataType, SparkSubFeed}
import org.apache.spark.sql.{types => spark}
import com.snowflake.snowpark.{types => snowpark}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Converter for Spark schema to Snowpark schema
 */
private[smartdatalake] object SparkSnowparkSchemaConverter extends SchemaConverter {

  override val fromSubFeedType: Type = typeOf[SparkSubFeed]
  override val toSubFeedType: Type = typeOf[SnowparkSubFeed]
  override def convertField(field: GenericField): SnowparkField = {
    field match {
      case sparkField: SparkField => SnowparkField(snowpark.StructField(sparkField.inner.name, convertDataType(sparkField.dataType).inner, sparkField.nullable))
      case _ => throw new IllegalStateException(s"Unsupported class ${field.getClass.getSimpleName} in method convertField")
    }
  }

  override def convertDataType(dataType: GenericDataType): SnowparkDataType = {
    dataType match {
      case sparkStructDataType: SparkStructDataType => SnowparkStructDataType(snowpark.StructType(sparkStructDataType.fields.map(convertField).map(_.inner)))
      case sparkArrayDataType: SparkArrayDataType => SnowparkArrayDataType(snowpark.ArrayType(convertDataType(sparkArrayDataType.elementDataType).inner))
      case sparkMapDataType: SparkMapDataType => SnowparkMapDataType(snowpark.MapType(convertDataType(sparkMapDataType.keyDataType).inner, convertDataType(sparkMapDataType.valueDataType).inner))
      case sparkSimpleDataType: SparkSimpleDataType => SnowparkSimpleDataType(simpleDataTypeMapping(sparkSimpleDataType.inner))
      case _ => throw new IllegalStateException(s"Unsupported class ${dataType.getClass.getSimpleName} in method convertDataType")
    }
  }

  private def simpleDataTypeMapping(dataType: spark.DataType): snowpark.DataType = {
    dataType match {
      case _: spark.StringType => snowpark.StringType
      case _: spark.VarcharType => snowpark.StringType
      case _: spark.CharType => snowpark.StringType
      case _: spark.BooleanType => snowpark.BooleanType
      case _: spark.ByteType => snowpark.ByteType
      case _: spark.ShortType => snowpark.ShortType
      case _: spark.IntegerType => snowpark.IntegerType
      case _: spark.LongType => snowpark.LongType
      case _: spark.FloatType => snowpark.FloatType
      case _: spark.DoubleType => snowpark.DoubleType
      case d: spark.DecimalType => snowpark.DecimalType(d.precision, d.scale)
      case _: spark.DateType => snowpark.DateType
      case _: spark.TimestampType => snowpark.TimestampType
      case _: spark.BinaryType => snowpark.BinaryType
    }
  }
}

/**
 * Converter for Snowpark schema to Spark schema
 */
private[smartdatalake] object SnowparkSparkSchemaConverter extends SchemaConverter {

  override val fromSubFeedType: Type = typeOf[SnowparkSubFeed]
  override val toSubFeedType: Type = typeOf[SparkSubFeed]
  override def convertField(field: GenericField): SparkField = {
    field match {
      case snowparkField: SnowparkField => SparkField(spark.StructField(snowparkField.inner.name.toLowerCase, convertDataType(snowparkField.dataType).inner, snowparkField.nullable))
      case _ => throw new IllegalStateException(s"Unsupported class ${field.getClass.getSimpleName} in method convertField")
    }
  }

  override def convertDataType(dataType: GenericDataType): SparkDataType = {
    dataType match {
      case snowparkStructDataType: SnowparkStructDataType => SparkStructDataType(spark.StructType(snowparkStructDataType.fields.map(convertField).map(_.inner)))
      case snowparkArrayDataType: SnowparkArrayDataType => SparkArrayDataType(spark.ArrayType(convertDataType(snowparkArrayDataType.elementDataType).inner))
      case snowparkMapDataType: SnowparkMapDataType => SparkMapDataType(spark.MapType(convertDataType(snowparkMapDataType.keyDataType).inner, convertDataType(snowparkMapDataType.valueDataType).inner))
      case snowparkSimpleDataType: SnowparkSimpleDataType => SparkSimpleDataType(simpleDataTypeMapping(snowparkSimpleDataType.inner))
      case _ => throw new IllegalStateException(s"Unsupported class ${dataType.getClass.getSimpleName} in method convertDataType")
    }
  }

  private def simpleDataTypeMapping(dataType: snowpark.DataType): spark.DataType = {
    dataType match {
      case snowpark.StringType => spark.StringType
      case snowpark.BooleanType => spark.BooleanType
      case snowpark.ByteType => spark.ByteType
      case snowpark.ShortType => spark.ShortType
      case snowpark.IntegerType => spark.IntegerType
      case snowpark.LongType => spark.LongType
      case snowpark.FloatType => spark.FloatType
      case snowpark.DoubleType => spark.DoubleType
      case d: snowpark.DecimalType => spark.DecimalType(d.precision, d.scale)
      case snowpark.DateType => spark.DateType
      case snowpark.TimestampType => spark.TimestampType
      case snowpark.BinaryType => spark.BinaryType
    }
  }
}