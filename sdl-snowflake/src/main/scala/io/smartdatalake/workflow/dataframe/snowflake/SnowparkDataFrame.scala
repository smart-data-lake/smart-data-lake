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

import com.snowflake.snowpark.types._
import com.snowflake.snowpark.{Column, DataFrame, RelationalGroupedDataFrame, Row}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

case class SnowparkDataFrame(inner: DataFrame) extends GenericDataFrame {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def schema: SnowparkSchema = SnowparkSchema(inner.schema)
  override def join(other: GenericDataFrame, joinCols: Seq[String]): SnowparkDataFrame = {
    other match {
      case snowparkOther: SnowparkDataFrame => SnowparkDataFrame(inner.join(snowparkOther.inner, joinCols))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def select(columns: Seq[GenericColumn]): SnowparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SnowparkDataFrame(inner.select(columns.map(_.asInstanceOf[SnowparkColumn].inner)))
  }
  override def groupBy(columns: Seq[GenericColumn]): SnowparkGroupedDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val snowparkCols = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkGroupedDataFrame(inner.groupBy(snowparkCols))
  }
  override def agg(columns: Seq[GenericColumn]): SnowparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val snowparkCols = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkDataFrame(inner.agg(snowparkCols.head, snowparkCols.tail:_*))
  }
  override def unionByName(other: GenericDataFrame): SnowparkDataFrame= {
    other match {
      case snowparkOther: SnowparkDataFrame => SnowparkDataFrame(inner.unionByName(snowparkOther.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def except(other: GenericDataFrame): SnowparkDataFrame= {
    other match {
      case snowparkOther: SnowparkDataFrame => SnowparkDataFrame(inner.except(snowparkOther.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def filter(expression: GenericColumn): SnowparkDataFrame = {
    expression match {
      case snowparkExpr: SnowparkColumn => SnowparkDataFrame(inner.filter(snowparkExpr.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }
  override def collect: Seq[GenericRow] = inner.collect.map(SnowparkRow)
  override def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = {
    SnowparkSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }
  override def withColumn(colName: String, expression: GenericColumn): GenericDataFrame = {
    expression match {
      case snowparkExpression: SnowparkColumn => SnowparkDataFrame(inner.withColumn(colName,snowparkExpression.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }
  override def drop(colName: String): GenericDataFrame = SnowparkDataFrame(inner.drop(colName))
  override def createOrReplaceTempView(viewName: String): Unit = {
    inner.createOrReplaceTempView(viewName)
  }
  override def isEmpty: Boolean = inner.count() == 0
  override def count: Long = inner.count()
}

case class SnowparkGroupedDataFrame(inner: RelationalGroupedDataFrame) extends GenericGroupedDataFrame {
  override def subFeedType: Type = typeOf[SnowparkSubFeed]
  override def agg(columns: Seq[GenericColumn]): SnowparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val snowparkCols = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkDataFrame(inner.agg(snowparkCols.head, snowparkCols.tail:_*))
  }
}

case class SnowparkSchema(inner: StructType) extends GenericSchema {
  override def subFeedType: Type = typeOf[SnowparkSubFeed]
  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = {
    val snowparkSchema = schema.convert(subFeedType).asInstanceOf[SnowparkSchema]
    val missingCols = SchemaUtil.schemaDiff(this, snowparkSchema,
      ignoreNullable = Environment.schemaValidationIgnoresNullability,
      deep = Environment.schemaValidationDeepComarison
    )
    if (missingCols.nonEmpty) Some(SnowparkSchema(StructType.apply(missingCols.collect{case x:SnowparkField => x.inner}.toSeq)))
    else None
  }
  override def columns: Seq[String] = inner.names
  override def fields: Seq[SnowparkField] = inner.fields.map(SnowparkField)
  override def sql: String = throw new NotImplementedError(s"Converting schema back to sql ddl is not supported by Snowpark")
  override def add(colName: String, dataType: GenericDataType): SnowparkSchema = {
    val snowparkDataType = SchemaConverter.convertDatatype(dataType, subFeedType).asInstanceOf[SnowparkDataType]
    SnowparkSchema(inner.add(StructField(colName, snowparkDataType.inner)))
  }
  override def add(field: GenericField): SnowparkSchema = {
    field match {
      case snowparkField: SnowparkField => SnowparkSchema(inner.add(snowparkField.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(field)
    }
  }
  override def remove(colName: String): SnowparkSchema = {
    SnowparkSchema(StructType(inner.filterNot(_.name == colName)))
  }
  override def filter(func: GenericField => Boolean): SnowparkSchema = {
    SnowparkSchema(StructType(fields.filter(func).map(_.inner)))
  }
  override def getEmptyDataFrame(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    val df = context.instanceRegistry.get[SnowflakeTableDataObject](dataObjectId).snowparkSession.createDataFrame(Seq.empty[Row], inner)
    SnowparkDataFrame(df)
  }
  override def getDataType(colName: String): GenericDataType = SnowparkDataType(inner.apply(colName).dataType)
  override def makeNullable: SnowparkSchema = SnowparkSchema(StructType(fields.map(_.makeNullable.inner)))
  override def toLowerCase: SnowparkSchema = SnowparkSchema(StructType(fields.map(_.toLowerCase.inner)))
  override def removeMetadata: SnowparkSchema = this // metadata not existing in Snowpark
}

case class SnowparkColumn(inner: Column) extends GenericColumn {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def ===(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner === snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def >(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner > snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def <(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner < snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def +(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner + snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def -(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner - snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def /(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner / snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def *(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner * snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def and(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner and snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def or(other: GenericColumn): SnowparkColumn = {
    other match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(inner or snowparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def isin(list: Any*): SnowparkColumn = {
    // snowpark does not yet support isin operator
    SnowparkColumn(list.map(inner===_).reduce(_ or _))
  }
  override def isNull: SnowparkColumn = SnowparkColumn(inner.is_null)
  override def as(name: String): SnowparkColumn = SnowparkColumn(inner.as(name))
  override def cast(dataType: GenericDataType): SnowparkColumn = {
    dataType match {
      case snowparkDataType: SnowparkDataType => SnowparkColumn(inner.cast(snowparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }
  override def exprSql: String = throw new NotImplementedError(s"Converting column back to sql expression is not supported by Snowpark")
}

case class SnowparkField(inner: StructField) extends GenericField {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def name: String = inner.name
  override def dataType: SnowparkDataType = SnowparkDataType(inner.dataType)
  override def nullable: Boolean = inner.nullable
  override def makeNullable: SnowparkField = SnowparkField(inner.copy(dataType = dataType.makeNullable.inner, nullable = true))
  override def toLowerCase: SnowparkField = SnowparkField(inner.copy(dataType = dataType.toLowerCase.inner, columnIdentifier = ColumnIdentifier(inner.name.toLowerCase)))
  override def removeMetadata: SnowparkField = this // metadata is not existing in Snowpark
}

trait SnowparkDataType extends GenericDataType {
  def inner: DataType
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def isSortable: Boolean = Seq(StringType, LongType, IntegerType, ShortType, FloatType, DoubleType, DecimalType, TimestampType, TimeType, DateType).contains(inner)
  override def typeName: String = inner.typeName
  override def sql: String = convertToSFType(inner)
  override def makeNullable: SnowparkDataType
  override def toLowerCase: SnowparkDataType
  override def removeMetadata: SnowparkDataType = this // metadata is not existing in Snowpark
}
case class SnowparkSimpleDataType(inner: DataType) extends SnowparkDataType {
  override def makeNullable: SnowparkDataType = this
  override def toLowerCase: SnowparkDataType = this
}
case class SnowparkStructDataType(override val inner: StructType) extends SnowparkDataType with GenericStructDataType {
  override def makeNullable: SnowparkDataType = SnowparkStructDataType(SnowparkSchema(inner).makeNullable.inner)
  override def toLowerCase: SnowparkDataType = SnowparkStructDataType(SnowparkSchema(inner).toLowerCase.inner)
  override def withOtherFields[T](other: GenericStructDataType with GenericDataType, func: (Seq[GenericField], Seq[GenericField]) => T): T = {
    other match {
      case snowparkOther: SnowparkStructDataType => func(inner.fields.map(SnowparkField), snowparkOther.inner.fields.map(SnowparkField))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def fields: Seq[SnowparkField] = inner.fields.map(SnowparkField)
}
case class SnowparkArrayDataType(inner: ArrayType) extends SnowparkDataType with GenericArrayDataType {
  override def makeNullable: SnowparkDataType = SnowparkArrayDataType(ArrayType(SnowparkArrayDataType(inner).makeNullable.inner))
  override def toLowerCase: SnowparkDataType = SnowparkArrayDataType(ArrayType(SnowparkArrayDataType(inner).toLowerCase.inner))
  override def withOtherElementType[T](other: GenericArrayDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case snowparkOther: SnowparkArrayDataType => func(SnowparkDataType(inner.elementType), SnowparkDataType(snowparkOther.inner.elementType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def containsNull: Boolean = true // not existing in Snowpark
  override def elementDataType: SnowparkDataType = SnowparkDataType(inner.elementType)
}
case class SnowparkMapDataType(inner: MapType) extends SnowparkDataType with GenericMapDataType {
  override def makeNullable: SnowparkDataType = SnowparkMapDataType(MapType(SnowparkDataType(inner.keyType).makeNullable.inner,SnowparkDataType(inner.valueType).makeNullable.inner))
  override def toLowerCase: SnowparkDataType = SnowparkMapDataType(MapType(SnowparkDataType(inner.keyType).toLowerCase.inner,SnowparkDataType(inner.valueType).toLowerCase.inner))
  override def withOtherKeyType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case snowparkOther: SnowparkMapDataType => func(SnowparkDataType(inner.keyType), SnowparkDataType(snowparkOther.inner.keyType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def withOtherValueType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case snowparkOther: SnowparkMapDataType => func(SnowparkDataType(inner.valueType), SnowparkDataType(snowparkOther.inner.valueType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def valueContainsNull: Boolean = true // not existing in Snowpark
  override def keyDataType: SnowparkDataType = SnowparkDataType(inner.keyType)
  override def valueDataType: SnowparkDataType = SnowparkDataType(inner.valueType)
}
object SnowparkDataType {
  def apply(inner: DataType): SnowparkDataType = inner match {
    case structType: StructType => SnowparkStructDataType(structType)
    case elementType: ArrayType => SnowparkArrayDataType(elementType)
    case mapType: MapType => SnowparkMapDataType(mapType)
    case x => SnowparkSimpleDataType(x)
  }
}

case class SnowparkRow(inner: Row) extends GenericRow {
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def get(index: Int): Any = inner.get(index)
  override def getAs[T](index: Int): T = get(index).asInstanceOf[T]
}