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

package io.smartdatalake.workflow.dataframe.spark

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import io.smartdatalake.workflow.dataframe._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset, Row}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

case class SparkDataFrame(inner: DataFrame) extends GenericDataFrame {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def schema: SparkSchema = SparkSchema(inner.schema)
  override def join(other: GenericDataFrame, joinCols: Seq[String]): SparkDataFrame = {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.join(sparkOther.inner, joinCols))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def select(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkDataFrame(inner.select(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def groupBy(columns: Seq[GenericColumn]): SparkGroupedDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkGroupedDataFrame(inner.groupBy(sparkCols:_*))
  }
  override def agg(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail:_*))
  }
  override def unionByName(other: GenericDataFrame): SparkDataFrame= {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.unionByName(sparkOther.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def except(other: GenericDataFrame): SparkDataFrame= {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.except(sparkOther.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def filter(expression: GenericColumn): SparkDataFrame = {
    expression match {
      case sparkExpr: SparkColumn => SparkDataFrame(inner.filter(sparkExpr.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }
  override def collect: Seq[GenericRow] = inner.collect.map(SparkRow)
  override def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): SparkSubFeed = {
    SparkSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }
  override def withColumn(colName: String, expression: GenericColumn): SparkDataFrame = {
    expression match {
      case sparkExpression: SparkColumn => SparkDataFrame(inner.withColumn(colName,sparkExpression.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }
  override def drop(colName: String): SparkDataFrame = SparkDataFrame(inner.drop(colName))
  override def createOrReplaceTempView(viewName: String): Unit = {
    inner.createOrReplaceTempView(viewName)
  }
  override def isEmpty: Boolean = inner.isEmpty
  override def count: Long = inner.count()
}

case class SparkGroupedDataFrame(inner: RelationalGroupedDataset) extends GenericGroupedDataFrame {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def agg(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail:_*))
  }
}

case class SparkSchema(inner: StructType) extends GenericSchema {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = {
    val sparkSchema = schema.convert(subFeedType).asInstanceOf[SparkSchema]
    val caseSensitive = SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
    val missingCols = SchemaUtil.schemaDiff(this, sparkSchema,
      ignoreNullable = Environment.schemaValidationIgnoresNullability,
      deep = Environment.schemaValidationDeepComarison,
      caseSensitive = caseSensitive
    )
    if (missingCols.nonEmpty) Some(SparkSchema(StructType(missingCols.collect{case x:SparkField => x.inner}.toSeq)))
    else None
  }
  override def columns: Seq[String] = inner.fieldNames
  override def fields: Seq[SparkField] = inner.fields.map(SparkField)
  override def sql: String = inner.toDDL
  override def add(colName: String, dataType: GenericDataType): SparkSchema = {
    val sparkDataType = SchemaConverter.convertDatatype(dataType, subFeedType).asInstanceOf[SparkDataType]
    SparkSchema(inner.add(StructField(colName, sparkDataType.inner)))
  }
  override def add(field: GenericField): SparkSchema = {
    field match {
      case sparkField: SparkField => SparkSchema(inner.add(sparkField.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(field)
    }
  }
  override def remove(colName: String): SparkSchema = {
    SparkSchema(StructType(inner.filterNot(_.name == colName)))
  }
  override def filter(func: GenericField => Boolean): SparkSchema = {
    SparkSchema(StructType(fields.filter(func).map(_.inner)))
  }
  override def getEmptyDataFrame(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SparkDataFrame = {
    SparkDataFrame(DataFrameUtil.getEmptyDataFrame(inner)(context.sparkSession))
  }
  override def getDataType(colName: String): SparkDataType = SparkDataType(inner.apply(colName).dataType)
  override def makeNullable: SparkSchema = SparkSchema(StructType(fields.map(_.makeNullable.inner)))
  override def toLowerCase: SparkSchema = SparkSchema(StructType(fields.map(_.toLowerCase.inner)))
  override def removeMetadata: SparkSchema = SparkSchema(StructType(fields.map(_.removeMetadata.inner)))
}

case class SparkColumn(inner: Column) extends GenericColumn {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def ===(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner === sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def >(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner > sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def <(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner < sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def +(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner + sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def -(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner - sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def /(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner / sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def *(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner * sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def and(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner and sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def or(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner or sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def isin(list: Any*): GenericColumn = SparkColumn(inner.isin(list:_*))
  override def isNull: GenericColumn = SparkColumn(inner.isNull)
  override def as(name: String): GenericColumn = SparkColumn(inner.as(name))
  override def cast(dataType: GenericDataType): GenericColumn = {
    dataType match {
      case sparkDataType: SparkDataType => SparkColumn(inner.cast(sparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }
  override def exprSql: String = inner.expr.sql
}

case class SparkField(inner: StructField) extends GenericField {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def name: String = inner.name
  override def dataType: SparkDataType = SparkDataType(inner.dataType)
  override def nullable: Boolean = inner.nullable
  override def makeNullable: SparkField = SparkField(inner.copy(dataType = dataType.makeNullable.inner, nullable = true))
  override def toLowerCase: SparkField = SparkField(inner.copy(dataType = dataType.toLowerCase.inner, name = inner.name.toLowerCase))
  override def removeMetadata: SparkField = SparkField(inner.copy(dataType = dataType.removeMetadata.inner, metadata = Metadata.empty))
}


trait SparkDataType extends GenericDataType {
  def inner: DataType
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def isSortable: Boolean = Seq(StringType, LongType, IntegerType, ShortType, FloatType, DoubleType, DecimalType, TimestampType, DateType).contains(inner)
  override def typeName: String = inner.typeName
  override def sql: String = inner.sql
  override def makeNullable: SparkDataType
  override def toLowerCase: SparkDataType
  override def removeMetadata: SparkDataType
}
case class SparkSimpleDataType(inner: DataType) extends SparkDataType {
  override def makeNullable: SparkDataType = this
  override def toLowerCase: SparkDataType = this
  override def removeMetadata: SparkDataType = this
}
case class SparkStructDataType(override val inner: StructType) extends SparkDataType with GenericStructDataType {
  override def makeNullable: SparkDataType = SparkStructDataType(SparkSchema(inner).makeNullable.inner)
  override def toLowerCase: SparkDataType = SparkStructDataType(SparkSchema(inner).toLowerCase.inner)
  override def removeMetadata: SparkDataType = SparkStructDataType(SparkSchema(inner).removeMetadata.inner)
  override def withOtherFields[T](other: GenericStructDataType with GenericDataType, func: (Seq[GenericField], Seq[GenericField]) => T): T = {
    other match {
      case sparkOther: SparkStructDataType => func(inner.fields.map(SparkField), sparkOther.inner.fields.map(SparkField))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def fields: Seq[SparkField] = inner.fields.map(SparkField)
}
case class SparkArrayDataType(inner: ArrayType) extends SparkDataType with GenericArrayDataType {
  override def makeNullable: SparkDataType = SparkArrayDataType(ArrayType(SparkDataType(inner.elementType).makeNullable.inner, containsNull = true))
  override def toLowerCase: SparkDataType = SparkArrayDataType(ArrayType(SparkDataType(inner.elementType).toLowerCase.inner, containsNull = inner.containsNull))
  override def removeMetadata: SparkDataType = SparkArrayDataType(ArrayType(SparkDataType(inner.elementType).removeMetadata.inner, containsNull = inner.containsNull))
  override def withOtherElementType[T](other: GenericArrayDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case sparkOther: SparkArrayDataType => func(SparkDataType(inner.elementType), SparkDataType(sparkOther.inner.elementType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def containsNull: Boolean = inner.containsNull
  override def elementDataType: SparkDataType = SparkDataType(inner.elementType)
}
case class SparkMapDataType(inner: MapType) extends SparkDataType with GenericMapDataType {
  override def makeNullable: SparkDataType = SparkMapDataType(MapType(SparkDataType(inner.keyType).makeNullable.inner,SparkDataType(inner.valueType).makeNullable.inner, valueContainsNull = true))
  override def toLowerCase: SparkDataType = SparkMapDataType(MapType(SparkDataType(inner.keyType).toLowerCase.inner,SparkDataType(inner.valueType).toLowerCase.inner, valueContainsNull = inner.valueContainsNull))
  override def removeMetadata: SparkDataType = SparkMapDataType(MapType(SparkDataType(inner.keyType).removeMetadata.inner,SparkDataType(inner.valueType).removeMetadata.inner, valueContainsNull = inner.valueContainsNull))
  override def withOtherKeyType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case sparkOther: SparkMapDataType => func(SparkDataType(inner.keyType), SparkDataType(sparkOther.inner.keyType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def withOtherValueType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case sparkOther: SparkMapDataType => func(SparkDataType(inner.valueType), SparkDataType(sparkOther.inner.valueType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
  override def valueContainsNull: Boolean = inner.valueContainsNull
  override def keyDataType: SparkDataType = SparkDataType(inner.keyType)
  override def valueDataType: SparkDataType = SparkDataType(inner.valueType)
}
object SparkDataType {
  def apply(inner: DataType): SparkDataType = inner match {
    case structType: StructType => SparkStructDataType(structType)
    case elementType: ArrayType => SparkArrayDataType(elementType)
    case mapType: MapType => SparkMapDataType(mapType)
    case x => SparkSimpleDataType(x)
  }
}

case class SparkRow(inner: Row) extends GenericRow {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def get(index: Int): Any = inner.get(index)
  override def getAs[T](index: Int): T = get(index).asInstanceOf[T]
}