/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.workflow.dataframe.spark

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.evolution.SchemaEvolution.listFind
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.util.spark.dataset
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.types._
import org.json4s.JString
import org.json4s.JsonAST.JValue

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

case class SparkDataFrame(inner: DataFrame) extends GenericDataFrame {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def schema: SparkSchema = SparkSchema(inner.schema)

  override def join(other: GenericDataFrame, joinCols: Seq[String], joinType: String): SparkDataFrame = {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.join(sparkOther.inner, joinCols, joinType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def join(other: GenericDataFrame, condition: GenericColumn, joinType: String): SparkDataFrame = {
    (other, condition) match {
      case (sparkOther: SparkDataFrame, sparkCondition: SparkColumn) => SparkDataFrame(inner.join(sparkOther.inner, sparkCondition.inner, joinType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def select(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkDataFrame(inner.select(columns.map(_.asInstanceOf[SparkColumn].inner): _*))
  }

  override def groupBy(columns: Seq[GenericColumn]): SparkGroupedDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkGroupedDataFrame(inner.groupBy(sparkCols: _*))
  }

  override def agg(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail: _*))
  }

  override def unionByName(other: GenericDataFrame, allowMissingColumns: Boolean = false): SparkDataFrame = {
    other match {
      case sparkOther: SparkDataFrame => SparkDataFrame(inner.unionByName(sparkOther.inner, allowMissingColumns))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def except(other: GenericDataFrame): SparkDataFrame = {
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

  override def limit(n: Int): SparkDataFrame = {
    SparkDataFrame(inner.limit(n))
  }

  override def orderBy(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkDataFrame(inner.orderBy(sparkCols: _*))
  }

  override def collect: Seq[GenericRow] = inner.collect().map(SparkRow)

  override def distinct: SparkDataFrame = SparkDataFrame(inner.distinct())

  override def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): SparkSubFeed = {
    SparkSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }

  override def withColumn(colName: String, expression: GenericColumn): SparkDataFrame = {
    expression match {
      case sparkExpression: SparkColumn => SparkDataFrame(inner.withColumn(colName, sparkExpression.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }

  override def withColumnRenamed(colName: String, newName: String): SparkDataFrame = {
    SparkDataFrame(inner.withColumnRenamed(colName, newName))
  }

  override def drop(colName: String): SparkDataFrame = SparkDataFrame(inner.drop(colName))

  override def drop(col: GenericColumn): GenericDataFrame = {
    col match {
      case sparkCol: SparkColumn => SparkDataFrame(inner.drop(sparkCol.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(col)
    }
  }

  override def createOrReplaceTempView(viewName: String): Unit = {
    inner.createOrReplaceTempView(viewName)
  }

  override def dropDuplicates(cols: Seq[String]): SparkDataFrame = {
    SparkDataFrame(inner.dropDuplicates(cols))
  }

  override def isEmpty: Boolean = inner.isEmpty

  override def count: Long = inner.count()

  override def cache: GenericDataFrame = SparkDataFrame(inner.cache())

  override def uncache: GenericDataFrame = SparkDataFrame(inner.unpersist())

  override def as(alias: String): GenericDataFrame = SparkDataFrame(inner.as(alias))

  override def showString(options: Map[String, String] = Map()): String = {
    val numRows = options.get("numRows").map(_.toInt).getOrElse(10)
    val truncate = options.get("truncate").map(_.toInt).getOrElse(20)
    val vertical = options.get("vertical").exists(_.toBoolean)
    DatasetHelper.showString(inner, numRows, truncate, vertical)
  }

  def explainString(options: Map[String, String] = Map()): String = {
    val mode = options.getOrElse("mode", "simple")
    inner.queryExecution.explainString(ExplainMode.fromString(mode.toLowerCase))
  }

  override def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean = false): (GenericDataFrame, DataFrameObservation) = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, aggregateColumns)
    // Some Spark data sources dont execute observations, e.g. jdbc. The generic observation can be forced for these cases.
    if (forceGenericObservation) {
      val observation = GenericCalculatedObservation(this, aggregateColumns: _*)
      // Cache the DataFrame to avoid duplicate calculation. If cache is not needed, create a GenericCalculationObservation directly.
      (this.cache, observation)
    } else {
      val observation = new SparkObservation(name)
      val sparkAggregatedColumns = aggregateColumns.map(_.asInstanceOf[SparkColumn].inner)
      val dfObserved = observation.on(inner, isExecPhase, sparkAggregatedColumns: _*)
      (SparkDataFrame(dfObserved), observation)
    }
  }

  def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): GenericDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, aggregateColumns)
    val sparkAggregatedColumns = aggregateColumns.map(_.asInstanceOf[SparkColumn].inner)
    val dfObserved = inner.observe(name, sparkAggregatedColumns.head, sparkAggregatedColumns.tail: _*)
    SparkDataFrame(dfObserved)
  }

  override def apply(columnName: String): GenericColumn = SparkColumn(inner.apply(columnName))

  override def isStreaming: Boolean = inner.isStreaming
}

case class SparkGroupedDataFrame(inner: RelationalGroupedDataset) extends GenericGroupedDataFrame {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def agg(columns: Seq[GenericColumn]): SparkDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkColumn].inner)
    SparkDataFrame(inner.agg(sparkCols.head, sparkCols.tail: _*))
  }
}

case class SparkSchema(inner: StructType) extends GenericSchema {

  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = {
    val sparkSchema = schema.convert(subFeedType).asInstanceOf[SparkSchema]
    val missingCols = SchemaUtil.schemaDiff(this, sparkSchema,
      ignoreNullable = Environment.schemaValidationIgnoresNullability,
      deep = Environment.schemaValidationDeepComarison,
      caseSensitive = Environment.caseSensitive
    )
    if (missingCols.nonEmpty) Some(SparkSchema(StructType(missingCols.collect { case x: SparkField => x.inner }.toSeq)))
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
    SparkDataFrame(dataset.getEmptyDataFrame(inner)(getSparkSession))
  }

  override def getDataType(colName: String): SparkDataType = {
    SparkDataType(listFind[StructField](inner, colName, _.name, Environment.caseSensitive)
      .getOrElse(throw new IllegalArgumentException(s"Column $colName does not exists. Available: ${inner.fieldNames.mkString(", ")}")).dataType
    )
  }

  override def makeNullable: SparkSchema = SparkSchema(StructType(fields.map(_.makeNullable.inner)))

  override def toLowerCase: SparkSchema = SparkSchema(StructType(fields.map(_.toLowerCase.inner)))

  override def removeMetadata: SparkSchema = SparkSchema(StructType(fields.map(_.removeMetadata.inner)))

  override def treeString(level: Int = Int.MaxValue): String = inner.treeString(level)
}

case class SparkColumn(inner: Column) extends GenericColumn {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def ===(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner === sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def =!=(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner =!= sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <=>(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner <=> sparkColumn.inner)
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

  override def >=(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner >= sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <=(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkColumn => SparkColumn(inner <= sparkColumn.inner)
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

  override def isin(list: Any*): GenericColumn = SparkColumn(inner.isin(list: _*))

  override def isNull: GenericColumn = SparkColumn(inner.isNull)

  override def isNotNull: GenericColumn = SparkColumn(inner.isNotNull)

  override def as(name: String): GenericColumn = SparkColumn(inner.as(name))

  override def cast(dataType: GenericDataType): GenericColumn = {
    dataType match {
      case sparkDataType: SparkDataType => SparkColumn(inner.cast(sparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }

  override def exprSql: String = inner.expr.sql

  override def desc: GenericColumn = SparkColumn(inner.desc)

  override def apply(extraction: Any): GenericColumn = SparkColumn(inner.apply(extraction))

  override def getName: Option[String] = inner.expr match {
    case c: NamedExpression => Some(c.name)
    case _ => None
  }
}

case class SparkField(inner: StructField) extends GenericField {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def name: String = inner.name

  override def dataType: SparkDataType = SparkDataType(inner.dataType)

  override def nullable: Boolean = inner.nullable

  override def comment: Option[String] = inner.getComment()

  override def makeNullable: SparkField = SparkField(inner.copy(dataType = dataType.makeNullable.inner, nullable = true))

  override def toLowerCase: SparkField = SparkField(inner.copy(dataType = dataType.toLowerCase.inner, name = inner.name.toLowerCase))

  override def removeMetadata: SparkField = SparkField(inner.copy(dataType = dataType.removeMetadata.inner, metadata = Metadata.empty))
}


trait SparkDataType extends GenericDataType {
  def inner: DataType

  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def isSortable: Boolean = inner match {
    case StringType | LongType | IntegerType | ShortType | FloatType | DoubleType | TimestampType | DateType => true
    case _: DecimalType => true // strange scala error in IntelliJ for unapply method, but ok if matching against the type.
    case _ => false
  }

  override def typeName: String = standardizeTypeName(inner.typeName.toLowerCase)

  override def sql: String = inner.sql

  override def makeNullable: SparkDataType

  override def toLowerCase: SparkDataType

  override def removeMetadata: SparkDataType

  override def isSameType(other: GenericDataType): Boolean = {
    other match {
      case sparkOther: SparkDataType => DataType.equalsIgnoreNullability(inner, sparkOther.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
}

case class SparkSimpleDataType(inner: DataType) extends SparkDataType with GenericSimpleDataType {
  override def makeNullable: SparkDataType = this

  override def toLowerCase: SparkDataType = this

  override def removeMetadata: SparkDataType = this

  override def isNumeric: Boolean = inner.isInstanceOf[NumericType]

  override def isImpreciseNumeric: Boolean = inner.isInstanceOf[FloatType] || inner.isInstanceOf[DoubleType]

  override def getDecimalSpec: Option[(Int, Int)] = inner match {
    case d: DecimalType => Some((d.precision, d.scale))
    case _ => None
  }

  def toJson: JValue = JString(inner.typeName)

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

  override def fieldIndex(fieldName: String): Int = inner.fieldIndex(fieldName)
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
  override def makeNullable: SparkDataType = SparkMapDataType(MapType(SparkDataType(inner.keyType).makeNullable.inner, SparkDataType(inner.valueType).makeNullable.inner, valueContainsNull = true))

  override def toLowerCase: SparkDataType = SparkMapDataType(MapType(SparkDataType(inner.keyType).toLowerCase.inner, SparkDataType(inner.valueType).toLowerCase.inner, valueContainsNull = inner.valueContainsNull))

  override def removeMetadata: SparkDataType = SparkMapDataType(MapType(SparkDataType(inner.keyType).removeMetadata.inner, SparkDataType(inner.valueType).removeMetadata.inner, valueContainsNull = inner.valueContainsNull))

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

  override def getStruct(index: Int): GenericRow = SparkRow(inner.getStruct(index))

  override def getAs[T: ClassTag](index: Int): T = inner.getAs[T](index)

  override def toSeq: Seq[Any] = inner.toSeq
}

case class SparkUnaryUdf(inner: Column => Column) extends GenericUnaryUdf {
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]

  override def convert(genericColumn: GenericColumn): GenericColumn = SparkColumn(inner(genericColumn.asInstanceOf[SparkColumn].inner))
}