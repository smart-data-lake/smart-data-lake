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
package io.smartdatalake.workflow.dataframe.sparkconnect

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.evolution.SchemaEvolution.listFind
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.sparkconnect.SparkConnectSubFeed.getSparkSession
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql._
import org.apache.spark.sql.custom.ColumnUtil
import org.apache.spark.sql.types._
import org.json4s.JString
import org.json4s.JsonAST.JValue

import java.io.ByteArrayOutputStream
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

case class SparkConnectDataFrame(override val inner: DataFrame) extends GenericDataFrame with DataFrameWrapper {
  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def schema: SparkConnectSchema = SparkConnectSchema(inner.schema)

  override def join(other: GenericDataFrame, joinCols: Seq[String], joinType: String): SparkConnectDataFrame = {
    other match {
      case sparkOther: SparkConnectDataFrame => SparkConnectDataFrame(inner.join(sparkOther.inner, joinCols, joinType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def join(other: GenericDataFrame, condition: GenericColumn, joinType: String): SparkConnectDataFrame = {
    (other, condition) match {
      case (sparkOther: SparkConnectDataFrame, sparkCondition: SparkConnectColumn) => SparkConnectDataFrame(inner.join(sparkOther.inner, sparkCondition.inner, joinType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def select(columns: Seq[GenericColumn]): SparkConnectDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkConnectDataFrame(inner.select(columns.map(_.asInstanceOf[SparkConnectColumn].inner).toIndexedSeq: _*))
  }

  override def groupBy(columns: Seq[GenericColumn]): SparkConnectGroupedDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkConnectColumn].inner)
    SparkConnectGroupedDataFrame(inner.groupBy(sparkCols.toIndexedSeq: _*))
  }

  override def agg(columns: Seq[GenericColumn]): SparkConnectDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkConnectColumn].inner)
    SparkConnectDataFrame(inner.agg(sparkCols.head, sparkCols.tail.toIndexedSeq: _*))
  }

  override def unionByName(other: GenericDataFrame, allowMissingColumns: Boolean = false): SparkConnectDataFrame = {
    other match {
      case sparkOther: SparkConnectDataFrame => SparkConnectDataFrame(inner.unionByName(sparkOther.inner, allowMissingColumns))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def except(other: GenericDataFrame): SparkConnectDataFrame = {
    other match {
      case sparkOther: SparkConnectDataFrame => SparkConnectDataFrame(inner.except(sparkOther.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def filter(expression: GenericColumn): SparkConnectDataFrame = {
    expression match {
      case sparkExpr: SparkConnectColumn => SparkConnectDataFrame(inner.filter(sparkExpr.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }

  override def limit(n: Int): SparkConnectDataFrame = {
    SparkConnectDataFrame(inner.limit(n))
  }

  override def orderBy(columns: Seq[GenericColumn]): SparkConnectDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkConnectColumn].inner)
    SparkConnectDataFrame(inner.orderBy(sparkCols.toIndexedSeq: _*))
  }

  override def collect: Seq[GenericRow] = inner.collect().toList.map(SparkConnectRow)

  override def distinct: SparkConnectDataFrame = SparkConnectDataFrame(inner.distinct())

  override def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): SparkConnectSubFeed = {
    SparkConnectSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }

  override def withColumn(colName: String, expression: GenericColumn): SparkConnectDataFrame = {
    expression match {
      case sparkExpression: SparkConnectColumn => SparkConnectDataFrame(inner.withColumn(colName, sparkExpression.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
    }
  }

  override def withColumnRenamed(colName: String, newName: String): SparkConnectDataFrame = {
    SparkConnectDataFrame(inner.withColumnRenamed(colName, newName))
  }

  override def drop(colName: String): SparkConnectDataFrame = SparkConnectDataFrame(inner.drop(colName))

  override def drop(col: GenericColumn): GenericDataFrame = {
    col match {
      case sparkCol: SparkConnectColumn => SparkConnectDataFrame(inner.drop(sparkCol.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(col)
    }
  }

  override def createOrReplaceTempView(viewName: String): Unit = {
    inner.createOrReplaceTempView(viewName)
  }

  override def dropDuplicates(cols: Seq[String]): SparkConnectDataFrame = {
    SparkConnectDataFrame(inner.dropDuplicates(cols))
  }

  override def isEmpty: Boolean = inner.isEmpty

  override def count: Long = inner.count()

  override def cache: GenericDataFrame = SparkConnectDataFrame(inner.cache())

  override def uncache: GenericDataFrame = SparkConnectDataFrame(inner.unpersist())

  override def as(alias: String): GenericDataFrame = SparkConnectDataFrame(inner.as(alias))

  override def showString(options: Map[String, String] = Map()): String = {
    val numRows = options.get("numRows").map(_.toInt).getOrElse(10)
    val truncate = options.get("truncate").map(_.toInt).getOrElse(20)
    val vertical = options.get("vertical").exists(_.toBoolean)
    SparkConnectDataFrame.captureStdout(inner.show(numRows, truncate, vertical))
  }

  def explainString(options: Map[String, String] = Map()): String = {
    val mode = options.getOrElse("mode", "simple")
    SparkConnectDataFrame.captureStdout(inner.explain(mode.toLowerCase))
  }

  override def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean = false): (GenericDataFrame, DataFrameObservation) = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, aggregateColumns)
    // Spark Connect has no QueryExecutionListener on the client side. Metrics are calculated with a separate query on the cached DataFrame.
    val observation = GenericCalculatedObservation(this, aggregateColumns.toIndexedSeq: _*)
    // Cache the DataFrame to avoid duplicate calculation. If cache is not needed, create a GenericCalculationObservation directly.
    (if (isExecPhase) this.cache else this, observation)
  }

  def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): GenericDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, aggregateColumns)
    val sparkAggregatedColumns = aggregateColumns.map(_.asInstanceOf[SparkConnectColumn].inner)
    val dfObserved = inner.observe(name, sparkAggregatedColumns.head, sparkAggregatedColumns.tail.toIndexedSeq: _*)
    SparkConnectDataFrame(dfObserved)
  }

  override def apply(columnName: String): GenericColumn = SparkConnectColumn(inner.apply(columnName))

  override def isStreaming: Boolean = inner.isStreaming
}

object SparkConnectDataFrame {
  /**
   * Spark Connect only offers print methods for show/explain, capture their output from stdout.
   */
  private[sparkconnect] def captureStdout[T](func: => T): String = {
    val baos = new ByteArrayOutputStream()
    Console.withOut(baos)(func)
    baos.toString
  }
}

case class SparkConnectGroupedDataFrame(inner: RelationalGroupedDataset) extends GenericGroupedDataFrame {
  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def agg(columns: Seq[GenericColumn]): SparkConnectDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val sparkCols = columns.map(_.asInstanceOf[SparkConnectColumn].inner)
    SparkConnectDataFrame(inner.agg(sparkCols.head, sparkCols.tail.toIndexedSeq: _*))
  }
}

case class SparkConnectSchema(inner: StructType) extends GenericSchema {

  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = {
    val sparkSchema = schema.convert(subFeedType).asInstanceOf[SparkConnectSchema]
    val missingCols = SchemaUtil.schemaDiff(this, sparkSchema,
      ignoreNullable = Environment.schemaValidationIgnoresNullability,
      deep = Environment.schemaValidationDeepComarison,
      caseSensitive = Environment.caseSensitive
    )
    if (missingCols.nonEmpty) Some(SparkConnectSchema(StructType(missingCols.collect { case x: SparkConnectField => x.inner }.toSeq)))
    else None
  }

  override def columns: Seq[String] = inner.fieldNames.toList

  override def fields: Seq[SparkConnectField] = inner.fields.toList.map(SparkConnectField)

  override def sql: String = inner.toDDL

  override def add(colName: String, dataType: GenericDataType): SparkConnectSchema = {
    val sparkDataType = SchemaConverter.convertDatatype(dataType, subFeedType).asInstanceOf[SparkConnectDataType]
    SparkConnectSchema(inner.add(StructField(colName, sparkDataType.inner)))
  }

  override def add(field: GenericField): SparkConnectSchema = {
    field match {
      case sparkField: SparkConnectField => SparkConnectSchema(inner.add(sparkField.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(field)
    }
  }

  override def remove(colName: String): SparkConnectSchema = {
    SparkConnectSchema(StructType(inner.filterNot(_.name == colName)))
  }

  override def filter(func: GenericField => Boolean): SparkConnectSchema = {
    SparkConnectSchema(StructType(fields.filter(func).map(_.inner)))
  }

  override def getEmptyDataFrame(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SparkConnectDataFrame = {
    SparkConnectDataFrame(getSparkSession.createDataFrame(List.empty[Row].asJava, inner))
  }

  override def getDataType(colName: String): SparkConnectDataType = {
    SparkConnectDataType(listFind[StructField](inner, colName, _.name, Environment.caseSensitive)
      .getOrElse(throw new IllegalArgumentException(s"Column $colName does not exists. Available: ${inner.fieldNames.mkString(", ")}")).dataType
    )
  }

  override def makeNullable: SparkConnectSchema = SparkConnectSchema(StructType(fields.map(_.makeNullable.inner)))

  override def toLowerCase: SparkConnectSchema = SparkConnectSchema(StructType(fields.map(_.toLowerCase.inner)))

  override def removeMetadata: SparkConnectSchema = SparkConnectSchema(StructType(fields.map(_.removeMetadata.inner)))

  override def treeString(level: Int = Int.MaxValue): String = inner.treeString(level)
}

case class SparkConnectColumn(inner: Column) extends GenericColumn {
  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def ===(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner === sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def =!=(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner =!= sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <=>(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner <=> sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def >(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner > sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner < sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def >=(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner >= sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <=(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner <= sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def +(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner + sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def -(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner - sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def /(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner / sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def *(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner * sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def and(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner and sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def or(other: GenericColumn): GenericColumn = {
    other match {
      case sparkColumn: SparkConnectColumn => SparkConnectColumn(inner or sparkColumn.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def isin(list: Any*): GenericColumn = SparkConnectColumn(inner.isin(list.toIndexedSeq: _*))

  override def isNull: GenericColumn = SparkConnectColumn(inner.isNull)

  override def isNotNull: GenericColumn = SparkConnectColumn(inner.isNotNull)

  override def as(name: String): GenericColumn = SparkConnectColumn(inner.as(name))

  override def cast(dataType: GenericDataType): GenericColumn = {
    dataType match {
      case sparkDataType: SparkConnectDataType => SparkConnectColumn(inner.cast(sparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }

  override def exprSql: String = throw new NotImplementedError(s"Converting a column back to a sql expression is not supported by Spark Connect")

  override def desc: GenericColumn = SparkConnectColumn(inner.desc)

  override def apply(extraction: Any): GenericColumn = SparkConnectColumn(inner.apply(extraction))

  override def getName: Option[String] = ColumnUtil.getName(inner)
}

case class SparkConnectField(inner: StructField) extends GenericField {
  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def name: String = inner.name

  override def dataType: SparkConnectDataType = SparkConnectDataType(inner.dataType)

  override def nullable: Boolean = inner.nullable

  override def comment: Option[String] = inner.getComment()

  override def makeNullable: SparkConnectField = SparkConnectField(inner.copy(dataType = dataType.makeNullable.inner, nullable = true))

  override def toLowerCase: SparkConnectField = SparkConnectField(inner.copy(dataType = dataType.toLowerCase.inner, name = inner.name.toLowerCase))

  override def removeMetadata: SparkConnectField = SparkConnectField(inner.copy(dataType = dataType.removeMetadata.inner, metadata = Metadata.empty))
}


trait SparkConnectDataType extends GenericDataType {
  def inner: DataType

  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def isSortable: Boolean = inner match {
    case StringType | LongType | IntegerType | ShortType | FloatType | DoubleType | TimestampType | DateType => true
    case _: DecimalType => true // strange scala error in IntelliJ for unapply method, but ok if matching against the type.
    case _ => false
  }

  override def typeName: String = standardizeTypeName(inner.typeName.toLowerCase)

  override def sql: String = inner.sql

  override def makeNullable: SparkConnectDataType

  override def toLowerCase: SparkConnectDataType

  override def removeMetadata: SparkConnectDataType

  override def isSameType(other: GenericDataType): Boolean = {
    other match {
      case sparkOther: SparkConnectDataType => DataType.equalsIgnoreNullability(inner, sparkOther.inner)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }
}

case class SparkConnectSimpleDataType(inner: DataType) extends SparkConnectDataType with GenericSimpleDataType {
  override def makeNullable: SparkConnectDataType = this

  override def toLowerCase: SparkConnectDataType = this

  override def removeMetadata: SparkConnectDataType = this

  override def isNumeric: Boolean = inner.isInstanceOf[NumericType]

  override def isImpreciseNumeric: Boolean = inner.isInstanceOf[FloatType] || inner.isInstanceOf[DoubleType]

  override def getDecimalSpec: Option[(Int, Int)] = inner match {
    case d: DecimalType => Some((d.precision, d.scale))
    case _ => None
  }

  def toJson: JValue = JString(inner.typeName)

}

case class SparkConnectStructDataType(override val inner: StructType) extends SparkConnectDataType with GenericStructDataType {
  override def makeNullable: SparkConnectDataType = SparkConnectStructDataType(SparkConnectSchema(inner).makeNullable.inner)

  override def toLowerCase: SparkConnectDataType = SparkConnectStructDataType(SparkConnectSchema(inner).toLowerCase.inner)

  override def removeMetadata: SparkConnectDataType = SparkConnectStructDataType(SparkConnectSchema(inner).removeMetadata.inner)

  override def withOtherFields[T](other: GenericStructDataType with GenericDataType, func: (Seq[GenericField], Seq[GenericField]) => T): T = {
    other match {
      case sparkOther: SparkConnectStructDataType => func(inner.fields.toList.map(SparkConnectField), sparkOther.inner.fields.toList.map(SparkConnectField))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def fields: Seq[SparkConnectField] = inner.fields.toList.map(SparkConnectField)

  override def fieldIndex(fieldName: String): Int = inner.fieldIndex(fieldName)
}

case class SparkConnectArrayDataType(inner: ArrayType) extends SparkConnectDataType with GenericArrayDataType {
  override def makeNullable: SparkConnectDataType = SparkConnectArrayDataType(ArrayType(SparkConnectDataType(inner.elementType).makeNullable.inner, containsNull = true))

  override def toLowerCase: SparkConnectDataType = SparkConnectArrayDataType(ArrayType(SparkConnectDataType(inner.elementType).toLowerCase.inner, containsNull = inner.containsNull))

  override def removeMetadata: SparkConnectDataType = SparkConnectArrayDataType(ArrayType(SparkConnectDataType(inner.elementType).removeMetadata.inner, containsNull = inner.containsNull))

  override def withOtherElementType[T](other: GenericArrayDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case sparkOther: SparkConnectArrayDataType => func(SparkConnectDataType(inner.elementType), SparkConnectDataType(sparkOther.inner.elementType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def containsNull: Boolean = inner.containsNull

  override def elementDataType: SparkConnectDataType = SparkConnectDataType(inner.elementType)
}

case class SparkConnectMapDataType(inner: MapType) extends SparkConnectDataType with GenericMapDataType {
  override def makeNullable: SparkConnectDataType = SparkConnectMapDataType(MapType(SparkConnectDataType(inner.keyType).makeNullable.inner, SparkConnectDataType(inner.valueType).makeNullable.inner, valueContainsNull = true))

  override def toLowerCase: SparkConnectDataType = SparkConnectMapDataType(MapType(SparkConnectDataType(inner.keyType).toLowerCase.inner, SparkConnectDataType(inner.valueType).toLowerCase.inner, valueContainsNull = inner.valueContainsNull))

  override def removeMetadata: SparkConnectDataType = SparkConnectMapDataType(MapType(SparkConnectDataType(inner.keyType).removeMetadata.inner, SparkConnectDataType(inner.valueType).removeMetadata.inner, valueContainsNull = inner.valueContainsNull))

  override def withOtherKeyType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case sparkOther: SparkConnectMapDataType => func(SparkConnectDataType(inner.keyType), SparkConnectDataType(sparkOther.inner.keyType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def withOtherValueType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T = {
    other match {
      case sparkOther: SparkConnectMapDataType => func(SparkConnectDataType(inner.valueType), SparkConnectDataType(sparkOther.inner.valueType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def valueContainsNull: Boolean = inner.valueContainsNull

  override def keyDataType: SparkConnectDataType = SparkConnectDataType(inner.keyType)

  override def valueDataType: SparkConnectDataType = SparkConnectDataType(inner.valueType)
}

object SparkConnectDataType {
  def apply(inner: DataType): SparkConnectDataType = inner match {
    case structType: StructType => SparkConnectStructDataType(structType)
    case elementType: ArrayType => SparkConnectArrayDataType(elementType)
    case mapType: MapType => SparkConnectMapDataType(mapType)
    case x => SparkConnectSimpleDataType(x)
  }
}

case class SparkConnectRow(inner: Row) extends GenericRow {
  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def get(index: Int): Any = inner.get(index)

  override def getStruct(index: Int): GenericRow = SparkConnectRow(inner.getStruct(index))

  override def getAs[T: ClassTag](index: Int): T = inner.getAs[T](index)

  override def toSeq: Seq[Any] = inner.toSeq
}

case class SparkConnectUnaryUdf(inner: Column => Column) extends GenericUnaryUdf {
  override def subFeedType: universe.Type = typeOf[SparkConnectSubFeed]

  override def convert(genericColumn: GenericColumn): GenericColumn = SparkConnectColumn(inner(genericColumn.asInstanceOf[SparkConnectColumn].inner))
}
