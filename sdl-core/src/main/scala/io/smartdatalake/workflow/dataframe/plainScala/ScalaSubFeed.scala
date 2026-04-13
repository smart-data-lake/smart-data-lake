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
package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.DataFrameSubFeed.assertCorrectSubFeedType
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion, SubFeed}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.reflect.runtime.universe.TypeTag
import io.smartdatalake.util.misc.SeqUtil._

/**
 * A pure Scala implementation of DataFrames and related classes for testing purposes without Spark dependencies.
 * There are many limitations -> dont use for production!
 * - column names are normally handled case sensitive. It is not configurable.
 * - Performance might be limited for large DataFrames, as algorithms are not optimized
 * - only a limited set of DataFrame functions are implemented, for example there is no support for UDFs or window functions
 */
case class ScalaSubFeed(override val dataFrame: Option[ScalaDataFrame],
                        override val dataObjectId: DataObjectId,
                        override val partitionValues: Seq[PartitionValues] = Seq(),
                        override val isDAGStart: Boolean = false,
                        override val isSkipped: Boolean = false,
                        override val isDummy: Boolean = false,
                        override val filter: Option[String] = None,
                        @transient override val observation: Option[DataFrameObservation] = None,
                        override val metrics: Option[MetricsMap] = None,
                        override val isStreaming: Option[Boolean] = Some(false)
                       ) extends DataFrameSubFeed {
  @transient override def tpe: Type = typeOf[ScalaSubFeed]

  override def persist: ScalaSubFeed = this

  override def unpersist: ScalaSubFeed = this

  override def hasReusableDataFrame: Boolean = dataFrame.isDefined && !isDummy && !isStreaming.getOrElse(false)

  override def withDataFrame(dataFrame: Option[GenericDataFrame]): ScalaSubFeed = this.copy(dataFrame = dataFrame.map(_.asInstanceOf[ScalaDataFrame]))

  override def toOutput(dataObjectId: SdlConfigObject.DataObjectId): ScalaSubFeed = this.copy(dataFrame = None, filter = None, isDAGStart = false, isSkipped = false, isDummy = false, dataObjectId = dataObjectId, observation = None, metrics = None)

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): ScalaSubFeed = {

    val (dataFrame, dummy) = other match {
      // both subfeeds have a DataFrame to reuse -> union DataFrames
      case scalaSubFeed: ScalaSubFeed if this.hasReusableDataFrame && scalaSubFeed.hasReusableDataFrame =>
        (this.dataFrame.map(_.unionByName(scalaSubFeed.dataFrame.get)), false)
      // both subfeeds have DataFrames, but they are not reusable, e.g. they just transport the schema
      case scalaSubFeed: ScalaSubFeed if this.dataFrame.isDefined || scalaSubFeed.dataFrame.isDefined =>
        (this.dataFrame.orElse(scalaSubFeed.dataFrame), true) // if only one subfeed is defined, we need to get a fresh DataFrame and convert this to a dummy
      // otherwise no dataframe
      case _ =>
        (None, false)
    }
    var resultSubfeed: ScalaSubFeed = this.copy(dataFrame = dataFrame.asInstanceOf[Option[ScalaDataFrame]]
      , partitionValues = unionPartitionValues(other.partitionValues)
      , isDAGStart = this.isDAGStart || other.isDAGStart
      , isSkipped = this.isSkipped && other.isSkipped
    )
    if (dummy && dataFrame.isDefined) resultSubfeed = this.copy(dataFrame = Some(ScalaDataFrame.returnEmpty(dataFrame.get.asInstanceOf[ScalaDataFrame].schema)), isDummy = true)
    // return
    resultSubfeed
  }

  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: SdlConfigObject.DataObjectId)(implicit context: ActionPipelineContext): ScalaSubFeed = {
    // apply input filter
    val inputFilter = if (this.dataObjectId == mainInputId) result.filter else None
    this.copy(partitionValues = result.inputPartitionValues, filter = inputFilter, isSkipped = false).breakLineage // breaklineage keeps DataFrame schema without content
      .asInstanceOf[ScalaSubFeed]
  }

  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): ScalaSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, filter = result.filter, isSkipped = false, dataFrame = None)
  }

  def applyExecutionModeResultForOutput(result: ExecutionModeResult, partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])(implicit context: ActionPipelineContext): ScalaSubFeed = {
    this.copy(partitionValues = result.getOutputPartitionValues(partitionValuesTransform), filter = result.filter, isSkipped = false, dataFrame = None)
  }
}

object ScalaSubFeed extends DataFrameSubFeedCompanion {

  // Members declared in io.smartdatalake.workflow.DataFrameSubFeedCompanion
  override def createSchema(fields: Seq[GenericField]): GenericSchema = {
    if (fields.isEmpty) throw new IllegalArgumentException("Please provide at least one field to create a schema")
    fields.head match {
      case _: ScalaColumnDefinition[_] => ScalaSchema(fields.map(_.asInstanceOf[ScalaColumnDefinition[_]]).toList)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(fields.head)
    }
  }

  @transient override def subFeedType: reflect.runtime.universe.Type = typeOf[ScalaSubFeed]
  override def getSubFeed(dataFrame: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    dataFrame match {
      case scalaDf: ScalaDataFrame => ScalaSubFeed(Some(scalaDf), dataObjectId, partitionValues)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataFrame)
    }
  }

  def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = schema match {
    case ss: ScalaSchema => ScalaDataFrame.returnEmpty(ss)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
  }

  // Members declared in io.smartdatalake.workflow.SubFeedConverter
  //TODO: ActionPipelineContext still has Spark dependencies (!)
  def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    subFeed match {
      case scalaSubFeed: ScalaSubFeed => scalaSubFeed.clearFilter().asInstanceOf[ScalaSubFeed] // make sure there is no filter, as filter can not be passed between actions.
      case _ => ScalaSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }


  /**
   * Members declared in io.smartdatalake.workflow.dataframe.DataFrameFunctions
   * will only be implemented as needed for the tests.
   */


  private def throwNotImplementedError: Nothing = throw new NotImplementedError("This Spark-like Dataframe function is not implemented for the plainScala version")

  def approxCountDistinct(column: GenericColumn, rsd: Option[Double]): ScalaAbstractColumn = throwNotImplementedError

  def array(columns: GenericColumn*): ScalaAbstractColumn = {
    val scalaColumns = columns.map {
      case c: ScalaAbstractColumn => c
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
    val dataTypes = scalaColumns.map(_.dataType).distinct
    assert(dataTypes.size == 1, "All columns in array function must have the same data type, but found: " + dataTypes.mkString(", "))
    ScalaManyExpr(scalaColumns, "array", _ => v => if (v.nonEmpty) Some(v.map(_.get)) else None, Some(ScalaArrayDataType(Some(dataTypes.head))))
  }

  def arrayType(dataType: GenericDataType): GenericDataType with GenericArrayDataType = throwNotImplementedError

  def array_construct_compact(columns: GenericColumn*): ScalaAbstractColumn = throwNotImplementedError

  def coalesce(columns: GenericColumn*): ScalaAbstractColumn = throwNotImplementedError

  def col(colName: String): ScalaAbstractColumn = ScalaColumnReference(colName)

  def concat(exprs: GenericColumn*): ScalaAbstractColumn = {
    require(exprs.nonEmpty, "concat requires at least one argument")
    val scalaExprs = exprs.map {
      case c: ScalaAbstractColumn => c
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
    scalaExprs.reduce { (left, right) =>
      ScalaBinaryExpr(left, right, "concat", _ => (a, b) => Some(s"${a.getOrElse("")}${b.getOrElse("")}"), Some(ScalaStringDataType))
    }
  }

  def count(column: GenericColumn): ScalaAbstractColumn = column match {
    case c: ScalaAbstractColumn if c == ScalaColumnReference("*") => ScalaAggregateExpr(c, "count", v => Some(v.size), () => ScalaIntDataType)
    case c: ScalaAbstractColumn => ScalaAggregateExpr(c, "count", v => Some(v.count(_.isDefined)), () => ScalaIntDataType)
    case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  def countDistinct(column: GenericColumn): ScalaAbstractColumn = column match {
    case c: ScalaAbstractColumn => ScalaAggregateExpr(c, "count", v => Some(v.flatten.distinct.size), () => ScalaIntDataType)
    case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  def explode(column: GenericColumn): ScalaAbstractColumn = {
    column match {
      case col: ScalaAbstractColumn => ScalaExplodeExpr(col)
      case _ => throw new IllegalArgumentException("The 'explode' function can only be used with a Sequence data type (ScalaSeqDataType)")
    }
  }

  def expr(sqlExpr: String): GenericColumn = ExpressionParser.parse(sqlExpr)(this)

  def field(name: String, dataType: GenericDataType, nullable: Boolean): GenericField = throwNotImplementedError

  def hash(column: GenericColumn): ScalaAbstractColumn = throwNotImplementedError

  override def colsComparisionExpr(cols: Seq[GenericColumn], useHash: Boolean): ScalaAbstractColumn = {
    assert(cols.forall(_.getName.nonEmpty), "All columns must have a name for colsComparisionExpr, otherwise the generated expression is not deterministic. Please check that all columns used for comparison are named.")
    val colNames = cols.map(_.getName.get)
    val scalaCols = cols.map {
      case c: ScalaAbstractColumn => c
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }.sortBy(_.getName.get)
    if (useHash) ScalaManyExpr(scalaCols, "hash_comparison", dataType => data => Some(colNames.zip(data).hashCode()), Some(ScalaIntDataType))
    else ScalaManyExpr(scalaCols, "concat_comparison", dataType => data => Some(colNames.zip(data.map(d => d.map(_.toString).getOrElse("<null>"))).map{case (a,b) => a+"="+b}.mkString(",")), Some(ScalaStringDataType))
  }

  def lit(value: Any): ScalaAbstractColumn = {
    val cls = value match {
      case x: Option[_] => x.map(_.getClass)
      case x => Option(x).map(_.getClass)
    }
    ScalaDataType.getFor(cls.getOrElse(classOf[Null])).createLiteral(value)
  }

  def mapType(keyType: GenericDataType, valueType: GenericDataType): GenericDataType with GenericMapDataType = throwNotImplementedError

  def max(column: GenericColumn): ScalaAbstractColumn = column match {
    case c: ScalaAbstractColumn => ScalaAggregateExpr(c, "max", d => d.maxOption(c.dataType.ordering.asInstanceOf[Ordering[Option[Any]]]).flatten, () => c.dataType)
    case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  def min(column: GenericColumn): ScalaAbstractColumn = column match {
    case c: ScalaAbstractColumn => ScalaAggregateExpr(c, "min", d => d.minOption(c.dataType.ordering.asInstanceOf[Ordering[Option[Any]]]).flatten, () => c.dataType)
    case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  def first(column: GenericColumn): ScalaAbstractColumn = column match {
    case c: ScalaAbstractColumn => ScalaAggregateExpr(c, "first", d => d.headOption.flatten, () => c.dataType)
    case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  def abs(column: GenericColumn): ScalaAbstractColumn = {
    column match {
      case c: ScalaAbstractColumn if c.dataType.isNumeric => ScalaUnaryExpr(c, "abs", v => v.map(v => c.dataType.numeric.abs(v.asInstanceOf[Nothing])), Some(ScalaBooleanDataType))
      case c: ScalaAbstractColumn => throw new IllegalStateException(s"Invalid data type for 'not' function: ${c.dataType.getClass.getSimpleName}. Only Boolean data type is supported.")
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  def least(columns: GenericColumn*): ScalaAbstractColumn = {
    val scalaColumns = columns.map {
      case c: ScalaAbstractColumn => c
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
    scalaColumns.reduce { (left, right) =>
      ScalaBinaryExpr(left, right, "least", dataType => {
        (a, b) => if (dataType.ordering.lteq(a, b)) a else b
      })
    }
  }

  def greatest(columns: GenericColumn*): ScalaAbstractColumn = {
    val scalaColumns = columns.map {
      case c: ScalaAbstractColumn => c
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
    scalaColumns.reduce { (left, right) =>
      ScalaBinaryExpr(left, right, "greatest", dataType => {
        (a, b) => if (dataType.ordering.gteq(a, b)) a else b
      })
    }
  }

  def not(column: GenericColumn): ScalaAbstractColumn = {
    column match {
      case c: ScalaAbstractColumn if c.dataType == ScalaBooleanDataType =>
        ScalaUnaryExpr(c, "not", v => v.map(v => !v.asInstanceOf[Boolean]), Some(ScalaBooleanDataType))
      case c: ScalaAbstractColumn => throw new IllegalStateException(s"Invalid data type for 'not' function: ${c.dataType.getClass.getSimpleName}. Only Boolean data type is supported.")
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  def raise_error(column: GenericColumn): ScalaAbstractColumn = throwNotImplementedError

  def regexp_extract(e: GenericColumn, regexp: String, groupIdx: Int): ScalaAbstractColumn = throwNotImplementedError

  def rowFromSeq(values: Seq[Any]): GenericRow = throwNotImplementedError

  def row_number: ScalaAbstractColumn = throwNotImplementedError

  def schemaEvolutionUdf(srcType: GenericDataType, tgtType: GenericDataType): GenericUnaryUdf = throwNotImplementedError

  def size(column: GenericColumn): ScalaAbstractColumn = {
    column match {
      case c: ScalaAbstractColumn if c.dataType.isInstanceOf[ScalaArrayDataType] =>
        ScalaUnaryExpr(c, "size", v => v.map(_.asInstanceOf[Seq[_]].size).orElse(Some(0)), Some(ScalaIntDataType))
      case c: ScalaAbstractColumn if c.dataType == ScalaStringDataType =>
        ScalaUnaryExpr(c, "size", v => v.map(_.asInstanceOf[String].length), Some(ScalaIntDataType))
      case c: ScalaAbstractColumn => throw new IllegalStateException(s"Invalid data type for 'size' function: ${c.dataType.getClass.getSimpleName}. Only String or Seq data type is supported.")
      case other => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = throwNotImplementedError

  def stringType: GenericDataType = throwNotImplementedError

  def struct(columns: GenericColumn*): ScalaAbstractColumn = throwNotImplementedError

  def structType(fields: Seq[GenericField]): GenericDataType with GenericStructDataType = throwNotImplementedError

  def structType(colTypes: Map[String, GenericDataType]): GenericDataType with GenericStructDataType = throwNotImplementedError

  def transform(column: GenericColumn, func: GenericColumn => GenericColumn): ScalaAbstractColumn = throwNotImplementedError

  def transform_keys(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): ScalaAbstractColumn = throwNotImplementedError

  def transform_values(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): ScalaAbstractColumn = throwNotImplementedError

  def when(condition: GenericColumn, value: GenericColumn): ScalaAbstractColumn with GenericWhen = {
    (condition, value) match {
      case (scalaCondition: ScalaAbstractColumn, sparkValue: ScalaAbstractColumn) => ScalaWhenExpr(scalaCondition, sparkValue)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${condition.subFeedType.typeSymbol.name}, ${value.subFeedType.typeSymbol.name} in method when")
    }
  }

  def window(aggFunction: () => GenericColumn, partitionBy: Seq[GenericColumn], orderBy: GenericColumn): ScalaAbstractColumn = throwNotImplementedError

  override def from_json(column: GenericColumn, dataType: GenericDataType): ScalaAbstractColumn = throwNotImplementedError

  def createField(name: String, dataType: GenericDataType, nullable: Boolean, comment: Option[String]): GenericField = {
    dataType match {
      case scalaDataType: ScalaDataType[_] => scalaDataType.createColumnDefinition(name, nullable, comment)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${dataType.subFeedType.typeSymbol.name}")
    }
  }

  def createSimpleDataType(tpe: String): GenericDataType with GenericSimpleDataType = ScalaDataType.getFor(tpe)

  def createStructDataType(fields: Seq[GenericField]): GenericDataType with GenericStructDataType = throwNotImplementedError

  def createArrayDataType(valueTpe: GenericDataType): GenericDataType with GenericArrayDataType = throwNotImplementedError

  def createMapDataType(keyTpe: GenericDataType, valueTpe: GenericDataType): GenericDataType with GenericMapDataType = throwNotImplementedError

  override def createDataFrame[A <: Product: ClassTag: TypeTag](rows: Seq[A])(implicit context: ActionPipelineContext): GenericDataFrame = {
    ScalaDataFrame.fromData(rows)
  }

  override def createDataFrame[A <: Product: ClassTag: TypeTag](rows: Seq[A], colNames: Seq[String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    ScalaDataFrame.fromData(rows.map(_.productIterator.toSeq), colNames)
  }
}


