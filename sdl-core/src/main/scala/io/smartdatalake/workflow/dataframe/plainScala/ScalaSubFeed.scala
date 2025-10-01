/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion, SubFeed}

import scala.reflect.runtime.universe.{Type, typeOf}

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

  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): ScalaSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, filter = result.filter, isSkipped = false, dataFrame = None)
  }
}

object ScalaSubFeed extends DataFrameSubFeedCompanion {

  // Members declared in io.smartdatalake.workflow.DataFrameSubFeedCompanion
  override def createSchema(fields: Seq[GenericField]): GenericSchema = {
    if (fields.isEmpty) throw new IllegalArgumentException("Please provide at least one field to create a schema")
    fields.head match {
      case scalaField: ScalaColumnDefinition[_] => ScalaSchema(fields.map(_.asInstanceOf[ScalaColumnDefinition[_]]).toList)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(fields.head)
    }
  }

  override protected def subFeedType: reflect.runtime.universe.Type = typeOf[ScalaSubFeed]

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


  def throwNotImplementedError = throw new NotImplementedError("This Spark-like Dataframe function is not implemented for the plainScala version")

  def approxCountDistinct(columns: GenericColumn, rsd: Option[Double]): GenericColumn = throwNotImplementedError

  def array(columns: GenericColumn*): GenericColumn = throwNotImplementedError

  def arrayType(dataType: GenericDataType): GenericDataType with GenericArrayDataType = throwNotImplementedError

  def array_construct_compact(columns: GenericColumn*): GenericColumn = throwNotImplementedError

  def coalesce(columns: GenericColumn*): GenericColumn = throwNotImplementedError

  def col(colName: String): GenericColumn = throwNotImplementedError

  def concat(exprs: GenericColumn*): GenericColumn = throwNotImplementedError

  def count(column: GenericColumn): GenericColumn = throwNotImplementedError

  def countDistinct(columns: GenericColumn*): GenericColumn = throwNotImplementedError

  def explode(column: GenericColumn): GenericColumn = throwNotImplementedError

  def expr(sqlExpr: String): GenericColumn = throwNotImplementedError

  def field(name: String, dataType: GenericDataType, nullable: Boolean): GenericField = throwNotImplementedError

  def hash(column: GenericColumn): GenericColumn = throwNotImplementedError

  def lit(value: Any): GenericColumn = throwNotImplementedError

  def mapType(keyType: GenericDataType, valueType: GenericDataType): GenericDataType with GenericMapDataType = throwNotImplementedError

  def max(column: GenericColumn): GenericColumn = throwNotImplementedError

  def min(column: GenericColumn): GenericColumn = throwNotImplementedError

  def not(column: GenericColumn): GenericColumn = throwNotImplementedError

  def raise_error(column: GenericColumn): GenericColumn = throwNotImplementedError

  def regexp_extract(e: GenericColumn, regexp: String, groupIdx: Int): GenericColumn = throwNotImplementedError

  def rowFromSeq(values: Seq[Any]): GenericRow = throwNotImplementedError

  def row_number: GenericColumn = throwNotImplementedError

  def schemaEvolutionUdf(srcType: GenericDataType, tgtType: GenericDataType): GenericUnaryUdf = throwNotImplementedError

  def size(column: GenericColumn): GenericColumn = throwNotImplementedError

  def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = throwNotImplementedError

  def stringType: GenericDataType = throwNotImplementedError

  def struct(columns: GenericColumn*): GenericColumn = throwNotImplementedError

  def structType(fields: Seq[GenericField]): GenericDataType with GenericStructDataType = throwNotImplementedError

  def structType(colTypes: Map[String, GenericDataType]): GenericDataType with GenericStructDataType = throwNotImplementedError

  def transform(column: GenericColumn, func: GenericColumn => GenericColumn): GenericColumn = throwNotImplementedError

  def transform_keys(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): GenericColumn = throwNotImplementedError

  def transform_values(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): GenericColumn = throwNotImplementedError

  def when(condition: GenericColumn, value: GenericColumn): GenericColumn with GenericWhen = throwNotImplementedError

  def window(aggFunction: () => GenericColumn, partitionBy: Seq[GenericColumn], orderBy: GenericColumn): GenericColumn = throwNotImplementedError

  override def from_json(column: GenericColumn, dataType: GenericDataType): GenericColumn = throwNotImplementedError
}


