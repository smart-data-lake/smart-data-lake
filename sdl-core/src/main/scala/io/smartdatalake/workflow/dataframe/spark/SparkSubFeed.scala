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
import io.smartdatalake.definitions.ExecutionModeResult
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{DataFrameUtil, DummyStreamProvider}
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.dataframe._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, functions}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * A SparkSubFeed is used to transport [[DataFrame]]'s between Actions.
 *
 * @param dataFrame Spark [[DataFrame]] to be processed. DataFrame should not be saved to state (@transient).
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param isDummy true if this subfeed only contains a dummy DataFrame. Dummy DataFrames can be used for validating the lineage in init phase, but not for the exec phase.
 * @param filter a spark sql filter expression. This is used by DataFrameIncrementalMode.
 */
case class SparkSubFeed(@transient override val dataFrame: Option[SparkDataFrame],
                        override val dataObjectId: DataObjectId,
                        override val partitionValues: Seq[PartitionValues],
                        override val isDAGStart: Boolean = false,
                        override val isSkipped: Boolean = false,
                        override val isDummy: Boolean = false,
                        override val filter: Option[String] = None
                       )
  extends DataFrameSubFeed {
  @transient
  override val tpe: Type = typeOf[SparkSubFeed]
  override def breakLineage(implicit context: ActionPipelineContext): SparkSubFeed = {
    // in order to keep the schema but truncate spark logical plan, a dummy DataFrame is created.
    // dummy DataFrames must be exchanged to real DataFrames before reading in exec-phase.
    if(dataFrame.isDefined && !isDummy && !context.simulation) convertToDummy(dataFrame.get.schema) else this
  }
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SparkSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): SparkSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }
  override def movePartitionColumnsLast(partitions: Seq[String]): SparkSubFeed = {
    withDataFrame(dataFrame.map(x => x.movePartitionColsLast(partitions)))
  }
  override def clearDAGStart(): SparkSubFeed = {
    this.copy(isDAGStart = false)
  }
  override def clearSkipped(): SparkSubFeed = {
    this.copy(isSkipped = false)
  }
  override def toOutput(dataObjectId: DataObjectId): SparkSubFeed = {
    this.copy(dataFrame = None, filter=None, isDAGStart = false, isSkipped = false, isDummy = false, dataObjectId = dataObjectId)
  }
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case sparkSubFeed: SparkSubFeed if this.hasReusableDataFrame && sparkSubFeed.hasReusableDataFrame =>
      this.copy(dataFrame = this.dataFrame.map(_.unionByName(sparkSubFeed.dataFrame.get))
        , partitionValues = unionPartitionValues(sparkSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || sparkSubFeed.isDAGStart)
    case sparkSubFeed: SparkSubFeed if this.dataFrame.isDefined || sparkSubFeed.dataFrame.isDefined =>
      this.copy(dataFrame = this.dataFrame.orElse(sparkSubFeed.dataFrame)
        , partitionValues = unionPartitionValues(sparkSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || sparkSubFeed.isDAGStart)
        .convertToDummy(this.dataFrame.orElse(sparkSubFeed.dataFrame).get.schema) // if only one subfeed is defined, we need to get a fresh DataFrame and convert this to a dummy
    case x => this.copy(dataFrame = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }
  override def clearFilter(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SparkSubFeed = {
    // if filter is removed, normally also the DataFrame must be removed so that the next action get's a fresh unfiltered DataFrame with all data of this DataObject
    if (breakLineageOnChange && filter.isDefined) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearFilter")
      this.copy(filter = None).breakLineage
    } else this.copy(filter = None)
  }
  override def persist: SparkSubFeed = {
    this.dataFrame.foreach(_.inner.persist) // Spark's persist & cache can be called without referencing the resulting DataFrame
    this
  }
  override def unpersist: SparkSubFeed = {
    this.dataFrame.foreach(_.inner.unpersist) // Spark's unpersist can be called without referencing the resulting DataFrame
    this
  }
  override def isStreaming: Option[Boolean] = dataFrame.map(_.inner.isStreaming)
  override def hasReusableDataFrame: Boolean = dataFrame.isDefined && !isDummy && !isStreaming.getOrElse(false)
  // TODO: still needed?
  def getFilterCol: Option[Column] = {
    filter.map(functions.expr)
  }
  private[smartdatalake] def convertToDummy(schema: SparkSchema)(implicit context: ActionPipelineContext): SparkSubFeed = {
    val dummyDf = dataFrame.map{
      dataFrame =>
        if (dataFrame.inner.isStreaming) SparkDataFrame(DummyStreamProvider.getDummyDf(schema.inner)(context.sparkSession))
        else schema.getEmptyDataFrame(dataObjectId)
    }
    this.copy(dataFrame = dummyDf, isDummy = true)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SparkSubFeed = {
    // apply input filter
    val inputFilter = if (this.dataObjectId == mainInputId) result.filter else None
    this.copy(partitionValues = result.inputPartitionValues, filter = inputFilter, isSkipped = false).breakLineage // breaklineage keeps DataFrame schema without content
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SparkSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, filter = result.filter, isSkipped = false, dataFrame = None)
  }
  override def withDataFrame(dataFrame: Option[GenericDataFrame]): SparkSubFeed = this.copy(dataFrame = dataFrame.map(_.asInstanceOf[SparkDataFrame]))
  override def withPartitionValues(partitionValues: Seq[PartitionValues]): DataFrameSubFeed = this.copy(partitionValues = partitionValues)
  override def asDummy(): SparkSubFeed = this.copy(isDummy = true)
  override def withFilter(partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = {
    this.copy(partitionValues = partitionValues, filter = filter)
      .applyFilter
  }
}

object SparkSubFeed extends DataFrameSubFeedCompanion {
  /**
   * This method is used to pass an output SubFeed as input SparkSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): SparkSubFeed = {
    subFeed match {
      case sparkSubFeed: SparkSubFeed => sparkSubFeed.clearFilter() // make sure there is no filter, as filter can not be passed between actions.
      case _ => SparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
  override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def col(colName: String): GenericColumn = {
    SparkColumn(functions.col(colName))
  }
  override def lit(value: Any): GenericColumn = {
    SparkColumn(functions.lit(value))
  }
  override def max(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.max(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def count(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.count(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def explode(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.explode(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = {
    schema match {
      case sparkSchema: SparkSchema => SparkDataFrame(DataFrameUtil.getEmptyDataFrame(sparkSchema.inner)(context.sparkSession))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
    }
  }
  override def getEmptyStreamingDataFrame(schema: GenericSchema)(implicit context: ActionPipelineContext): GenericDataFrame = {
    schema match {
      case sparkSchema: SparkSchema => SparkDataFrame(DummyStreamProvider.getDummyDf(sparkSchema.inner)(context.sparkSession))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
    }
  }
  override def getSubFeed(df: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    df match {
      case sparkDf: SparkDataFrame => SparkSubFeed(Some(sparkDf), dataObjectId, partitionValues)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
    }
  }
  override def stringType: GenericDataType = SparkDataType(StringType)
  override def arrayType(dataType: GenericDataType): GenericDataType = {
    dataType match {
      case sparkDataType: SparkDataType => SparkDataType(ArrayType(sparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }
  override def structType(fields: Map[String,GenericDataType]): GenericDataType = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields.values.toSeq)
    val sparkFields = fields.map{ case (name,dataType) => StructField(name, dataType.asInstanceOf[SparkDataType].inner)}.toSeq
    SparkDataType(StructType(sparkFields))
  }
  /**
   * Construct array from given columns removing null values (Snowpark API)
   */
  override def array_construct_compact(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SparkColumn(functions.flatten(functions.array(functions.array(columns.map(_.asInstanceOf[SparkColumn].inner):_*))))
  }
  override def array(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SparkColumn(functions.array(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def struct(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SparkColumn(functions.struct(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def expr(sqlExpr: String): GenericColumn = SparkColumn(functions.expr(sqlExpr))
  override def when(condition: GenericColumn, value: GenericColumn): GenericColumn = {
    (condition, value) match {
      case (sparkCondition: SparkColumn, sparkValue: SparkColumn) => SparkColumn(functions.when(sparkCondition.inner, sparkValue.inner))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${condition.subFeedType.typeSymbol.name}, ${value.subFeedType.typeSymbol.name} in method when")
    }
  }
  override def not(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.not(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = {
    SparkDataFrame(context.sparkSession.sql(query))
  }
  override def createSchema(fields: Seq[GenericField]): GenericSchema = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields)
    SparkSchema(StructType(fields.map(_.asInstanceOf[SparkField].inner)))
  }
}