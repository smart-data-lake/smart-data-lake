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

package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ScalaUtil
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, DataObject, SchemaValidation, UserDefinedSchema}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.Type

/**
 * A SubFeed that holds a DataFrame
 */
trait DataFrameSubFeed extends SubFeed  {
  @transient
  def tpe: Type // concrete type of this DataFrameSubFeed
  implicit lazy val companion: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(tpe)
  def dataFrame: Option[GenericDataFrame]
  def observation: Option[Observation]
  def persist: DataFrameSubFeed
  def unpersist: DataFrameSubFeed
  def schema: Option[GenericSchema] = dataFrame.map(_.schema)
  def hasReusableDataFrame: Boolean
  def isDummy: Boolean
  def filter: Option[String]
  def clearFilter(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def breakLineage(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def clearDAGStart(): DataFrameSubFeed
  override def clearSkipped(): DataFrameSubFeed
  def isStreaming: Option[Boolean]
  def withDataFrame(dataFrame: Option[GenericDataFrame]): DataFrameSubFeed
  def withObservation(observation: Option[Observation]): DataFrameSubFeed
  def withPartitionValues(partitionValues: Seq[PartitionValues]): DataFrameSubFeed
  def withFilter(partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed
  def applyFilter: DataFrameSubFeed = {
    // apply partition filter
    val partitionValuesColumn = partitionValues.flatMap(_.keys).distinct
    val dfPartitionFiltered = if (partitionValues.isEmpty) dataFrame
    else if (partitionValuesColumn.size == 1) {
      // filter with Sql "isin" expression if only one column
      val filterExpr = companion.col(partitionValuesColumn.head).isin(partitionValues.flatMap(_.elements.values):_*)
      dataFrame.map(_.filter(filterExpr))
    } else {
      // filter with and/or expression if multiple partition columns
      val filterExpr = PartitionValues.createFilterExpr(partitionValues)
      dataFrame.map(_.filter(filterExpr))
    }
    // apply generic filter
    val dfResult = if (filter.isDefined) dfPartitionFiltered.map(_.filter(companion.expr(filter.get)))
    else dfPartitionFiltered
    // return updated SubFeed
    withDataFrame(dfResult)
  }
  def asDummy(): DataFrameSubFeed
  def transform(transformer: GenericDataFrame => GenericDataFrame): DataFrameSubFeed = withDataFrame(dataFrame.map(transformer))
  def movePartitionColumnsLast(partitions: Seq[String]): DataFrameSubFeed
}

trait DataFrameSubFeedCompanion extends SubFeedConverter[DataFrameSubFeed] with DataFrameFunctions {
  protected def subFeedType: universe.Type
  /**
   * This method can create the schema for reading DataObjects.
   * If SubFeed subtypes have DataObjects with other methods to create a schema, they can override this method.
   */
  def getDataObjectReadSchema(dataObject: DataObject with CanCreateDataFrame)(implicit context: ActionPipelineContext): Option[GenericSchema] = {
    dataObject match {
      case input: UserDefinedSchema if input.schema.isDefined =>
        input.schema.map(dataObject.createReadSchema)
      case input: SchemaValidation if input.schemaMin.isDefined =>
        input.schemaMin.map(dataObject.createReadSchema)
      case _ => None
    }
  }

  /**
   * Get an empty DataFrame with a defined schema.
   * @param dataObjectId Snowpark implementation needs to get the Snowpark-Session from the DataObject. This should not be used otherwise.
   */
  def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame
  def getEmptyStreamingDataFrame(schema: GenericSchema)(implicit context: ActionPipelineContext): GenericDataFrame = throw new NotImplementedError(s"getEmptyStreamingDataFrame is not implemented for ${subFeedType.typeSymbol.name}")
  def getSubFeed(dataFrame: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrameSubFeed
  def createSchema(fields: Seq[GenericField]): GenericSchema
}

object DataFrameSubFeed {
  private[smartdatalake] def getCompanion(tpe: Type): DataFrameSubFeedCompanion = ScalaUtil.companionOf[DataFrameSubFeedCompanion](tpe)
  private[smartdatalake] def getCompanion(fullTpeName: String): DataFrameSubFeedCompanion = ScalaUtil.companionOf[DataFrameSubFeedCompanion](fullTpeName)

  /**
   * Get implementation of generic DataFrameFunctions.
   */
  def getFunctions(tpe: Type): DataFrameFunctions = getCompanion(tpe) // down cast to reduce interface
  private[smartdatalake] def getFunctions(fullTpeName: String): DataFrameFunctions = ScalaUtil.companionOf[DataFrameFunctions](fullTpeName)

  /**
   * Helper method to throw exception for wrong subfeed type including method name of caller
   */
  private[smartdatalake] def throwIllegalSubFeedTypeException(obj: GenericTypedObject): Nothing = {
    val parentMethod = Thread.currentThread().getStackTrace.drop(2).find(_.getClassName.startsWith("io.smartdatalake")).map(_.getMethodName).getOrElse("<unknown>")
    throw new IllegalStateException(s"Unsupported subFeedType ${obj.subFeedType.typeSymbol.name} in method $parentMethod")
  }

  /**
   * Helper method to assert subfeed type for a list of generic objects, throwing exception including method name of caller
   */
  private[smartdatalake] def assertCorrectSubFeedType(expectedTpe: Type, elements: Seq[GenericTypedObject]): Unit = {
    val parentMethod = Thread.currentThread().getStackTrace.drop(2).find(_.getClassName.startsWith("io.smartdatalake")).map(_.getMethodName).getOrElse("<unknown>")
    assert(elements.forall(_.subFeedType =:= expectedTpe), s"Unsupported subFeedType(s) ${elements.filter(c => !(c.subFeedType =:= expectedTpe)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method $parentMethod")
  }
}