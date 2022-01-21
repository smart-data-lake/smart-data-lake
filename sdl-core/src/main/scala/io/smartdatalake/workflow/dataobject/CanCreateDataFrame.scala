/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericDataType, GenericSchema}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.Type

private[smartdatalake] trait CanCreateDataFrame {

  /**
   * Get a Spark DataFrame for given partition values
   */
  def getDataFrame(partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit context: ActionPipelineContext) : GenericDataFrame

  /**
   * Get a GenericDataFrameSubFeed for the given language.
   * See getSubFeedSupportedTypes for supported languages.
   */
  private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed

  /**
   * Declare supported Language for getting DataFrame
   */
  private[smartdatalake] def getSubFeedSupportedTypes: Seq[Type]

  /**
   * Creates the read schema based on a given write schema.
   * Normally this is the same, but some DataObjects can remove & add columns on read (e.g. KafkaTopicDataObject, SparkFileDataObject)
   * In this cases we have to break the DataFrame lineage und create a dummy DataFrame in init phase.
   */
  def createReadSchema(writeSchema: GenericSchema)(implicit context: ActionPipelineContext): GenericSchema = writeSchema

  protected def addFieldIfNotExisting(writeSchema: GenericSchema, colName: String, dataType: GenericDataType): GenericSchema = {
    if (!writeSchema.columns.contains(colName)) writeSchema.add(colName, dataType)
    else writeSchema
  }
}

