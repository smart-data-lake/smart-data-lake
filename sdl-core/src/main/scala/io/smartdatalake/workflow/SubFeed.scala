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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.DataFrameUtil
import io.smartdatalake.util.streaming.DummyStreamProvider
import io.smartdatalake.workflow.dataobject.FileRef
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * A SubFeed transports references to data between Actions.
 * Data can be represented by different technologies like Files or DataFrame.
 */
trait SubFeed extends DAGResult {
  def dataObjectId: DataObjectId
  def partitionValues: Seq[PartitionValues]
  def isDAGStart: Boolean

  /**
   * Break lineage.
   * This means to discard an existing DataFrame or List of FileRefs, so that it is requested again from the DataObject.
   * This is usable to break long DataFrame Lineages over multiple Actions and instead reread the data from an intermediate table
   * @return
   */
  def breakLineage(implicit session: SparkSession, context: ActionPipelineContext): SubFeed

  def clearPartitionValues(): SubFeed

  def updatePartitionValues(partitions: Seq[String]): SubFeed

  def clearDAGStart(): SubFeed

  override def resultId: String = dataObjectId.id
}

/**
 * A SparkSubFeed is used to transport [[DataFrame]]'s between Actions.
 *
 * @param dataFrame Spark [[DataFrame]] to be processed. DataFrame should not be saved to state (@transient).
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isDummy true if this subfeed only contains a dummy DataFrame. Dummy DataFrames can be used for validating the lineage in init phase, but not for the exec phase.
 * @param filter a spark sql filter expression. This is used by SparkIncrementalMode.
 */
case class SparkSubFeed(@transient dataFrame: Option[DataFrame],
                        override val dataObjectId: DataObjectId,
                        override val partitionValues: Seq[PartitionValues],
                        override val isDAGStart: Boolean = false,
                        isDummy: Boolean = false,
                        filter: Option[String] = None
                       )
  extends SubFeed {
  override def breakLineage(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // in order to keep the schema but truncate spark logical plan, a dummy DataFrame is created.
    // dummy DataFrames must be exchanged to real DataFrames before reading in exec-phase.
    if(dataFrame.isDefined && !context.simulation) convertToDummy(dataFrame.get.schema) else this
  }
  override def clearPartitionValues(): SparkSubFeed = {
    this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String]): SparkSubFeed = {
    val updatedPartitionValues = partitionValues.map( pvs => PartitionValues(pvs.elements.filterKeys(partitions.contains))).filter(_.nonEmpty)
    this.copy(partitionValues = updatedPartitionValues)
  }
  def movePartitionColumnsLast(partitions: Seq[String]): SparkSubFeed = {
    this.copy(dataFrame = this.dataFrame.map( df => HiveUtil.movePartitionColsLast(df, partitions)))
  }
  override def clearDAGStart(): SparkSubFeed = {
    this.copy(isDAGStart = false)
  }
  def clearFilter(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // if filter is removed, also the DataFrame must be removed so that the next action get's an unfiltered DataFrame
    if (filter.isDefined) this.copy(filter = None).breakLineage else this
  }
  def persist: SparkSubFeed = {
    this.copy(dataFrame = this.dataFrame.map(_.persist))
  }
  def isStreaming: Option[Boolean] = dataFrame.map(_.isStreaming)
  def getFilterCol: Option[Column] = {
    filter.map(functions.expr)
  }
  private[smartdatalake] def convertToDummy(schema: StructType)(implicit session: SparkSession): SparkSubFeed = {
    val dummyDf = dataFrame.map{
      df =>
        if (df.isStreaming) DummyStreamProvider.getDummyDf(schema)
        else DataFrameUtil.getEmptyDataFrame(schema)
    }
    this.copy(dataFrame = dummyDf, isDummy = true)
  }
}
object SparkSubFeed {
  def fromSubFeed( subFeed: SubFeed ): SparkSubFeed = {
    subFeed match {
      case sparkSubFeed: SparkSubFeed => sparkSubFeed
      case _ => SparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart)
    }
  }
}

/**
 * A FileSubFeed is used to transport references to files between Actions.
 *
 * @param fileRefs path to files to be processed
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param processedInputFileRefs used to remember processed input FileRef's for post processing (e.g. delete after read)
 */
case class FileSubFeed(fileRefs: Option[Seq[FileRef]],
                       override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isDAGStart: Boolean = false,
                       processedInputFileRefs: Option[Seq[FileRef]] = None
                      )
  extends SubFeed {
  override def breakLineage(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed = {
    this.copy(fileRefs = None)
  }
  override def clearPartitionValues(): FileSubFeed = {
    this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String]): FileSubFeed = {
    val updatedPartitionValues = partitionValues.map( pvs => PartitionValues(pvs.elements.filterKeys(partitions.contains))).filter(_.nonEmpty)
    this.copy(partitionValues = updatedPartitionValues)
  }
  def checkPartitionValuesColsExisting(partitions: Set[String]): Boolean = {
    partitionValues.forall( pvs => partitions.diff(pvs.keys).isEmpty)
  }
  override def clearDAGStart(): FileSubFeed = {
    this.copy(isDAGStart = false)
  }
}
object FileSubFeed {
  def fromSubFeed( subFeed: SubFeed ): FileSubFeed = {
    subFeed match {
      case fileSubFeed: FileSubFeed => fileSubFeed
      case _ => FileSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart)
    }
  }
}

/**
 * A InitSubFeed is used to initialize first Nodes of a [[DAG]].
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 */
case class InitSubFeed(override val dataObjectId: DataObjectId, override val partitionValues: Seq[PartitionValues])
  extends SubFeed {
  override def isDAGStart: Boolean = true
  override def breakLineage(implicit session: SparkSession, context: ActionPipelineContext): InitSubFeed = this
  override def clearPartitionValues(): InitSubFeed = {
    this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String]): InitSubFeed = {
    val updatedPartitionValues = partitionValues.map( pvs => PartitionValues(pvs.elements.filterKeys(partitions.contains))).filter(_.nonEmpty)
    this.copy(partitionValues = updatedPartitionValues)
  }
  override def clearDAGStart(): InitSubFeed = throw new NotImplementedException() // calling clearDAGStart makes no sense on InitSubFeed
}
