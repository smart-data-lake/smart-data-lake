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
import io.smartdatalake.definitions.ExecutionModeResult
import io.smartdatalake.util.dag.{DAG, DAGResult}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.ScalaUtil.optionalizeMap
import io.smartdatalake.util.misc.{DataFrameUtil, SmartDataLakeLogger}
import io.smartdatalake.util.streaming.DummyStreamProvider
import io.smartdatalake.workflow.dataobject.FileRef
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * A SubFeed transports references to data between Actions.
 * Data can be represented by different technologies like Files or DataFrame.
 */
sealed trait SubFeed extends DAGResult with SmartDataLakeLogger {
  def dataObjectId: DataObjectId
  def partitionValues: Seq[PartitionValues]
  def isDAGStart: Boolean
  def isSkipped: Boolean

  /**
   * Break lineage.
   * This means to discard an existing DataFrame or List of FileRefs, so that it is requested again from the DataObject.
   * On one side this is usable to break long DataFrame Lineages over multiple Actions and instead reread the data from
   * an intermediate table. On the other side it is needed if partition values or filter condition are changed.
   */
  def breakLineage(implicit context: ActionPipelineContext): SubFeed

  def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SubFeed

  def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): SubFeed

  def clearDAGStart(): SubFeed

  def clearSkipped(): SubFeed

  def toOutput(dataObjectId: DataObjectId): SubFeed

  def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed

  override def resultId: String = dataObjectId.id

  def unionPartitionValues(otherPartitionValues: Seq[PartitionValues]): Seq[PartitionValues] = {
    // union is only possible if both inputs have partition values defined. Otherwise default is no partition values which means to read all data.
    if (this.partitionValues.nonEmpty && otherPartitionValues.nonEmpty) (this.partitionValues ++ otherPartitionValues).distinct
    else Seq()
  }

  def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SubFeed
  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SubFeed
}
object SubFeed {
  def filterPartitionValues(partitionValues: Seq[PartitionValues], partitions: Seq[String]): Seq[PartitionValues] = {
    partitionValues.map( pvs => PartitionValues(pvs.elements.filterKeys(partitions.contains))).filter(_.nonEmpty)
  }
}

/**
 * An interface to be implemented by SubFeed companion objects for subfeed conversion
 */
trait SubFeedConverter[S <: SubFeed] {
  def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): S
  def get(subFeed: SubFeed): S = subFeed match {
    case specificSubFeed: S @unchecked => specificSubFeed
  }
}

/**
 * A SparkSubFeed is used to transport [[DataFrame]]'s between Actions.
 *
 * @param dataFrame Spark [[DataFrame]] to be processed. DataFrame should not be saved to state (@transient).
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param isDummy true if this subfeed only contains a dummy DataFrame. Dummy DataFrames can be used for validating the lineage in init phase, but not for the exec phase.
 * @param filter a spark sql filter expression. This is used by SparkIncrementalMode.
 */
case class SparkSubFeed(@transient dataFrame: Option[DataFrame],
                        override val dataObjectId: DataObjectId,
                        override val partitionValues: Seq[PartitionValues],
                        override val isDAGStart: Boolean = false,
                        override val isSkipped: Boolean = false,
                        isDummy: Boolean = false,
                        filter: Option[String] = None
                       )
  extends SubFeed {
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
  def movePartitionColumnsLast(partitions: Seq[String]): SparkSubFeed = {
    this.copy(dataFrame = this.dataFrame.map( df => HiveUtil.movePartitionColsLast(df, partitions)))
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
      this.copy(dataFrame = Some(this.dataFrame.get.unionByName(sparkSubFeed.dataFrame.get))
        , partitionValues = unionPartitionValues(sparkSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || sparkSubFeed.isDAGStart)
    case sparkSubFeed: SparkSubFeed if this.dataFrame.isDefined || sparkSubFeed.dataFrame.isDefined =>
      this.copy(dataFrame = this.dataFrame.orElse(sparkSubFeed.dataFrame)
        , partitionValues = unionPartitionValues(sparkSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || sparkSubFeed.isDAGStart)
      .convertToDummy(this.dataFrame.orElse(sparkSubFeed.dataFrame).get.schema)
    case x => this.copy(dataFrame = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }
  def clearFilter(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SparkSubFeed = {
    // if filter is removed, also the DataFrame must be removed so that the next action get's a fresh unfiltered DataFrame with all data of this DataObject
    if (breakLineageOnChange && filter.isDefined) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearFilter")
      this.copy(filter = None).breakLineage
    } else this.copy(filter = None)
  }
  def persist: SparkSubFeed = {
    this.copy(dataFrame = this.dataFrame.map(_.persist))
  }
  def isStreaming: Option[Boolean] = dataFrame.map(_.isStreaming)
  def hasReusableDataFrame: Boolean = dataFrame.isDefined && !isDummy && !isStreaming.getOrElse(false)
  def getFilterCol: Option[Column] = {
    filter.map(functions.expr)
  }
  private[smartdatalake] def convertToDummy(schema: StructType)(implicit context: ActionPipelineContext): SparkSubFeed = {
    val dummyDf = dataFrame.map{
      df =>
        if (df.isStreaming) DummyStreamProvider.getDummyDf(schema)(context.sparkSession)
        else DataFrameUtil.getEmptyDataFrame(schema)(context.sparkSession)
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
}
object SparkSubFeed extends SubFeedConverter[SparkSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input SparkSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): SparkSubFeed = {
    subFeed match {
      case sparkSubFeed: SparkSubFeed => sparkSubFeed.clearFilter() // make sure there is no filter, as filter can not be passed between actions.
      case _ => SparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}

/**
 * A FileSubFeed is used to transport references to files between Actions.
 *
 * @param fileRefs path to files to be processed
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param fileRefMapping store mapping of input to output file references. This is also used for post processing (e.g. delete after read).
 */
case class FileSubFeed(fileRefs: Option[Seq[FileRef]],
                       override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isDAGStart: Boolean = false,
                       override val isSkipped: Boolean = false,
                       fileRefMapping: Option[Seq[FileRefMapping]] = None
                      )
  extends SubFeed {
  override def breakLineage(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(fileRefs = None, fileRefMapping = None)
  }

  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): FileSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }

  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): FileSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }

  def checkPartitionValuesColsExisting(partitions: Set[String]): Boolean = {
    partitionValues.forall(pvs => partitions.diff(pvs.keys).isEmpty)
  }

  override def clearDAGStart(): FileSubFeed = {
    this.copy(isDAGStart = false)
  }

  override def clearSkipped(): FileSubFeed = {
    this.copy(isSkipped = false)
  }

  override def toOutput(dataObjectId: DataObjectId): FileSubFeed = {
    this.copy(fileRefs = None, fileRefMapping = None, isDAGStart = false, isSkipped = false, dataObjectId = dataObjectId)
  }

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case fileSubFeed: FileSubFeed if this.fileRefs.isDefined && fileSubFeed.fileRefs.isDefined =>
      this.copy(fileRefs = this.fileRefs.map(_ ++ fileSubFeed.fileRefs.get)
        , partitionValues = unionPartitionValues(fileSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || fileSubFeed.isDAGStart)
    case fileSubFeed: FileSubFeed =>
      this.copy(fileRefs = None, partitionValues = unionPartitionValues(fileSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || fileSubFeed.isDAGStart)
    case x => this.copy(fileRefs = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }

  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, fileRefs = result.fileRefs, fileRefMapping = None)
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, fileRefs = None, fileRefMapping = None)
  }
}
object FileSubFeed extends SubFeedConverter[FileSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input FileSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): FileSubFeed = {
    subFeed match {
      case fileSubFeed: FileSubFeed => fileSubFeed
      case _ => FileSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}

/**
 * Src/Tgt tuple representing the mapping of a file reference
 */
case class FileRefMapping(src: FileRef, tgt: FileRef)

/**
 * A ScriptSubFeed is used to notify DataObjects and subsequent actions about the completion of a script.
 * It allows to pass on arbitrary informations as key/values.
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param parameters arbitrary informations as key/value to pass on
 */
case class ScriptSubFeed(parameters: Option[Map[String,String]] = None,
                         override val dataObjectId: DataObjectId,
                         override val partitionValues: Seq[PartitionValues],
                         override val isDAGStart: Boolean = false,
                         override val isSkipped: Boolean = false
                        )
  extends SubFeed {
  override def breakLineage(implicit context: ActionPipelineContext): ScriptSubFeed = this
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    this.copy(partitionValues = updatedPartitionValues)
  }
  override def clearDAGStart(): ScriptSubFeed = {
    this.copy(isDAGStart = false)
  }
  override def clearSkipped(): ScriptSubFeed = {
    this.copy(isSkipped = false)
  }
  override def toOutput(dataObjectId: DataObjectId): ScriptSubFeed = {
    this.copy(dataObjectId = dataObjectId, parameters = None, isDAGStart = false, isSkipped = false)
  }
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case subFeed: ScriptSubFeed =>
      this.copy(
        parameters = optionalizeMap(this.parameters.getOrElse(Map()) ++ subFeed.parameters.getOrElse(Map())),
        partitionValues = unionPartitionValues(subFeed.partitionValues), isDAGStart = this.isDAGStart || subFeed.isDAGStart
      )
    case x => this.copy(parameters = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, parameters = None)
  }
}
object ScriptSubFeed extends SubFeedConverter[ScriptSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input FileSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): ScriptSubFeed = {
    subFeed match {
      case subFeed: ScriptSubFeed => subFeed
      case _ => ScriptSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}

/**
 * An InitSubFeed is used to initialize first Nodes of a [[DAG]].
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isSkipped true if this subfeed is the result of a skipped action
 */
case class InitSubFeed(override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isSkipped: Boolean = false
                      )
  extends SubFeed {
  override def isDAGStart: Boolean = true
  override def breakLineage(implicit context: ActionPipelineContext): InitSubFeed = this
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): InitSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): InitSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }
  override def clearDAGStart(): InitSubFeed = throw new NotImplementedException() // calling clearDAGStart makes no sense on InitSubFeed
  override def clearSkipped(): InitSubFeed = throw new NotImplementedException() // calling clearSkipped makes no sense on InitSubFeed
  override def toOutput(dataObjectId: DataObjectId): FileSubFeed = throw new NotImplementedException()
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case x => this.copy(partitionValues = unionPartitionValues(x.partitionValues))
  }
  def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
}
