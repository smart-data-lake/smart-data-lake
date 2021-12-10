/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.definitions.{Condition, DefaultExecutionModeExpressionData}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ExpressionEvaluationException, SmartDataLakeLogger, SparkExpressionUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

sealed trait HousekeepingMode {
  private[smartdatalake] def prepare(dataObject: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Unit
  private[smartdatalake] def postWrite(dataObject: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Unit
}

/**
 * Keep partitions while retention condition is fulfilled, delete other partitions.
 * Example: cleanup partitions with partition layout dt=<yyyymmdd> after 90 days:
 * {{{
 * housekeepingMode = {
 *   type = PartitionRetentionMode
 *   retentionCondition = "datediff(now(), to_date(elements['dt'], 'yyyyMMdd')) <= 90"
 * }
 * }}}
 * @param retentionCondition Condition to decide if a partition should be kept. Define a spark sql expression
 *                           working with the attributes of [[PartitionExpressionData]] returning a boolean with value true if the partition should be kept.
 */
case class PartitionRetentionMode(retentionCondition: String, description: Option[String] = None) extends HousekeepingMode with SmartDataLakeLogger {
  override private[smartdatalake] def prepare(dataObject: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    assert(dataObject.isInstanceOf[CanHandlePartitions], s"(${dataObject.id}) PartitionRetentionMode only supports DataObject that can handle partitions")
    SparkExpressionUtil.syntaxCheck[PartitionExpressionData,Boolean](dataObject.id, Some("houskeepingMode.retentionCondition"), retentionCondition)
  }
  override private[smartdatalake] def postWrite(dataObject: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    dataObject match {
      case partitionedDataObject: DataObject with CanHandlePartitions if partitionedDataObject.partitions.isEmpty =>
        throw ConfigurationException(s"(${dataObject.id}) PartitionRetentionMode not supported for DataObject without partition columns defined")
      case partitionedDataObject: DataObject with CanHandlePartitions =>
        val pvs = partitionedDataObject.listPartitions
        val pvsEvaluated = SparkExpressionUtil.evaluateSeq[PartitionExpressionData, Boolean](dataObject.id, Some("housekeepingMode.retentionCondition"), retentionCondition, pvs.map(pv => PartitionExpressionData.from(context, dataObject.id, pv)))
        val pvsToDelete = pvsEvaluated
          .filterNot{ case (pvs,keep) => keep.getOrElse(throw ExpressionEvaluationException(s"(${dataObject.id}.housekeepingMode.retentionCondition) expression evaluation should not return 'null' (partitionValue=$pvs"))}
          .map(x => PartitionValues(x._1.elements))
        partitionedDataObject.deletePartitions(pvsToDelete)
        logger.info(s"(${dataObject.id}) Housekeeping cleaned partitions ${pvsToDelete.mkString(", ")}" )
    }
  }
}

/**
 * Archive and compact old partitions:
 * Archive partition reduces the number of partitions in the past by moving older partitions into special "archive partitions".
 * Compact partition reduces the number of files in a partition by rewriting them with Spark.
 * Example: archive and compact a table with partition layout run_id=<integer>
 *  - archive partitions after 1000 partitions into "archive partition" equal to floor(run_id/1000)
 *  - compact "archive partition" when full
 * {{{
 * housekeepingMode = {
 *   type = PartitionArchiveCompactionMode
 *   archivePartitionExpression = "if( elements['run_id'] < runId - 1000, map('run_id', elements['run_id'] div 1000), elements)"
 *   compactPartitionExpression = "elements['run_id'] % 1000 = 0 and elements['run_id'] <= runId - 2000"
 * }
 * }}}
 * @param archivePartitionExpression Expression to define the archive partition for a given partition. Define a spark
 *                                   sql expression working with the attributes of [[PartitionExpressionData]] returning archive
 *                                   partition values as Map[String,String]. If return value is the same as input elements, partition is not touched,
 *                                   otherwise all files of the partition are moved to the returned partition definition.
 *                                   Be aware that the value of the partition columns changes for these files/records.
 * @param compactPartitionExpression Expression to define partitions which should be compacted. Define a spark
 *                                   sql expression working with the attributes of [[PartitionExpressionData]] returning a
 *                                   boolean = true when this partition should be compacted.
 *                                   Once a partition is compacted, it is marked as compacted and will not be compacted again.
 *                                   It is therefore ok to return true for all partitions which should be compacted, regardless if they have been compacted already.
 */
case class PartitionArchiveCompactionMode(archivePartitionExpression: Option[String] = None, compactPartitionExpression: Option[String] = None, description: Option[String] = None) extends HousekeepingMode with SmartDataLakeLogger {
  override private[smartdatalake] def prepare(dataObject: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    assert(dataObject.isInstanceOf[CanHandlePartitions], s"(${dataObject.id}) PartitionRetentionMode only supports DataObject that can handle partitions")
    archivePartitionExpression.foreach(expression => SparkExpressionUtil.syntaxCheck[PartitionExpressionData, Map[String,String]](dataObject.id, Some("housekeepingMode.archivePartitionExpression"), expression))
    compactPartitionExpression.foreach(expression => SparkExpressionUtil.syntaxCheck[PartitionExpressionData, Boolean](dataObject.id, Some("housekeepingMode.compactPartitionExpression"), expression))
  }
  override private[smartdatalake] def postWrite(dataObject: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    dataObject match {
      case partitionedDataObject: DataObject with CanHandlePartitions if partitionedDataObject.partitions.isEmpty =>
        throw ConfigurationException(s"(${dataObject.id}) PartitionArchiveCompactionMode not supported for DataObject without partition columns defined")
      case partitionedDataObject: DataObject with CanHandlePartitions =>
        val pvs = partitionedDataObject.listPartitions
        // evaluate partition to archive
        val pvsToArchiveMapping = archivePartitionExpression.map( expression =>
          SparkExpressionUtil.evaluateSeq[PartitionExpressionData, Map[String,String]](dataObject.id, Some(s"housekeepingMode.archivePartitionExpression"), expression, pvs.map(pv => PartitionExpressionData.from(context, dataObject.id, pv)))
            .map{ case (input, resultPvs) => (input.elements, resultPvs.getOrElse(throw ExpressionEvaluationException(s"(${dataObject.id}) housekeepingMode.archivePartitionExpression result is null for partition value ${input.elements}")))}
            .filter{ case (inputPvs, resultPvs) => inputPvs != resultPvs}
            .map{ case (inputPvs, resultPvs) => (PartitionValues(inputPvs), PartitionValues(resultPvs))}
          ).getOrElse(Seq())
        val pvsToArchive = pvsToArchiveMapping.map(_._1)
        // evaluate partitions to compact
        val pvsToCompact = compactPartitionExpression.map( expression =>
          SparkExpressionUtil.evaluateSeq[PartitionExpressionData, Boolean](dataObject.id, Some("housekeepingMode.compactPartitionExpression"), expression, pvs.map(pv => PartitionExpressionData.from(context, dataObject.id, pv)))
            .map{ case (input, doCompact) => (input.elements, doCompact.getOrElse(throw ExpressionEvaluationException(s"(${dataObject.id}) housekeepingMode.compactPartitionExpression result is null for partition value ${input.elements}")))}
            .filter{ case (inputPvs, doCompact) => doCompact }
            .map{ case (inputPvs, _) => PartitionValues(inputPvs) }
            .filter(pvs => !pvsToArchive.contains(pvs)) // filter out partitions to archive, as they dont need compaction anymore
        ).getOrElse(Seq())
        // archive
        partitionedDataObject.movePartitions(pvsToArchiveMapping)
        logger.info(s"(${dataObject.id}) Housekeeping archived partitions ${pvsToArchive.mkString(", ")}" )
        // compact
        partitionedDataObject.compactPartitions(pvsToCompact)
        logger.info(s"(${dataObject.id}) Housekeeping compacted partitions ${pvsToCompact.mkString(", ")}" )
    }
  }
}

case class PartitionExpressionData(feed: String, application: String, runId: Int, runStartTime: Timestamp, dataObjectId: String, elements: Map[String,String])
private[smartdatalake] object PartitionExpressionData {
  def from(context: ActionPipelineContext, dataObjectId: DataObjectId, partitionValues: PartitionValues): PartitionExpressionData = {
    PartitionExpressionData(context.feed, context.application, context.executionId.runId, Timestamp.valueOf(context.runStartTime), dataObjectId.id, partitionValues.getMapString)
  }
}