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
package io.smartdatalake.workflow.dataobject.generic

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config._
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ExpressionEvaluationException, ExpressionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.DataObject

import java.sql.Timestamp

trait HousekeepingMode extends ParsableFromConfig[HousekeepingMode] with ConfigHolder {
  def prepare(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit
  def postWrite(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit
}

/**
 * Keep partitions while retention condition is fulfilled, delete other partitions.
 *
 * The condition is evaluated after every write against all existing partitions of the DataObject, and every
 * partition for which it returns false is deleted. Use it to implement a rolling time window on a partitioned
 * DataObject. Use [[PartitionArchiveMode]] instead if old data should be kept but consolidated into fewer
 * partitions.
 *
 * Example: cleanup partitions with partition layout dt=<yyyymmdd> after 90 days:
 * {{{
 * dataObjects = {
 *   int-departures {
 *     type = ParquetFileDataObject
 *     path = "~{env.basedir}/int_departures"
 *     partitions = [dt]
 *     housekeepingMode = {
 *       type = PartitionRetentionMode
 *       retentionCondition = "datediff(now(), to_date(elements['dt'], 'yyyyMMdd')) <= 90"
 *     }
 *   }
 * }
 * }}}
 *
 * @note Only supported for DataObjects implementing `CanHandlePartitions`; this is asserted in the prepare phase.
 *       If such a DataObject has no partition column defined, a ConfigurationException is thrown when housekeeping
 *       runs after the write.
 * @see [[PartitionArchiveMode]]
 * @param retentionCondition
 *   Condition to decide if a partition should be kept. Define a spark sql expression working with
 *   the attributes of [[PartitionExpressionData]] returning a boolean with value true if the
 *   partition should be kept.
 * @param description
 *   Optional description of this housekeeping mode, e.g. to document the business rule behind the
 *   retention condition. It is not interpreted by SDLB.
 */
case class PartitionRetentionMode(retentionCondition: String, description: Option[String] = None) extends HousekeepingMode
    with SmartDataLakeLogger {
  override def prepare(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit = {
    assert(dataObject.isInstanceOf[CanHandlePartitions],
      s"(${dataObject.id}) PartitionRetentionMode only supports DataObject that can handle partitions")
    ExpressionUtil.syntaxCheck[PartitionExpressionData, Boolean](dataObject.id, Some("houskeepingMode.retentionCondition"),
      retentionCondition)
  }
  override def postWrite(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit =
    dataObject match {
      case partitionedDataObject: DataObject with CanHandlePartitions if partitionedDataObject.partitions.isEmpty =>
        throw ConfigurationException(
          s"(${dataObject.id}) PartitionRetentionMode not supported for DataObject without partition columns defined"
        )
      case partitionedDataObject: DataObject with CanHandlePartitions =>
        val pvs = partitionedDataObject.listPartitions
        val pvsEvaluated =
          ExpressionUtil.evaluateSeq[PartitionExpressionData, Boolean](dataObject.id, Some("housekeepingMode.retentionCondition"),
            retentionCondition, pvs.map(pv => PartitionExpressionData.from(context, dataObject.id, pv)))
        val pvsToDelete = pvsEvaluated
          .filterNot { case (pvs, keep) =>
            keep.getOrElse(throw ExpressionEvaluationException(
                s"(${dataObject.id}.housekeepingMode.retentionCondition) expression evaluation should not return 'null' (partitionValue=$pvs"
              ))
          }
          .map(x => PartitionValues(x._1.elements))
        partitionedDataObject.deletePartitions(pvsToDelete)
        logger.info(s"(${dataObject.id}) Housekeeping cleaned partitions ${pvsToDelete.mkString(", ")}")
    }
}

object PartitionRetentionMode extends FromConfigFactory[HousekeepingMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PartitionRetentionMode =
    extract[PartitionRetentionMode](config)
}

/**
 * Archive old partitions: Archive partition reduces the number of partitions in the past by moving
 * older partitions into special "archive partitions".
 *
 * Use it to avoid an ever growing number of small partitions for a DataObject that is loaded frequently, while still
 * keeping the historical data. In contrast to [[PartitionRetentionMode]] no data is deleted, but the partition column
 * values of the archived records change to the archive partition value. Note that files are relocated as they are and
 * not merged, so the number of files stays the same.
 *
 * Example: archive a table with partition layout run_id=<integer>, moving partitions older than 1000 runs into an
 * "archive partition" equal to floor(run_id/1000):
 * {{{
 * dataObjects = {
 *   int-departures {
 *     type = ParquetFileDataObject
 *     path = "~{env.basedir}/int_departures"
 *     partitions = [run_id]
 *     housekeepingMode = {
 *       type = PartitionArchiveMode
 *       archivePartitionExpression = "if( elements['run_id'] < runId - 1000, map('run_id', elements['run_id'] div 1000), elements)"
 *     }
 *   }
 * }
 * }}}
 *
 * @note Only supported for DataObjects implementing `CanHandlePartitions`; this is asserted in the prepare phase.
 *       If such a DataObject has no partition column defined, a ConfigurationException is thrown when housekeeping
 *       runs after the write.
 * @see [[PartitionRetentionMode]]
 * @param archivePartitionExpression
 *   Expression to define the archive partition for a given partition. Define a spark sql expression
 *   working with the attributes of [[PartitionExpressionData]] returning archive partition values
 *   as Map[String,String]. If return value is the same as input elements, partition is not touched,
 *   otherwise all files of the partition are moved to the returned partition definition. Be aware
 *   that the value of the partition columns changes for these files/records.
 *   If not defined, no partition is archived.
 * @param description
 *   Optional description of this housekeeping mode, e.g. to document the archiving strategy.
 *   It is not interpreted by SDLB.
 */
case class PartitionArchiveMode(
    archivePartitionExpression: Option[String] = None,
    description: Option[String] = None
) extends HousekeepingMode with SmartDataLakeLogger {
  override def prepare(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit = {
    assert(dataObject.isInstanceOf[CanHandlePartitions],
      s"(${dataObject.id}) PartitionRetentionMode only supports DataObject that can handle partitions")
    archivePartitionExpression.foreach(expression =>
      ExpressionUtil.syntaxCheck[PartitionExpressionData, Map[String, String]](dataObject.id,
        Some("housekeepingMode.archivePartitionExpression"), expression)
    )
  }
  override def postWrite(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit =
    dataObject match {
      case partitionedDataObject: DataObject with CanHandlePartitions if partitionedDataObject.partitions.isEmpty =>
        throw ConfigurationException(
          s"(${dataObject.id}) PartitionArchiveCompactionMode not supported for DataObject without partition columns defined"
        )
      case partitionedDataObject: DataObject with CanHandlePartitions =>
        val pvs = partitionedDataObject.listPartitions
        // evaluate partition to archive
        val pvsToArchiveMapping = archivePartitionExpression.map(expression =>
          ExpressionUtil.evaluateSeq[PartitionExpressionData, Map[String, String]](dataObject.id,
            Some(s"housekeepingMode.archivePartitionExpression"), expression,
            pvs.map(pv => PartitionExpressionData.from(context, dataObject.id, pv)))
            .map { case (input, resultPvs) =>
              (input.elements,
                resultPvs.getOrElse(throw ExpressionEvaluationException(
                    s"(${dataObject.id}) housekeepingMode.archivePartitionExpression result is null for partition value ${input.elements}"
                  )))
            }
            .filter { case (inputPvs, resultPvs) => inputPvs != resultPvs }
            .map { case (inputPvs, resultPvs) => (PartitionValues(inputPvs), PartitionValues(resultPvs)) }
        ).getOrElse(Seq())
        val pvsToArchive = pvsToArchiveMapping.map(_._1)
        // archive
        if (pvsToArchiveMapping.nonEmpty) {
          partitionedDataObject.movePartitions(pvsToArchiveMapping)
          logger.info(s"(${dataObject.id}) Housekeeping archived partitions ${pvsToArchive.mkString(", ")}")
        }
    }
}

object PartitionArchiveMode extends FromConfigFactory[HousekeepingMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PartitionArchiveMode =
    extract[PartitionArchiveMode](config)
}

case class PartitionExpressionData(
    feed: String,
    application: String,
    runId: Int,
    runStartTime: Timestamp,
    dataObjectId: String,
    elements: Map[String, String]
)
object PartitionExpressionData {
  def from(context: ActionPipelineContext, dataObjectId: DataObjectId, partitionValues: PartitionValues): PartitionExpressionData =
    PartitionExpressionData(context.feed, context.application, context.executionId.runId, Timestamp.valueOf(context.runStartTime),
      dataObjectId.id, partitionValues.getMapString)
}
