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

package io.smartdatalake.metrics

import io.smartdatalake.config.SdlConfigObject

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionMetrics
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.spark.scheduler.{AccumulableInfo, SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

import scala.collection.mutable

/**
 * A parameter object holding the spark metrics for a spark stage.
 *
 * @see [[org.apache.spark.scheduler.StageInfo]] for more details on these metrics.
 */
private[smartdatalake] case class SparkStageMetrics(jobInfo: JobInfo, stageId: Int, stageName: String, numTasks: Int,
                                                    submissionTimestamp: Long, completionTimeStamp: Long,
                                                    executorRuntimeInMillis: Long, executorCpuTimeInNanos: Long,
                                                    executorDeserializeTimeInMillis: Long, executorDeserializeCpuTimeInNanos: Long,
                                                    resultSerializationTimeInMillis: Long, resultSizeInBytes: Long,
                                                    jvmGarbageCollectionTimeInMillis: Long,
                                                    memoryBytesSpilled: Long, diskBytesSpilled: Long,
                                                    peakExecutionMemoryInBytes: Long,
                                                    bytesRead: Long, recordsRead: Long,
                                                    bytesWritten: Long, recordsWritten: Long,
                                                    shuffleFetchWaitTimeInMillis: Long,
                                                    shuffleRemoteBlocksFetched: Long, shuffleLocalBlocksFetched: Long, shuffleTotalBlocksFetched: Long,
                                                    shuffleRemoteBytesRead: Long, shuffleLocalBytesRead: Long, shuffleTotalBytesRead: Long,
                                                    shuffleRecordsRead: Long,
                                                    shuffleWriteTimeInNanos: Long, shuffleBytesWritten: Long,
                                                    shuffleRecordsWritten: Long,
                                                    accumulables: Seq[AccumulableInfo]
                                                   ) extends ActionMetrics {

  lazy val stageSubmissionTime: Instant = Instant.ofEpochMilli(submissionTimestamp)
  lazy val stageCompletionTime: Instant = Instant.ofEpochMilli(completionTimeStamp)
  lazy val stageRuntime: Duration = Duration.ofMillis(completionTimeStamp - submissionTimestamp)
  lazy val aggregatedExecutorRuntime: Duration = Duration.ofMillis(executorRuntimeInMillis)
  lazy val aggregatedExecutorCpuTime: Duration = Duration.ofMillis(executorCpuTimeInNanos / 1000000)
  lazy val aggregatedExecutorGarbageCollectionTime: Duration = Duration.ofMillis(jvmGarbageCollectionTimeInMillis)
  lazy val aggregatedExecutorDeserializeTime: Duration = Duration.ofMillis(executorDeserializeTimeInMillis)
  lazy val aggregatedExecutorDeserializeCpuTime: Duration = Duration.ofMillis(executorDeserializeCpuTimeInNanos / 1000000)
  lazy val resultSerializationTime: Duration = Duration.ofMillis(resultSerializationTimeInMillis)
  lazy val shuffleFetchWaitTime: Duration = Duration.ofMillis(shuffleFetchWaitTimeInMillis)
  lazy val shuffleWriteTime: Duration = Duration.ofMillis(shuffleWriteTimeInNanos / 1000000)

  // formatters
  private lazy val dateTimeFormat = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault)
  private def durationString(valueSeparator: String)(name: String, duration: Duration): String = s"${keyValueString(valueSeparator)(name, duration.toString)}"
  private def keyValueString(valueSeparator: String)(key: String, value: String): String = s"$key$valueSeparator$value"

  /**
   * @return A printable string reporting all metrics.
   */
  override def getAsText: String = {
    val valueSeparator: String = "="
    val durationStringWithSeparator = durationString(valueSeparator)(_, _)
    val keyValueStringWithSeparator = keyValueString(valueSeparator)(_, _)

    s"""job_id=${jobInfo.id} stage_id=$stageId:
       |    ${keyValueStringWithSeparator("job_group", jobInfo.group)}
       |    ${keyValueStringWithSeparator("job_description", jobInfo.description)}
       |    ${keyValueStringWithSeparator("stage_name", stageName)}
       |    ${keyValueStringWithSeparator("num_tasks", numTasks.toString)}
       |    ${keyValueStringWithSeparator("submitted", dateTimeFormat.format(stageSubmissionTime))}
       |    ${keyValueStringWithSeparator("completed", dateTimeFormat.format(stageCompletionTime))}
       |    ${durationStringWithSeparator("runtime", stageRuntime)}
       |    ${durationStringWithSeparator("executor_aggregated_runtime", aggregatedExecutorRuntime)}
       |    ${durationStringWithSeparator("executor_aggregated_cputime", aggregatedExecutorCpuTime)}
       |    ${durationStringWithSeparator("executor_aggregated_deserializetime", aggregatedExecutorDeserializeTime)}
       |    ${durationStringWithSeparator("executor_aggregated_deserializecputime", aggregatedExecutorDeserializeCpuTime)}
       |    ${durationStringWithSeparator("result_serializationtime", resultSerializationTime)}
       |    ${keyValueStringWithSeparator("result_size", resultSizeInBytes.toString)} B
       |    ${durationStringWithSeparator("jvm_aggregated_gc_time", aggregatedExecutorGarbageCollectionTime)}
       |    ${keyValueStringWithSeparator("memory_spilled", memoryBytesSpilled.toString)} B
       |    ${keyValueStringWithSeparator("disk_spilled", diskBytesSpilled.toString)} B
       |    ${keyValueStringWithSeparator("peak_execution_memory", peakExecutionMemoryInBytes.toString)} B
       |    ${keyValueStringWithSeparator("bytes_read", bytesRead.toString)} B
       |    ${keyValueStringWithSeparator("bytes_written", bytesWritten.toString)} B
       |    ${keyValueStringWithSeparator("records_read", recordsRead.toString)}
       |    ${keyValueStringWithSeparator("records_written", recordsWritten.toString)}
       |    ${durationStringWithSeparator("shuffle_fetch_waittime", shuffleFetchWaitTime)}
       |    ${keyValueStringWithSeparator("shuffle_remote_blocks_fetched", shuffleRemoteBlocksFetched.toString)}
       |    ${keyValueStringWithSeparator("shuffle_local_blocks_fetched", shuffleLocalBlocksFetched.toString)}
       |    ${keyValueStringWithSeparator("shuffle_total_blocks_fetched", shuffleTotalBlocksFetched.toString)}
       |    ${keyValueStringWithSeparator("shuffle_remote_bytes_read", shuffleRemoteBytesRead.toString)} B
       |    ${keyValueStringWithSeparator("shuffle_local_bytes_read", shuffleLocalBytesRead.toString)}  B
       |    ${keyValueStringWithSeparator("shuffle_total_bytes_read", shuffleTotalBytesRead.toString )} B
       |    ${keyValueStringWithSeparator("shuffle_records_read", shuffleRecordsRead.toString)}
       |    ${durationStringWithSeparator("shuffle_write_time", shuffleWriteTime)}
       |    ${keyValueStringWithSeparator("shuffle_bytes_written", shuffleBytesWritten.toString)} B
       |    ${keyValueStringWithSeparator("shuffle_records_written", shuffleRecordsWritten.toString)}""".stripMargin
  }

  def getId: String = jobInfo.toString
  def getOrder: Long = stageId
  def getMainInfos: Map[String, Any] = {
    Map("stage_duration" -> stageRuntime, "records_written" -> recordsWritten, "bytes_written" -> bytesWritten, "num_tasks" -> numTasks.toLong, "stage" -> stageName.split(' ').head )
  }
}
private[smartdatalake] case class JobInfo(id: Int, group: String, description: String, executionId: Option[SDLExecutionId]) {
  val dataObjectIdRegex = (s"DataObject~(${SdlConfigObject.idRegexStr})").r.unanchored
  val dataObjectId = description match {
    case dataObjectIdRegex(id) => Some(DataObjectId(id))
    case _ => None // there are some stages which are created by Spark DataFrame operations which dont manipulate Actions target DataObject's, e.g. pivot operator
  }
}