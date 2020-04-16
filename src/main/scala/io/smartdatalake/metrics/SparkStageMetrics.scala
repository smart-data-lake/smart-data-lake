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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, PeriodFormatter, PeriodFormatterBuilder}
import org.joda.time.{Duration, Instant}

import scala.collection.mutable

/**
 * A parameter object holding the spark metrics for a spark stage.
 *
 * @see [[org.apache.spark.scheduler.StageInfo]] for more details on these metrics.
 */
private[smartdatalake] case class SparkStageMetrics(jobId: Int, jobDescription: String, stageId: Int, stageName: String,
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
                             shuffleRecordsWritten: Long
                            ) {

  lazy val stageSubmissionTime: Instant  = new Instant(submissionTimestamp)
  lazy val stageCompletionTime: Instant  = new Instant(completionTimeStamp)
  lazy val stageRuntime: Duration = Duration.millis(completionTimeStamp - submissionTimestamp)
  lazy val aggregatedExecutorRuntime: Duration = Duration.millis(executorRuntimeInMillis)
  lazy val aggregatedExecutorCpuTime: Duration = Duration.millis(executorCpuTimeInNanos / 1000000)
  lazy val aggregatedExecutorGarbageCollectionTime: Duration = Duration.millis(jvmGarbageCollectionTimeInMillis)
  lazy val aggregatedExecutorDeserializeTime: Duration = Duration.millis(executorDeserializeTimeInMillis)
  lazy val aggregatedExecutorDeserializeCpuTime: Duration = Duration.millis(executorDeserializeCpuTimeInNanos / 1000000)
  lazy val resultSerializationTime: Duration = Duration.millis(resultSerializationTimeInMillis)
  lazy val shuffleFetchWaitTime: Duration = Duration.millis(shuffleFetchWaitTimeInMillis)
  lazy val shuffleWriteTime: Duration = Duration.millis(shuffleWriteTimeInNanos / 1000000)

  /**
   * Formats [[Duration]]s as human readable strings.
   */
  lazy val durationFormatter: PeriodFormatter = new PeriodFormatterBuilder()
    .appendDays().appendSuffix(" d").appendSeparator(" ")
    .appendHours().appendSuffix(" hr").appendSeparator( " ")
    .minimumPrintedDigits(2)
    .appendMinutes().appendSuffix(" min").appendSeparator(" ")
    .appendSecondsWithMillis().appendSuffix(" s").toFormatter

  /**
   * Formats [[org.joda.time.DateTime]]s as human readable strings.
   */
  lazy val dateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZ")


  private def durationString(valueSeparator: String, formatter: PeriodFormatter)(name: String, duration: Duration): String = {
    s"${keyValueString(valueSeparator)(name, duration.getMillis.toString)} ms (${formatter.print(duration.toPeriod)})"
  }

  private def keyValueString(valueSeparator: String)(key: String, value: String): String = s"$key$valueSeparator$value"

  /**
   * @return A printable string reporting all metrics.
   */
  def report(valueSeparator: String = "="): String = {
    val durationStringWithSeparator = durationString(valueSeparator, durationFormatter)(_, _)
    val keyValueStringWithSeparator = keyValueString(valueSeparator)(_, _)

    s"""job_id=$jobId stage_id=$stageId:
       |    ${keyValueStringWithSeparator("job_description", jobDescription)}
       |    ${keyValueStringWithSeparator("stage_name", stageName)}
       |    ${keyValueStringWithSeparator("submitted", {stageSubmissionTime.toDateTime.toString(dateTimeFormat)})}
       |    ${keyValueStringWithSeparator("completed", {stageCompletionTime.toDateTime.toString(dateTimeFormat)})}
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
       |    ${keyValueStringWithSeparator("schuffle_remote_blocks_fetched", shuffleRemoteBlocksFetched.toString)}
       |    ${keyValueStringWithSeparator("schuffle_local_blocks_fetched", shuffleLocalBlocksFetched.toString)}
       |    ${keyValueStringWithSeparator("schuffle_total_blocks_fetched", shuffleTotalBlocksFetched.toString)}
       |    ${keyValueStringWithSeparator("schuffle_remote_bytes_read", shuffleRemoteBytesRead.toString)} B
       |    ${keyValueStringWithSeparator("schuffle_local_bytes_read", shuffleLocalBytesRead.toString)}  B
       |    ${keyValueStringWithSeparator("schuffle_total_bytes_read", shuffleTotalBytesRead.toString )} B
       |    ${keyValueStringWithSeparator("schuffle_records_read", shuffleRecordsRead.toString)}
       |    ${durationStringWithSeparator("shuffle_write_time", shuffleWriteTime)}
       |    ${keyValueStringWithSeparator("schuffle_bytes_written", shuffleBytesWritten.toString)} B
       |    ${keyValueStringWithSeparator("schuffle_records_written", shuffleRecordsWritten.toString)}""".stripMargin
  }
}

/**
 * Collects spark metrics for spark stages.
 */
private[smartdatalake] class StageMetricsListener extends SparkListener with SmartDataLakeLogger {

  /**
   * Stores jobID and jobDescription indexed by stage ids.
   */
  val jobInfoLookupTable: mutable.Map[Int, (Int, String)] = mutable.Map.empty

  /**
   * Holds the metrics for all completed stages.
   */
  val stageMetricsCollection: mutable.ListBuffer[SparkStageMetrics] = mutable.ListBuffer.empty

  /**
   * On job start, register the job ids and stage ids.
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach { stageId =>
      jobInfoLookupTable(stageId) = (jobStart.jobId, jobStart.properties.getProperty("spark.job.description"))
    }
  }

  /**
   * On stage completion store the spark metrics
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val taskMetrics = stageCompleted.stageInfo.taskMetrics
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
    val (jobId, jobDescription) = jobInfoLookupTable(stageId)

    stageMetricsCollection += SparkStageMetrics(jobId, jobDescription, stageId, stageCompleted.stageInfo.name,
      stageCompleted.stageInfo.submissionTime.getOrElse(-1L), stageCompleted.stageInfo.completionTime.getOrElse(-1L),
      taskMetrics.executorRunTime, taskMetrics.executorCpuTime, taskMetrics.executorDeserializeTime, taskMetrics.executorDeserializeCpuTime,
      taskMetrics.resultSerializationTime, taskMetrics.resultSize,
      taskMetrics.jvmGCTime,
      taskMetrics.memoryBytesSpilled, taskMetrics.diskBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.bytesRead, taskMetrics.inputMetrics.recordsRead,
      taskMetrics.outputMetrics.bytesWritten, taskMetrics.outputMetrics.recordsWritten,
      shuffleReadMetrics.fetchWaitTime,
      shuffleReadMetrics.remoteBlocksFetched, shuffleReadMetrics.localBlocksFetched, shuffleReadMetrics.totalBlocksFetched,
      shuffleReadMetrics.remoteBytesRead, shuffleReadMetrics.localBytesRead, shuffleReadMetrics.totalBytesRead,
      shuffleReadMetrics.recordsRead,
      taskMetrics.shuffleWriteMetrics.writeTime, taskMetrics.shuffleWriteMetrics.bytesWritten, taskMetrics.shuffleWriteMetrics.recordsWritten
    )
  }
}
