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
package io.smartdatalake.workflow.action.executionMode

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.spark.{DummyStreamProvider, SparkStreamingMetrics, SparkStreamingQueryListener}
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.DataFrameActionImpl
import io.smartdatalake.workflow.action.executionMode.ProcessAllMode.extract
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.dataobject.spark.{CanCreateStreamingDataFrame, CanWriteSparkDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
 * Spark streaming execution mode uses Spark Structured Streaming to incrementally execute data loads and keep track of processed data.
 * This mode needs a DataObject implementing CanCreateStreamingDataFrame and works only with SparkSubFeeds.
 * This mode can be executed synchronously in the DAG by using triggerType=Once, or asynchronously as Streaming Query with triggerType = ProcessingTime or Continuous.
 *
 * @param checkpointLocation location for checkpoints of streaming query to keep state
 * @param triggerType        define execution interval of Spark streaming query. Possible values are Once (default), ProcessingTime & Continuous. See [[Trigger]] for details.
 *                           Note that this is only applied if SDL is executed in streaming mode. If SDL is executed in normal mode, TriggerType=Once is used always.
 *                           If triggerType=Once, the action is repeated with Trigger.Once in SDL streaming mode.
 * @param triggerTime        Time as String in triggerType = ProcessingTime or Continuous. See [[Trigger]] for details.
 * @param inputOptions       additional option to apply when reading streaming source. This overwrites options set by the DataObjects.
 * @param outputOptions      additional option to apply when writing to streaming sink. This overwrites options set by the DataObjects.
 */
case class SparkStreamingMode(checkpointLocation: String, triggerType: String = "Once", triggerTime: Option[String] = None, inputOptions: Map[String, String] = Map(), outputOptions: Map[String, String] = Map(), outputMode: OutputMode = OutputMode.Append) extends DataFrameStreamingExecutionMode {
  // parse trigger from config attributes
  private[smartdatalake] val trigger = triggerType.toLowerCase match {
    case "once" =>
      assert(triggerTime.isEmpty, "triggerTime must not be set for SparkStreamingMode with triggerType=Once")
      Trigger.AvailableNow()
    case "processingtime" =>
      assert(triggerTime.isDefined, "triggerTime must be set for SparkStreamingMode with triggerType=ProcessingTime")
      Trigger.ProcessingTime(triggerTime.get)
    case "continuous" =>
      assert(triggerTime.isDefined, "triggerTime must be set for SparkStreamingMode with triggerType=Continuous")
      Trigger.Continuous(triggerTime.get)
  }

  override def isAsynchronous: Boolean = trigger != Trigger.AvailableNow()

  // Streaming query state (one per action instance; SparkStreamingMode is instantiated per action config)
  @volatile private var _streamingQuery: Option[StreamingQuery] = None
  override def isStreamingStarted: Boolean = _streamingQuery.nonEmpty
  override def notifyStreamingQueryTerminated(): Unit = { _streamingQuery = None }
  override def resetStreamingState(): Unit = { _streamingQuery = None }

  override def enrichSubFeedForStreamingInput(
    input: DataObject with CanCreateDataFrame,
    subFeed: DataFrameSubFeed,
    phase: ExecutionPhase,
    refreshDataFrame: Boolean
  )(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val sparkSubFeed = subFeed.asInstanceOf[SparkSubFeed]
    implicit val sparkSession: SparkSession = SparkSubFeed.getSparkSession(context)
    if (refreshDataFrame) {
      assert(input.isInstanceOf[CanCreateStreamingDataFrame],
        s"DataObject ${input.id} doesn't implement CanCreateStreamingDataFrame. Cannot create StreamingDataFrame for SparkStreamingMode")
      val df = input.asInstanceOf[CanCreateStreamingDataFrame].getStreamingDataFrame(inputOptions, sparkSubFeed.dataFrame.map(_.schema.inner))
      sparkSubFeed.copy(dataFrame = Some(SparkDataFrame(df)), partitionValues = Seq()) // remove partition values for streaming
    } else if (sparkSubFeed.isStreaming.contains(false)) {
      // convert to dummy streaming DataFrame
      val emptyStreamingDataFrame = sparkSubFeed.dataFrame.map(df => DummyStreamProvider.getDummyDf(df.schema.inner))
      sparkSubFeed.copy(dataFrame = emptyStreamingDataFrame.map(SparkDataFrame), partitionValues = Seq())
    } else sparkSubFeed
  }

  override def writeSubFeedStreaming(
    action: DataFrameActionImpl,
    subFeed: DataFrameSubFeed,
    output: DataObject with CanWriteDataFrame,
    queryName: String
  )(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val sparkOutput = output.asInstanceOf[CanWriteSparkDataFrame]
    if (isAsynchronous && context.appConfig.streaming) {
      // Asynchronous: start streaming query once, then return immediately on subsequent DAG runs
      if (_streamingQuery.isEmpty) {
        val queryListener = new SparkStreamingQueryListener(action, output.id, queryName)
        val streamingQuery = sparkOutput.writeStreamingDataFrame(subFeed.dataFrame.get, trigger, outputOptions, checkpointLocation, queryName, outputMode, action.saveModeOptions)
        queryListener.waitForFirstProgress()
        streamingQuery.exception.foreach(throw _)
        val streamingMetrics = SparkStreamingMetrics(streamingQuery.lastProgress)
        if (streamingMetrics.noData) logger.info(s"(${action.id}) no data to process for ${output.id} in first micro-batch streaming mode")
        _streamingQuery = Some(streamingQuery)
        val runtimeMetrics = action.runtimeData.getMetrics(output.id).map(_.getMainInfos).getOrElse(Map())
        subFeed.withMetrics(streamingMetrics.getMainInfos ++ runtimeMetrics).asInstanceOf[DataFrameSubFeed]
      } else {
        logger.debug(s"(${action.id}) streaming query already started")
        subFeed
      }
    } else {
      // Synchronous: run with Trigger.AvailableNow and wait for completion
      val queryListener = new SparkStreamingQueryListener(action, output.id, queryName)
      val streamingQuery = sparkOutput.writeStreamingDataFrame(subFeed.dataFrame.get, Trigger.AvailableNow(), outputOptions, checkpointLocation, queryName, outputMode, action.saveModeOptions)
      streamingQuery.awaitTermination()
      queryListener.waitForFirstProgress()
      val streamingMetrics = SparkStreamingMetrics(streamingQuery.lastProgress)
      if (streamingMetrics.noData) logger.info(s"(${action.id}) no data to process for ${output.id} in streaming mode")
      val runtimeMetrics = action.runtimeData.getMetrics(output.id).map(_.getMainInfos).getOrElse(Map())
      subFeed.withMetrics(streamingMetrics.getMainInfos ++ runtimeMetrics).asInstanceOf[DataFrameSubFeed]
    }
  }

  override def factory: FromConfigFactory[ExecutionMode] = SparkStreamingMode
}

object SparkStreamingMode extends FromConfigFactory[ExecutionMode] {
  import configs.{ConfigReader, Result}
  implicit val outputModeReader: ConfigReader[OutputMode] = {
    ConfigReader.fromConfig(_.toString.toLowerCase match {
      case "append" => Result.successful(OutputMode.Append())
      case "complete" => Result.successful(OutputMode.Complete())
      case "update" => Result.successful(OutputMode.Update())
      case x => Result.failure(configs.ConfigError(s"$x is not a value of OutputMode. Supported values are append, complete, update."))
    })
  }
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkStreamingMode = {
    extract[SparkStreamingMode](config)
  }
}
