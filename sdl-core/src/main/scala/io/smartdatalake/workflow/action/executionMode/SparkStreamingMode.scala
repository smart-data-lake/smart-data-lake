/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow.action.executionMode.ProcessAllMode.extract
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

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
case class SparkStreamingMode(checkpointLocation: String, triggerType: String = "Once", triggerTime: Option[String] = None, inputOptions: Map[String, String] = Map(), outputOptions: Map[String, String] = Map(), outputMode: OutputMode = OutputMode.Append) extends ExecutionMode {
  // parse trigger from config attributes
  private[smartdatalake] val trigger = triggerType.toLowerCase match {
    case "once" =>
      assert(triggerTime.isEmpty, "triggerTime must not be set for SparkStreamingMode with triggerType=Once")
      Trigger.Once()
    case "processingtime" =>
      assert(triggerTime.isDefined, "triggerTime must be set for SparkStreamingMode with triggerType=ProcessingTime")
      Trigger.ProcessingTime(triggerTime.get)
    case "continuous" =>
      assert(triggerTime.isDefined, "triggerTime must be set for SparkStreamingMode with triggerType=Continuous")
      Trigger.Continuous(triggerTime.get)
  }

  override private[smartdatalake] def isAsynchronous = trigger != Trigger.Once

  override def factory: FromConfigFactory[ExecutionMode] = SparkStreamingMode
}

object SparkStreamingMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkStreamingMode = {
    extract[SparkStreamingMode](config)
  }
}
