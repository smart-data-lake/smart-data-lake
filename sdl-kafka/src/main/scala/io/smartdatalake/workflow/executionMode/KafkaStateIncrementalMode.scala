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

package io.smartdatalake.workflow.executionMode

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.action.executionMode.{ExecutionMode, ExecutionModeResult}
import io.smartdatalake.workflow.dataobject.{DataObject, KafkaTopicDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataObjectState, SubFeed}

import java.sql.Timestamp

/**
 * A special incremental execution mode for Kafka Inputs, remembering the state from the last increment through the Kafka Consumer, e.g. committed offsets.
 * @param delayedMaxTimestampExpr Optional expression to define a delay to read latest offsets from Kafka. The expression has to return a timestamp which is used to select ending offsets to read from Kafka.
 *                                Define a spark sql expression working with the attributes of [[DefaultExpressionData]] returning a timestamp.
 *                                Default is to read latest offsets existing in Kafka.
 */
case class KafkaStateIncrementalMode(delayedMaxTimestampExpr: Option[String] = None) extends ExecutionMode {
  private var kafkaInputs: Seq[KafkaTopicDataObject] = Seq()

  override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    // validate delayedMaxTimestampExpr expression
    delayedMaxTimestampExpr.foreach(expression => SparkExpressionUtil.syntaxCheck[DefaultExpressionData, Timestamp](actionId, Some("delayedMaxTimestampExpr"), expression))
  }

  override def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = {
    // check that there is at least one kafka input DataObject
    kafkaInputs = subFeeds.map(s => context.instanceRegistry.get[DataObject](s.dataObjectId))
      .collect { case input: KafkaTopicDataObject => input }
    assert(kafkaInputs.nonEmpty, s"KafkaStateIncrementalMode needs at least one KafkaTopicDataObject as input")
  }

  override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed, partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])(implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    // prepare options
    val delayedMaxTimestamp = delayedMaxTimestampExpr.flatMap(expr => SparkExpressionUtil.evaluate[DefaultExpressionData, Timestamp](actionId, Some("delayedMaxTimestampExpr"), expr, DefaultExpressionData.from(context, Seq())))
    val options = Map(
      KafkaTopicDataObject.delayedMaxTimestampOption -> delayedMaxTimestamp.map(_.toString)
    ).collect { case (key, Some(value)) => key -> value } // remove keys with empty values
    // enable kafka incremental mode
    kafkaInputs.foreach(_.enableKafkaStateIncrementalMode(delayedMaxTimestamp))
    // return execution mode
    Some(ExecutionModeResult(options = options))
  }
  override def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    // commit offsets read to Kafka
    kafkaInputs.foreach(_.commitIncrementalOutputState)
  }

  override def factory: FromConfigFactory[ExecutionMode] = KafkaStateIncrementalMode
}

object KafkaStateIncrementalMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): KafkaStateIncrementalMode = {
    extract[KafkaStateIncrementalMode](config)
  }
}