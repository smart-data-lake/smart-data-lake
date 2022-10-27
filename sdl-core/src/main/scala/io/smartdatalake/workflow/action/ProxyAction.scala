/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action

import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, SdlConfigObject}
import io.smartdatalake.definitions.{Condition, ExecutionMode}
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.agent.Agent
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SubFeed}
import org.apache.spark.sql.types.StructType

case class ProxyAction(wrappedAction: Action, override val id: SdlConfigObject.ActionId, agent: Agent) extends Action {

  override def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = {
    common(subFeeds, ExecutionPhase.Exec)
  }

  override def factory: FromConfigFactory[Action] = wrappedAction.factory

  override def nodeId: NodeId = wrappedAction.nodeId

  override def atlasName: String = wrappedAction.atlasName

  override def metadata: Option[ActionMetadata] = wrappedAction.metadata

  override def inputs: Seq[DataObject] = wrappedAction.inputs

  override def outputs: Seq[DataObject] = wrappedAction.outputs

  override def executionCondition: Option[Condition] = wrappedAction.executionCondition

  override def executionMode: Option[ExecutionMode] = wrappedAction.executionMode

  override def metricsFailCondition: Option[String] = wrappedAction.metricsFailCondition

  override def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = {
    common(subFeeds, ExecutionPhase.Init)
  }

  def common(subFeeds: Seq[SubFeed], executionPhase: ExecutionPhase)(implicit context: ActionPipelineContext): Seq[SubFeed] = {
    val agentClient = AgentClient(agent)
    val hoconInstructions = AgentClient.prepareHoconInstructions(wrappedAction, context.instanceRegistry.getConnections, agent, executionPhase)
    agentClient.sendSDLMessage(hoconInstructions)

    val instructionId = hoconInstructions.agentInstruction.get.instructionId

    while (!agentClient.socket.pendingResults.contains(instructionId)) {
      Thread.sleep(1000)
      println("waiting...")
    }
    val response = agentClient.socket.pendingResults.get(instructionId)
    agentClient.socket.pendingResults.remove(instructionId)

    val outputDO = response.get.agentResult.get.dataObjectIdToSchema.head

    val requiredType = StructType.fromDDL(outputDO._2)

    val emptyDF = DataFrameUtil.getEmptyDataFrame(requiredType)(context.sparkSession)
    val subFeed = SparkSubFeed(dataFrame = Some(SparkDataFrame(emptyDF)), dataObjectId = DataObjectId(outputDO._1), partitionValues = Nil,
      isDummy = true, filter = None)
    logger.info(response.toString)
    Seq(subFeed)
  }
}
