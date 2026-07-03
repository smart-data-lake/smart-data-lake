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
package io.smartdatalake.workflow.action

import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ExcludeFromSchemaExport, FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.agent.Agent
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase, SubFeed}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.Type

/**
 * This[[Action]] executes the action defined by [[wrappedAction]] on a remote agent defined by [[agent]].
 * If the execution of [[wrappedAction]] is successful, the ProxyAction will return an empty [[SparkSubFeed]] with the schema of the action's result.
 * @param wrappedAction: the action to execute on the agent
 * @param agent: the agent on which the action should be executed
 */
case class ProxyAction(wrappedAction: Action, override val id: SdlConfigObject.ActionId, agent: Agent)
                      (implicit val instanceRegistry: InstanceRegistry) extends Action with ExcludeFromSchemaExport {
  assert(wrappedAction.isInstanceOf[DataFrameActionImpl], "Only Actions handling DataFrames supported by ProxyAction for now")
  private val dataFrameAction = wrappedAction.asInstanceOf[DataFrameActionImpl]

  override def factory: FromConfigFactory[Action] = wrappedAction.factory

  override def nodeId: NodeId = wrappedAction.nodeId

  override def metadata: Option[ActionMetadata] = wrappedAction.metadata

  override def inputs: Seq[DataObject] = wrappedAction.inputs

  override def outputs: Seq[DataObject] = wrappedAction.outputs

  override def executionCondition: Option[Condition] = wrappedAction.executionCondition

  override def executionMode: Option[ExecutionMode] = wrappedAction.executionMode

  override def metricsFailCondition: Option[String] = wrappedAction.metricsFailCondition

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    runOnAgent(ExecutionPhase.Prepare)
  }

  override def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = {
    runOnAgent(ExecutionPhase.Init)
  }

  override def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = {
    runOnAgent(ExecutionPhase.Exec)
  }

  def runOnAgent(executionPhase: ExecutionPhase)(implicit context: ActionPipelineContext): Seq[SubFeed] = {

    val agentClient = agent.getClient

    val hoconInstructions = AgentClient.prepareHoconInstructions(wrappedAction, context.instanceRegistry.getConnections, agent, executionPhase)
    val response = agentClient.sendSDLMessage(hoconInstructions)

    // throw exception if execution on agent failed
    response.errorMsg.foreach(e => throw RemoteAgentException(e))

    // if succeeded, create subfeeds with empty dataframes but correct schema
    response.dataObjectIdToSchema.map {
      case (dataObjectId: DataObjectId, schema: String) => convertToEmptySubFeed(dataObjectId, schema, dataFrameAction.subFeedType)
    }.toSeq
  }

  def convertToEmptySubFeed(dataObjectId: DataObjectId, schemaDdl: String, subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val companion = DataFrameSubFeed.getCompanion(subFeedType)
    // TODO: generic create schema from string without using StructType.fromDDL
    val schema = SparkSchema(StructType.fromDDL(schemaDdl))
    val emptyDF = companion.getEmptyDataFrame(schema, dataObjectId)
    companion.getSubFeed(dataFrame = emptyDF, dataObjectId = dataObjectId, Seq())
      .asDummy()
  }
}

case class RemoteAgentException(msg: String) extends Exception(msg)