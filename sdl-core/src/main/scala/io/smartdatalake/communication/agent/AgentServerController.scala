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

package io.smartdatalake.communication.agent

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilder, SmartDataLakeBuilderConfig, SmartDataLakeBuilderConfigTrait}
import io.smartdatalake.communication.message.{AgentResult, SDLMessage, SDLMessageType}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.DataObject

import java.time.LocalDateTime

case class AgentServerController(
                            instanceRegistry: InstanceRegistry,
                            sdlb: SmartDataLakeBuilder
                          ) {
  def handle[SmartDataLakeBuilderConfigImpl <: SmartDataLakeBuilderConfigTrait[_]](message: SDLMessage, agentServerSDLBConfig: SmartDataLakeBuilderConfigImpl): Option[SDLMessage] = message match {
    case SDLMessage(SDLMessageType.EndConnection, _ ,_,_,_,_) => None
    case SDLMessage(SDLMessageType.AgentInstruction, None, None, None, agentInstructionOpt, None) => agentInstructionOpt match {
      case Some(agentInstruction) =>
        implicit val instanceRegistryImplicit: InstanceRegistry = instanceRegistry
        val configFromString = ConfigFactory.parseString(agentInstruction.hoconConfig, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

        val connectionsToRegister: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
          .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

        instanceRegistryImplicit.register(connectionsToRegister)

        val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
          .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }
        instanceRegistryImplicit.register(dataObjects)

        val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
          .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

        instanceRegistryImplicit.register(actions)

        val sdlConfig = SmartDataLakeBuilderConfig(agentServerSDLBConfig.feedSel,applicationName = agentServerSDLBConfig.applicationName, configuration = agentServerSDLBConfig.configuration, master = agentServerSDLBConfig.master,
          deployMode = agentServerSDLBConfig.deployMode, username = agentServerSDLBConfig.username, kerberosDomain = agentServerSDLBConfig.kerberosDomain,
          keytabPath = agentServerSDLBConfig.keytabPath, partitionValues = agentServerSDLBConfig.partitionValues, multiPartitionValues = agentServerSDLBConfig.multiPartitionValues,
          parallelism = agentServerSDLBConfig.parallelism, statePath = agentServerSDLBConfig.statePath, overrideJars = agentServerSDLBConfig.overrideJars
          , test = agentServerSDLBConfig.test, streaming = agentServerSDLBConfig.streaming)

        val resultingSubfeeds = sdlb.agentExec(appConfig = sdlConfig, phase = agentInstruction.phase)(instanceRegistryImplicit)

        //TODO support other subfeed types than SparkSubFeed
        val resultingDataObjectIdToSchema = resultingSubfeeds.map(subFeed => DataObjectId(subFeed.dataObjectId.id) -> subFeed.asInstanceOf[SparkSubFeed].dataFrame.get.inner.schema.toDDL).toMap

        Some(SDLMessage(SDLMessageType.AgentResult, agentResult = Some(AgentResult(instructionId = agentInstruction.instructionId, phase = agentInstruction.phase, dataObjectIdToSchema = resultingDataObjectIdToSchema))))
    }
  }


}
