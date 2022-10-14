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

import com.typesafe.config.{ConfigObject, ConfigRenderOptions, ConfigValueFactory}
import io.smartdatalake.config.ConfigParser.{CONFIG_SECTION_ACTIONS, CONFIG_SECTION_CONNECTIONS, CONFIG_SECTION_DATAOBJECTS}
import io.smartdatalake.workflow.action.{Action, RemoteActionConfig}
import io.smartdatalake.workflow.connection.Connection
import org.eclipse.jetty.websocket.client.WebSocketClient

import java.net.URI
import scala.collection.JavaConverters._

object AgentClient {
  def prepareHoconInstructions(actionToSerialize: Action, connectionsToSerialize: Seq[Connection]): String = {
    val allConnectedDataObjects = actionToSerialize.inputs ++ actionToSerialize.outputs
    val allConnectionIds = allConnectedDataObjects.map(_._config.get).filter(_.hasPath("connectionId")).map(_.getValue("connectionId").render(ConfigRenderOptions.concise().setJson(false)))
    val relevantConnections: Seq[Connection] = connectionsToSerialize.filter(connectionId => allConnectionIds.contains(connectionId.id.id))

    val hoconConfigToSend: ConfigObject = ConfigValueFactory.fromMap(
      Map(CONFIG_SECTION_ACTIONS ->
        ConfigValueFactory.fromMap(
          Map(actionToSerialize.id.id ->
            ConfigValueFactory.fromAnyRef(actionToSerialize._config.get.root())).asJava)
        ,
        CONFIG_SECTION_DATAOBJECTS ->
          ConfigValueFactory.fromMap(
            allConnectedDataObjects.map(dataObject =>
              dataObject.id.id -> ConfigValueFactory.fromAnyRef(dataObject._config.get.root())).toMap
              .asJava),

        CONFIG_SECTION_CONNECTIONS ->
          ConfigValueFactory.fromMap(
            relevantConnections.map(connection =>
              connection.id.id -> ConfigValueFactory.fromAnyRef(connection._config.get.root())).toMap
              .asJava)).asJava
    )
    val hoconString = hoconConfigToSend.render(ConfigRenderOptions.concise().setJson(false))
    hoconString
  }
}
case class AgentClient(remoteActionConfig: RemoteActionConfig) {
  val socket = new AgentClientSocket()

  def sendInstructions(instructions: String): Unit = {
    val uri = URI.create(remoteActionConfig.remoteAgentURL)
    val client = new WebSocketClient

    client.start()

    val fut = client.connect(socket, uri)
    // Wait for Connect
    val session = fut.get
    session.getRemote.sendString(instructions)
  }
}
