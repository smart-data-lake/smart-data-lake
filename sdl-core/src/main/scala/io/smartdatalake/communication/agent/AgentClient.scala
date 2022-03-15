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

import io.smartdatalake.workflow.action.{Action, RemoteActionConfig}
import org.eclipse.jetty.websocket.client.WebSocketClient

import java.net.URI

case class AgentClient(remoteActionConfig: RemoteActionConfig) {
  def sendAction(actionToSerialize: Action): Unit = {
    val uri = URI.create(remoteActionConfig.remoteAgentURL)
    val client = new WebSocketClient

    client.start()
    val socket = new AgentClientSocket()
    val fut = client.connect(socket, uri)
    // Wait for Connect
    val session = fut.get

    val serializedConfig = SerializedConfig(inputDataObjects = actionToSerialize.inputs.map(_._config.get)
      , outputDataObjects = actionToSerialize.outputs.map(_._config.get), action = actionToSerialize._config.get)

    val hoconString = serializedConfig.asHoconString
    session.getRemote.sendString(hoconString)
  }
}
