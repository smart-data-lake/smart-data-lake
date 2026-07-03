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
package io.smartdatalake.workflow.agent

import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.config.SdlConfigObject.AgentId
import io.smartdatalake.config.{ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.workflow.connection.Connection

private[smartdatalake] trait Agent extends SdlConfigObject with ParsableFromConfig[Agent] {
  /**
   * A unique identifier for this instance.
   */
  override def id: AgentId

  /**
   * Optional Map of private connections that this agent has access to.
   *
   * If an Agent has no own Hocon connection configuration in the remote location, connections can be defined here
   * and they will override connections defined in the global connections section for execution on the Agent.
   * This allows the Agent to use some connections that are only accessible in the Agent's environment.
   *
   * Note that Agent Servers need to be started with useOnlyLocalConnectionConfig=false to use this feature for security reasons.
   */
  def connections: Map[String, Connection]

  /**
   * The client that is used to communicate with this agent
   */
  def getClient: AgentClient

  def toStringShort: String = {
    s"$id[${this.getClass.getSimpleName}]"
  }

}
