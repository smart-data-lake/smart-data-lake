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

package io.smartdatalake.workflow.agent

import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.config.SdlConfigObject.AgentId
import io.smartdatalake.config.{ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.workflow.AtlasExportable
import io.smartdatalake.workflow.connection.Connection

private[smartdatalake] trait Agent extends SdlConfigObject with ParsableFromConfig[Agent] with AtlasExportable {
  /**
   * A unique identifier for this instance.
   */
  override val id: AgentId

  val url: String

  val connections: Map[String, Connection]

  val agentClientClassName : String

  def toStringShort: String = {
    s"$id[${this.getClass.getSimpleName}]"
  }

  override def atlasName: String = id.id
}
