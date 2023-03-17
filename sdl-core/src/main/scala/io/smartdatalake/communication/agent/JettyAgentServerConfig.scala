/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.communication.agent.JettyAgentServerConfig.{DefaultPort, MaxPortRetries}

object JettyAgentServerConfig {
  val DefaultPort = 4441
  val MaxPortRetries = 10
}
/**
 * Configuration for the Server that provides live status info of the current DAG Execution
 *
 * @param port           : port with which the first connection attempt is made
 * @param maxPortRetries : If port is already in use, we will increment port by one and try with that new port.
 *                       maxPortRetries describes how many times this should be attempted. If set to 0 it will not be attempted.
 *                       Values below 0 are not allowed.
 */
case class JettyAgentServerConfig(port: Int = DefaultPort, maxPortRetries: Int = MaxPortRetries, sdlConfig: SmartDataLakeBuilderConfig)