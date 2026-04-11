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
package io.smartdatalake.app

import io.smartdatalake.communication.agent.AzureRelayAgentServer
import io.smartdatalake.util.hdfs.PartitionValues
import scopt.OParser

/**
 * Smart Data Lake Builder application for agent mode.
 *
 * Sets master to local[*] and deployMode to client by default.
 */

case class LocalAzureRelayAgentSmartDataLakeBuilderConfig(override val feedSel: String = null,
                                                          override val applicationName: Option[String] = Some("AgentApp"),
                                                          override val configuration: Seq[String] = Seq(),
                                                          override val configurationValueOverwrite: Map[String, String] = Map(),
                                                          override val partitionValues: Option[Seq[PartitionValues]] = None,
                                                          override val parallelism: Int = 1,
                                                          override val statePath: Option[String] = None,
                                                          override val test: Option[TestMode.Value] = None,
                                                          override val streaming: Boolean = false,
                                                          azureRelayURL: Option[String] = None,
                                                          override val useOnlyLocalConnectionConfig: Boolean = true)
  extends CanBuildAgentSmartDataLakeBuilderConfig[LocalAzureRelayAgentSmartDataLakeBuilderConfig]

object LocalAzureRelayAgentSmartDataLakeBuilder extends SmartDataLakeBuilder {

  private val agentParser: OParser[_, LocalAzureRelayAgentSmartDataLakeBuilderConfig] = {
    val builder = OParser.builder[LocalAzureRelayAgentSmartDataLakeBuilderConfig]
    import builder._
    OParser.sequence(
      parserGeneric(feedSelRequired = false),
      opt[String]('u', "url")
        .required()
        .action((arg, config) => config.copy(azureRelayURL = Some(arg)))
        .text(s"Url of the Azure Relay Hybrid Connection that this Server should connect to"),
      opt[Boolean]('b', "useOnlyLocalConnectionConfig")
        .action((arg, config) => config.copy(useOnlyLocalConnectionConfig = arg))
        .text(
          s"""
             | Dont allow receiving connection configurations from the client, only use local ones.
             | This is a security feature to avoid that the client can connect to arbitrary data sources.
             | Default is true.
          """.stripMargin)
    )
  }

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logProgramStart()

    OParser.parse(agentParser, args, LocalAzureRelayAgentSmartDataLakeBuilderConfig()) match {
      case Some(agentServerConfig) =>
        val server = AzureRelayAgentServer(this, agentServerConfig)
        server.start()
      case None =>
        throwOParserError()
    }
  }
}
