/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.app

import io.smartdatalake.communication.agent.JettyAgentServerConfig.{DefaultPort, MaxPortRetries}
import io.smartdatalake.communication.agent.{JettyAgentServer, JettyAgentServerConfig}
import io.smartdatalake.util.hdfs.PartitionValues
import scopt.OParser

/**
 * Smart Data Lake Builder application for agent mode using simple, unsecure websocket communication with Jetty.
 * This is recommended for development use only.
 *
 */
object LocalJettyAgentSmartDataLakeBuilder extends SmartDataLakeBuilder {

  private val agentParser: OParser[_, LocalJettyAgentSmartDataLakeBuilderConfig] = {
    val builder = OParser.builder[LocalJettyAgentSmartDataLakeBuilderConfig]
    import builder._
    OParser.sequence(
      parserGeneric(false),
      opt[Int]('p', "port")
        .action((arg, config) => config.copy(port = arg))
        .text(s"Port that this agent listens to. Default is ${JettyAgentServerConfig.DefaultPort}"),
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

    OParser.parse(agentParser, args, LocalJettyAgentSmartDataLakeBuilderConfig()) match {
      case Some(agentServerConfig) =>
        val server = JettyAgentServer(this, agentServerConfig)
        server.start()
      case None =>
        throwOParserError()
    }
  }
}

case class LocalJettyAgentSmartDataLakeBuilderConfig(override val feedSel: String = "*", // agent normally executes all feeds in registry
                                                     override val applicationName: Option[String] = Some("jetty-agent"),
                                                     override val configuration: Seq[String] = Seq(),
                                                     override val configurationValueOverwrite: Map[String, String] = Map(),
                                                     override val partitionValues: Option[Seq[PartitionValues]] = None,
                                                     override val parallelism: Int = 1,
                                                     override val statePath: Option[String] = None,
                                                     override val test: Option[TestMode.Value] = None,
                                                     override val streaming: Boolean = false,
                                                     port: Int = DefaultPort,
                                                     maxPortRetries: Int = MaxPortRetries,
                                                     override val useOnlyLocalConnectionConfig: Boolean = true
                                                    )
  extends CanBuildAgentSmartDataLakeBuilderConfig[LocalJettyAgentSmartDataLakeBuilderConfig]