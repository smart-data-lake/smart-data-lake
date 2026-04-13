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
package io.smartdatalake.app

import io.smartdatalake.communication.agent.StorageAgentServer
import io.smartdatalake.util.hdfs.PartitionValues
import scopt.OParser

/**
 * Smart Data Lake Builder application for agent mode using storage for communication.
 */
object LocalStorageAgentSmartDataLakeBuilder extends SmartDataLakeBuilder {

  private val agentParser: OParser[_, LocalStorageAgentSmartDataLakeBuilderConfig] = {
    val builder = OParser.builder[LocalStorageAgentSmartDataLakeBuilderConfig]
    import builder._
    OParser.sequence(
      parserGeneric(false),
      opt[String]('p', "path")
        .required()
        .action((value, c) => c.copy(path = value))
        .text("Hadoop path where the agent reads instructions from and writes result information to."),
      opt[Int]('i', "pollIntervalSec")
        .action((value, c) => c.copy(pollIntervalSec = value))
        .text("Polling interval in Seconds. Default is 60 seconds."),
      opt[Int]("stopAfterSec")
        .action((value, c) => c.copy(stopAfterSec = Some(value)))
        .text("Number of seconds the agent run, and stop afterwards."),
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

    OParser.parse(agentParser, args, LocalStorageAgentSmartDataLakeBuilderConfig()) match {
      case Some(config) =>

        // poll for instructions
        val server = new StorageAgentServer(this, config)
        var doPoll = true
        while (doPoll) {
          doPoll = server.pollForInstructions()
        }

      case None =>
        throwOParserError()
    }
  }
}

case class LocalStorageAgentSmartDataLakeBuilderConfig(override val feedSel: String = "*", // agent normally executes all feeds in registry
                                                       override val applicationName: Option[String] = Some("storage-agent"),
                                                       override val configuration: Seq[String] = Seq(),
                                                       override val configurationValueOverwrite: Map[String, String] = Map(),
                                                       override val partitionValues: Option[Seq[PartitionValues]] = None,
                                                       override val parallelism: Int = 1,
                                                       override val statePath: Option[String] = None,
                                                       override val test: Option[TestMode.Value] = None,
                                                       override val streaming: Boolean = false,
                                                       path: String = null,
                                                       pollIntervalSec: Int = 60,
                                                       stopAfterSec: Option[Int] = None,
                                                       override val useOnlyLocalConnectionConfig: Boolean = true
                                                      )
  extends CanBuildAgentSmartDataLakeBuilderConfig[LocalStorageAgentSmartDataLakeBuilderConfig]