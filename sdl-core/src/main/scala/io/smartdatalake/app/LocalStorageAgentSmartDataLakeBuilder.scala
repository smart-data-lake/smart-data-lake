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
        .text("Number of seconds the agent run, and stop afterwards.")
    )
  }

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logProgramStart()

    val envConfig = LocalStorageAgentSmartDataLakeBuilderConfig(
      master = sys.env.get("SDL_SPARK_MASTER_URL").orElse(Some("local[*]")),
      deployMode = sys.env.get("SDL_SPARK_DEPLOY_MODE").orElse(Some("client")),
      configuration = sys.env.get("SDL_CONFIGURATION").map(_.split(',').toSeq).getOrElse(Seq()),
      parallelism = sys.env.get("SDL_PARALELLISM").map(_.toInt).getOrElse(1),
      statePath = sys.env.get("SDL_STATE_PATH"),
    )

    OParser.parse(agentParser, args, envConfig) match {
      case Some(config) =>

        // poll for instructions
        val server = new StorageAgentServer(this)
        var doPoll = true
        while (doPoll) {
          doPoll = server.pollForInstructions(config)
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
                                                       override val master: Option[String] = None,
                                                       override val deployMode: Option[String] = None,
                                                       override val partitionValues: Option[Seq[PartitionValues]] = None,
                                                       override val parallelism: Int = 1,
                                                       override val statePath: Option[String] = None,
                                                       override val test: Option[TestMode.Value] = None,
                                                       override val streaming: Boolean = false,
                                                       path: String = null,
                                                       pollIntervalSec: Int = 60,
                                                       stopAfterSec: Option[Int] = None
                                                      )
  extends CanBuildSmartDataLakeBuilderConfig[LocalStorageAgentSmartDataLakeBuilderConfig]