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

import io.smartdatalake.communication.agent.{AgentServerController, AzureRelayAgentServer, AzureRelayAgentServerConfig, JettyAgentServer, JettyAgentServerConfig}
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import scopt.OptionParser

/**
 * Smart Data Lake Builder application for agent mode.
 *
 * Sets master to local[*] and deployMode to client by default.
 */
object LocalAzureRelayAgentSmartDataLakeBuilder extends SmartDataLakeBuilder {

  val agentParser: OptionParser[AzureRelayAgentServerConfig] = new OptionParser[AzureRelayAgentServerConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)

    head(appType, s"$appVersion")

    parser.opt[String]('u', "url")
      .action((arg, config) => config.copy(azureRelayURL = Some(arg)))
      .text(s"Uri of the Azure Relay Hybrid Connection that this Server should connect to")
  }

   /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Program $appType v$appVersion")

    // Set defaults from environment variables
    val envconfig = initConfigFromEnvironment.copy(
      master = sys.env.get("SDL_SPARK_MASTER_URL").orElse(Some("local[*]")),
      deployMode = sys.env.get("SDL_SPARK_DEPLOY_MODE").orElse(Some("client")),
      configuration = sys.env.get("SDL_CONFIGURATION").map(_.split(',')),
      parallelism = sys.env.get("SDL_PARALELLISM").map(_.toInt).getOrElse(1),
      statePath = sys.env.get("SDL_STATE_PATH"),
      applicationName = Some("agent")
    )

    agentParser.parse(args, AzureRelayAgentServerConfig(sdlConfig = envconfig)) match {
      case Some(agentServerConfig) =>
        val agentController: AgentServerController = AgentServerController(new InstanceRegistry, this)
        AzureRelayAgentServer.start(agentServerConfig, agentController)
      case None => logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
