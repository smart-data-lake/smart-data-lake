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

import io.smartdatalake.communication.agent.{AgentController, AgentServer, AgentServerConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.hadoop.conf.Configuration

import java.io.File
import java.time.LocalDateTime

/**
 * Smart Data Lake Builder application for agent mode.
 *
 * Sets master to local[*] and deployMode to client by default.
 * //TODO Build the same for non local?
 */
object LocalAgentSmartDataLakeBuilder extends SmartDataLakeBuilder {

  // optional master and deploy-mode settings to override defaults local[*] / client. Note that using something different than master=local is experimental.
  parser.opt[String]('m', "master")
    .action((arg, config) => config.copy(master = Some(arg)))
    .text("The Spark master URL passed to SparkContext (default=local[*], yarn, spark://HOST:PORT, mesos://HOST:PORT, k8s://HOST:PORT).")
  parser.opt[String]('x', "deploy-mode")
    .action((arg, config) => config.copy(deployMode = Some(arg)))
    .text("The Spark deploy mode passed to SparkContext (default=client, cluster).")

  // optional kerberos authentication parameters for local mode
  parser.opt[String]('d', "kerberos-domain")
    .action((arg, config) => config.copy(kerberosDomain = Some(arg)))
    .text("Kerberos-Domain for authentication (USERNAME@KERBEROS-DOMAIN) in local mode.")
  parser.opt[String]('u', "username")
    .action((arg, config) => config.copy(username = Some(arg)))
    .text("Kerberos username for authentication (USERNAME@KERBEROS-DOMAIN) in local mode.")
  parser.opt[File]('k', "keytab-path")
    .action((arg, config) => config.copy(keytabPath = Some(arg)))
    .text("Path to the Kerberos keytab file for authentication in local mode.")

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
      username = sys.env.get("SDL_KERBEROS_USER"),
      kerberosDomain = sys.env.get("SDL_KERBEROS_DOMAIN"),
      keytabPath = sys.env.get("SDL_KEYTAB_PATH").map(new File(_)),
      configuration = sys.env.get("SDL_CONFIGURATION").map(_.split(',')),
      parallelism = sys.env.get("SDL_PARALELLISM").map(_.toInt).getOrElse(1),
      statePath = sys.env.get("SDL_STATE_PATH")
    )

    val agentController: AgentController = AgentController(new InstanceRegistry, this)
    AgentServer.start(AgentServerConfig(sdlConfig = envconfig), agentController)

    val result = exec(envconfig, SDLExecutionId.executionId1, LocalDateTime.now(), LocalDateTime.now(), Map(), Seq(), Seq(), None, Seq(), simulation = false, globalConfig = GlobalConfig())(agentController.instanceRegistry)


    // start
    //1. Start Websocket
    //2. Start some sleep-loop waiting for instructions
    //When receving info from websocket write special hocon conf file to execute
    implicit val defaultHadoopConf: Configuration = new Configuration()
    //TODO manipulate config with input from websocket eg change file path
    //    val configToRun = config.copy(configuration = Some(Seq("theConfigPathSavedByWebsocket")))

    //Create dataobjects and action, add to instanceRegistry like in
    // test("action dag with 2 dependent actions from same predecessor, PartitionDiffMode and another action with no data to process") {
    //


  }


}
