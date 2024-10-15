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

import io.smartdatalake.communication.agent.{AgentServerController, AzureRelayAgentServer}
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import scopt.OParser

import java.io.File

/**
 * Smart Data Lake Builder application for agent mode.
 *
 * Sets master to local[*] and deployMode to client by default.
 */

case class LocalAzureRelayAgentSmartDataLakeBuilderConfig(feedSel: String = null,
                                                          applicationName: Option[String] = Some("AgentApp"),
                                                          configuration: Option[Seq[String]] = None,
                                                          master: Option[String] = Some("local[*]"),
                                                          deployMode: Option[String] = None, partitionValues: Option[Seq[PartitionValues]] = None,
                                                          multiPartitionValues: Option[Seq[PartitionValues]] = None,
                                                          parallelism: Int = 1,
                                                          statePath: Option[String] = None,
                                                          overrideJars: Option[Seq[String]] = None,
                                                          test: Option[TestMode.Value] = None,
                                                          streaming: Boolean = false,
                                                          azureRelayURL: Option[String] = None)
  extends CanBuildSmartDataLakeBuilderConfig[LocalAzureRelayAgentSmartDataLakeBuilderConfig] {
  override def withfeedSel(value: String): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(feedSel = value)

  override def withapplicationName(value: Option[String]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(applicationName = value)

  override def withconfiguration(value: Option[Seq[String]]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(configuration = value)

  override def withmaster(value: Option[String]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(master = value)

  override def withdeployMode(value: Option[String]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(deployMode = value)

  override def withpartitionValues(value: Option[Seq[PartitionValues]]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(partitionValues = value)

  override def withmultiPartitionValues(value: Option[Seq[PartitionValues]]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(multiPartitionValues = value)

  override def withparallelism(value: Int): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(parallelism = value)

  override def withstatePath(value: Option[String]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(statePath = value)

  override def withoverrideJars(value: Option[Seq[String]]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(overrideJars = value)

  override def withtest(value: Option[TestMode.Value]): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(test = value)

  override def withstreaming(value: Boolean): LocalAzureRelayAgentSmartDataLakeBuilderConfig = copy(streaming = value)
}

object LocalAzureRelayAgentSmartDataLakeBuilder extends SmartDataLakeBuilder {

  val agentParser: OParser[_, LocalAzureRelayAgentSmartDataLakeBuilderConfig] = {
    val builder = OParser.builder[LocalAzureRelayAgentSmartDataLakeBuilderConfig]
    import builder._
    OParser.sequence(
      parserGeneric(feedSelRequired = false),
      opt[String]('u', "url")
        .required()
        .action((arg, config) => config.copy(azureRelayURL = Some(arg)))
        .text(s"Url of the Azure Relay Hybrid Connection that this Server should connect to"),
    )
  }

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Program $appType v$appVersion")

    OParser.parse(agentParser, args, LocalAzureRelayAgentSmartDataLakeBuilderConfig()) match {
      case Some(agentServerConfig) =>
        val agentController: AgentServerController = AgentServerController(new InstanceRegistry, this)
        AzureRelayAgentServer.start(agentServerConfig, agentController)
      case None => logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
