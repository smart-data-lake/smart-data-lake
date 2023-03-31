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

import io.smartdatalake.communication.agent.JettyAgentServerConfig.{DefaultPort, MaxPortRetries}
import io.smartdatalake.communication.agent.{AgentServerController, JettyAgentServer, JettyAgentServerConfig}
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import scopt.OParser

import java.io.File




case class LocalJettyAgentSmartDataLakeBuilderConfig(override val feedSel: String = null,
                                                     override val applicationName: Option[String] = None,
                                                     override val configuration: Option[Seq[String]] = None,
                                                     override val master: Option[String] = None,
                                                     override val deployMode: Option[String] = None,
                                                     override val username: Option[String] = None,
                                                     override val kerberosDomain: Option[String] = None,
                                                     override val keytabPath: Option[File] = None,
                                                     override val partitionValues: Option[Seq[PartitionValues]] = None,
                                                     override val multiPartitionValues: Option[Seq[PartitionValues]] = None,
                                                     override val parallelism: Int = 1,
                                                     override val statePath: Option[String] = None,
                                                     override val overrideJars: Option[Seq[String]] = None,
                                                     override val test: Option[TestMode.Value] = None,
                                                     override val streaming: Boolean = false,
                                                     port: Int = DefaultPort,
                                                     maxPortRetries: Int = MaxPortRetries
                                                    )

  extends SmartDataLakeBuilderConfigTrait[LocalJettyAgentSmartDataLakeBuilderConfig] {
  override def withfeedSel(value: String): LocalJettyAgentSmartDataLakeBuilderConfig = copy(feedSel = value)

  override def withapplicationName(value: Option[String]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(applicationName = value)

  override def withconfiguration(value: Option[Seq[String]]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(configuration = value)

  override def withmaster(value: Option[String]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(master = value)

  override def withdeployMode(value: Option[String]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(deployMode = value)

  override def withusername(value: Option[String]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(username = value)

  override def withkerberosDomain(value: Option[String]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(kerberosDomain = value)

  override def withkeytabPath(value: Option[File]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(keytabPath = value)

  override def withpartitionValues(value: Option[Seq[PartitionValues]]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(partitionValues = value)

  override def withmultiPartitionValues(value: Option[Seq[PartitionValues]]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(multiPartitionValues = value)

  override def withparallelism(value: Int): LocalJettyAgentSmartDataLakeBuilderConfig = copy(parallelism = value)

  override def withstatePath(value: Option[String]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(statePath = value)

  override def withoverrideJars(value: Option[Seq[String]]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(overrideJars = value)

  override def withtest(value: Option[TestMode.Value]): LocalJettyAgentSmartDataLakeBuilderConfig = copy(test = value)

  override def withstreaming(value: Boolean): LocalJettyAgentSmartDataLakeBuilderConfig = copy(streaming = value)
}