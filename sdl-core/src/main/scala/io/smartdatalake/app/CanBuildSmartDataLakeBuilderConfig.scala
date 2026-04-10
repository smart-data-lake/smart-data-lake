/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.typesafe.config.Config
import io.smartdatalake.config.ConfigLoader
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ProductUtil
import io.smartdatalake.workflow.HadoopFileActionDAGRunStateStore
import org.apache.hadoop.conf.Configuration

trait CanBuildSmartDataLakeBuilderConfig[R] {

  //All builder functions for all fields of the common SmartDataLakeBuilderConfig
  def withFeedSel(value: String): R = ProductUtil.dynamicCopy(this, "feedSel", value).asInstanceOf[R]

  def withApplicationName(value: Option[String]): R = ProductUtil.dynamicCopy(this, "applicationName", value).asInstanceOf[R]

  def addConfiguration(value: Seq[String]): R = ProductUtil.dynamicCopy(this, "configuration", configuration ++ value).asInstanceOf[R]

  def addConfigurationValueOverwrite(value: (String, String)): R = ProductUtil.dynamicCopy(this, "configurationValueOverwrite", configurationValueOverwrite + value).asInstanceOf[R]

  def addPartitionValues(value: Seq[PartitionValues]): R = ProductUtil.dynamicCopy(this, "partitionValues", Some(partitionValues.getOrElse(Seq()) ++ value)).asInstanceOf[R]

  def withParallelism(value: Int): R = ProductUtil.dynamicCopy(this, "parallelism", value).asInstanceOf[R]

  def withStatePath(value: Option[String]): R = ProductUtil.dynamicCopy(this, "statePath", value).asInstanceOf[R]

  def withTest(value: Option[TestMode.Value]): R = ProductUtil.dynamicCopy(this, "test", value).asInstanceOf[R]

  def withStreaming(value: Boolean): R = ProductUtil.dynamicCopy(this, "streaming", value).asInstanceOf[R]

  def withMaster(value: Option[String]): R = ProductUtil.dynamicCopy(this, "master", value).asInstanceOf[R]

  def withDeployMode(value: Option[String]): R = ProductUtil.dynamicCopy(this, "deployMode", value).asInstanceOf[R]

  // abstract instance variables
  def feedSel: String

  def applicationName: Option[String]

  def configuration: Seq[String]

  def configurationValueOverwrite: Map[String, String]

  def partitionValues: Option[Seq[PartitionValues]]

  def parallelism: Int

  def statePath: Option[String]

  def test: Option[TestMode.Value]

  def streaming: Boolean

  def master: Option[String]

  def deployMode: Option[String]

  // helper methods
  def validate(): Unit = {
    assert(!applicationName.exists(_.contains(HadoopFileActionDAGRunStateStore.fileNamePartSeparator)),
      s"Application name must not contain character '${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}' ($applicationName)")
    assert(!applicationName.exists(_.matches(".*\\s.*")), s"Application name must not contain spaces ($applicationName)")
    assert(!master.contains("yarn") || deployMode.nonEmpty, "spark deploy-mode must be set if spark master=yarn")
    assert(configuration.nonEmpty, "Configuration files are empty")
    assert(statePath.isEmpty || applicationName.isDefined, "application name must be defined if state path is set")
    assert(!streaming || statePath.isDefined, "state path must be set if streaming is enabled")
  }

  val appName: String = applicationName.getOrElse(feedSel)

  def isDryRun: Boolean = test.contains(TestMode.DryRun)

  @JsonIgnore
  def getHoconConfig(validateCompletness: Boolean = true)(implicit hadoopConfiguration: Configuration): Config = {
    val config = ConfigLoader.loadConfigFromFilesystem(configuration, hadoopConfiguration, configurationValueOverwrite)
    require(config.hasPath("actions") || !validateCompletness, s"No configuration parsed or it does not have a section called actions")
    require(config.hasPath("dataObjects") || !validateCompletness, s"No configuration parsed or it does not have a section called dataObjects")
    config
  }

  @JsonIgnore
  def getStdAppConfig: SmartDataLakeBuilderConfig = {
    SmartDataLakeBuilderConfig(feedSel, applicationName, configuration, configurationValueOverwrite, master, deployMode, partitionValues, parallelism, statePath, test, streaming)
  }
}


trait CanBuildAgentSmartDataLakeBuilderConfig[R] extends CanBuildSmartDataLakeBuilderConfig[R] {

  def useOnlyLocalConnectionConfig: Boolean

}