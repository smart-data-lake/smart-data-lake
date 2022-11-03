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

package io.smartdatalake.config

import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SerializableHadoopConfiguration
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.conf.Configuration

import java.time.LocalDateTime

/**
 * Helper methods to use config outside of SmartDataLakeBuilder, e.g. Notebooks
 */
object ConfigToolbox {

  /**
   * Load and parse config objects
   *
   * @param locations list of config locations
   * @param userClassLoader when working in notebooks and loading dependencies through the notebook metadata configuration, it might be needed to pass the ClassLoader of the notebook, otherwise this function might not be able to load classes referenced in the configuration.
   * @return instanceRegistry with parsed config objects and parsed global config
   */
  def loadAndParseConfig(locations: Seq[String], userClassLoader: Option[ClassLoader] = None): (InstanceRegistry, GlobalConfig) = {
    // override classloader if given
    userClassLoader.foreach(classLoader => Environment._classLoader = Some(classLoader))
    // load config
    val defaultHadoopConf: Configuration = new Configuration()
    val config = ConfigLoader.loadConfigFromFilesystem(locations, defaultHadoopConf)
    val globalConfig = GlobalConfig.from(config)
    val registry = ConfigParser.parse(config)
    (registry, globalConfig)
  }

  /**
   * Create an action pipeline context used by many DataObject and Action methods.
   */
  def getDefaultActionPipelineContext(implicit sparkSession : org.apache.spark.sql.SparkSession, instanceRegistry : io.smartdatalake.config.InstanceRegistry) : ActionPipelineContext = {
    val defaultHadoopConf = new SerializableHadoopConfiguration(new Configuration())
    val name = "interactive"
    val globalConfig = GlobalConfig()
    val context = ActionPipelineContext(name, name, SDLExecutionId.executionId1, instanceRegistry, Some(LocalDateTime.now()), SmartDataLakeBuilderConfig(name, Some(name)), phase = ExecutionPhase.Exec, serializableHadoopConf = defaultHadoopConf, globalConfig = globalConfig)
    globalConfig._sparkSession = Option(sparkSession)
    context
  }

}
