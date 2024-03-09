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

package io.smartdatalake.lab

import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession

/**
 * An interface for accessing SDLB objects for interactive use (lab, development of transformation)
 *
 * @param session                  Spark session to use
 * @param configuration            One or multiple configuration files or directories containing configuration files, separated by comma.
 * @param dataObjectCatalogFactory A method to create a data object catalog instance to be used by this SmartDataLakeBuilderLab.
 *                                 Note that this is normally an instance from a class created by LabCatalogGenerator in a second compile phase.
 * @param userClassLoader          when working in notebooks and loading dependencies through the notebook metadata configuration,
 *                                 it might be needed to pass the ClassLoader of the notebook, otherwise SDLB might not be able to load classes referenced in the configuration.
 */
case class SmartDataLakeBuilderLab[T](
                                       private val session: SparkSession,
                                       private val configuration: Seq[String],
                                       private val dataObjectCatalogFactory: (InstanceRegistry, ActionPipelineContext) => T,
                                       private val userClassLoader: Option[ClassLoader] = None
                                     ) {
  @transient val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(configuration, userClassLoader, session.sparkContext.hadoopConfiguration)
  @transient val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(session, registry)
  @transient val data: T = dataObjectCatalogFactory(registry, context)
}

object SmartDataLakeBuilderLab {
  /**
   * Writing into DataObjects using SmartDataLakeBuilderLab is disabled by default, as this should be done through data pipelines and not interactively.
   * It can be enabled by setting the following variable to true.
   */
  var enableWritingDataObjects = false
}
