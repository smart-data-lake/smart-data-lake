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

package io.smartdatalake.util.lab

import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.util.misc.{CustomCodeUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, DataObject}
import org.apache.spark.sql.SparkSession

/**
 * An interface for accessing SDLB objects for interactive use (lab, development of transformation)
 *
 * @param session Spark session to use
 * @param configuration One or multiple configuration files or directories containing configuration files, separated by comma.
 */
case class SparkLab(session: SparkSession, configuration: Seq[String]) {
  val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(configuration)
  val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(session, registry)
  val data = SparkLab.getAnonymousCatalogInstance(registry, context)
}

object SparkLab extends SmartDataLakeLogger {
  import scala.reflect.runtime.universe._

  // get Scala Toolbox to compile code at runtime
  import scala.tools.reflect.ToolBox
  private lazy val tb = scala.reflect.runtime.currentMirror.mkToolBox()

  def generateDataObjectsCatalogClassEntries(registry: InstanceRegistry, context: ActionPipelineContext) = {
    val wrappers = registry.getDataObjects.flatMap {
      case x: DataObject with CanCreateSparkDataFrame =>
        Some(q"""val ${DataFrameUtil.strToLowerCamelCase(x.id.id)} = LabSparkDataObjectWrapper[x.getClass.getName](x, context)""")
      case x =>
        logger.info(s"No catalog entry created for ${x.id} of type ${x.getClass.getSimpleName}, as it does not implement CanCreateSparkDataFrame")
        None
    }
    val cls = q"""
      class DataObjectsAccessor(registry: InstanceRegistry, context: ActionPipelineContext) {
        ${}
      }
    """
    wrappers
  }
  def getAnonymousCatalogInstance(registry: InstanceRegistry, context: ActionPipelineContext) = {
    tb.eval(q"new { ${generateDataObjectsCatalogClassEntries(registry, context).mkString("\n")} }")
  }
}