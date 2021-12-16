/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

package io.smartdatalake.workflow.action.customlogic

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, ParsableFromConfig}
import io.smartdatalake.smartdatalake.SnowparkDataFrame
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.ActionPipelineContext

// Project-specific custom Snowpark transformers should extend this trait
trait CustomSnowparkDfsTransformer extends Serializable {
  def transform(options: Map[String, String], dfs: Map[String, SnowparkDataFrame]): Map[String, SnowparkDataFrame]
}

// Transformers defined in CustomSnowparkActions in the configuration files are parsed into this case class
case class SnowparkDfsTransformer(val name: String = "snowparkScalaTransform",
                                  val description: Option[String] = None,
                                  className: String,
                                  options: Map[String, String] = Map())
  extends ParsableFromConfig[SnowparkDfsTransformer] {

  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomSnowparkDfsTransformer](className)

  def transform(dfs: Map[String, SnowparkDataFrame])(implicit context: ActionPipelineContext): Map[String, SnowparkDataFrame] = {
    customTransformer.transform(options, dfs)
  }

  override def factory: FromConfigFactory[SnowparkDfsTransformer] = SnowparkDfsTransformer

  override def toString: String = s"className: $className"
}

// This companion object ensures that SnowparkDfsTransformer can be parsed from the configuration
object SnowparkDfsTransformer extends FromConfigFactory[SnowparkDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry):
  SnowparkDfsTransformer = {
    extract[SnowparkDfsTransformer](config)
  }
}
