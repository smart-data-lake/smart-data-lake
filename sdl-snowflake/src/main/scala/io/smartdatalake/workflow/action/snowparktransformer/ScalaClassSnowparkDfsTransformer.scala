/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.snowparktransformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.smartdatalake.SnowparkDataFrame
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.customlogic.CustomSnowparkDfsTransformer

case class ScalaClassSnowparkDfsTransformer(override val name: String = "snowparkScalaTransform",
                                            override val description: Option[String] = None,
                                            className: String, options: Map[String, String] = Map(),
                                            runtimeOptions: Map[String, String] = Map())
  extends OptionsSnowparkDfsTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomSnowparkDfsTransformer](className)

  override def transformWithOptions(actionId: ActionId, dfs: Map[String, SnowparkDataFrame],
                                    options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, SnowparkDataFrame] = {
    customTransformer.transform(options, dfs)
  }

  override def factory: FromConfigFactory[ParsableSnowparkDfsTransformer] = ScalaClassSnowparkDfsTransformer
}


object ScalaClassSnowparkDfsTransformer extends FromConfigFactory[ParsableSnowparkDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry):
  ScalaClassSnowparkDfsTransformer = {
    extract[ScalaClassSnowparkDfsTransformer](config)
  }
}
