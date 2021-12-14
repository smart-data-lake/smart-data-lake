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

package io.smartdatalake.workflow.action.snowparktransformer

import io.smartdatalake.config.ParsableFromConfig
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.smartdatalake.SnowparkDataFrame
import io.smartdatalake.workflow.ActionPipelineContext

trait SnowparkDfsTransformer {
  def name: String

  def description: Option[String]

  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = Unit

  def transform(actionId: ActionId, dfs: Map[String, SnowparkDataFrame])
               (implicit context: ActionPipelineContext): Map[String, SnowparkDataFrame]

  private[smartdatalake]
  def applyTransformation(actionId: ActionId, dfs: Map[String, SnowparkDataFrame])
                         (implicit context: ActionPipelineContext): Map[String, SnowparkDataFrame] = {
    transform(actionId, dfs)
  }
}

trait ParsableSnowparkDfsTransformer extends SnowparkDfsTransformer with ParsableFromConfig[ParsableSnowparkDfsTransformer]


trait OptionsSnowparkDfsTransformer extends ParsableSnowparkDfsTransformer {
  def options: Map[String, String]

  def runtimeOptions: Map[String, String]

  def transformWithOptions(actionId: ActionId, dfs: Map[String, SnowparkDataFrame], options: Map[String, String])
                          (implicit context: ActionPipelineContext): Map[String, SnowparkDataFrame]

  override def transform(actionId: ActionId, dfs: Map[String, SnowparkDataFrame])
                        (implicit context: ActionPipelineContext): Map[String, SnowparkDataFrame] = {
    transformWithOptions(actionId, dfs, options)
  }
}
