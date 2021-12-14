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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.smartdatalake.SnowparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, SnowparkSubFeed}

trait SnowparkDfTransformer {
  def name: String

  def description: Option[String]

  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = Unit

  def transform(actionId: ActionId, df: SnowparkDataFrame, dataObjectId: DataObjectId)
               (implicit context: ActionPipelineContext): SnowparkDataFrame

  private[smartdatalake] def applyTransformation(actionId: ActionId, subFeed: SnowparkSubFeed)
                                                (implicit context: ActionPipelineContext): SnowparkSubFeed = {
    val transformedDf = subFeed.dataFrame.map(df =>
      transform(actionId, df, subFeed.dataObjectId))
    subFeed.copy(dataFrame = transformedDf)
  }
}

trait ParsableSnowparkDfTransformer extends SnowparkDfTransformer with ParsableFromConfig[ParsableSnowparkDfTransformer]

