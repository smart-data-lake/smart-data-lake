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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.action.customlogic.SnowparkDfsTransformer
import io.smartdatalake.workflow.dataobject.{CanCreateSnowparkDataFrame, CanWriteSnowparkDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SnowparkSubFeed}

// This action takes n input dataObjects, passes them through a CustomSnowparkDfsTransformer using Snowpark DataFrames,
// then writes the result to m output dataObjects
case class CustomSnowparkAction(override val id: ActionId,
                                inputIds: Seq[DataObjectId],
                                outputIds: Seq[DataObjectId],
                                metadata: Option[ActionMetadata] = None,
                                transformer: SnowparkDfsTransformer)
                               (implicit instanceRegistry: InstanceRegistry) extends SnowparkActionImpl {

  override val recursiveInputs: Seq[DataObject with CanCreateSnowparkDataFrame] =
    recursiveInputIds.map(getInputDataObject[DataObject with CanCreateSnowparkDataFrame])

  override val inputs: Seq[DataObject with CanCreateSnowparkDataFrame] =
    inputIds.map(getInputDataObject[DataObject with CanCreateSnowparkDataFrame])

  override val outputs: Seq[DataObject with CanWriteSnowparkDataFrame] =
    outputIds.map(getOutputDataObject[DataObject with CanWriteSnowparkDataFrame])

  validateConfig()

  override def transform(inputSubFeeds: Seq[SnowparkSubFeed], outputSubFeeds: Seq[SnowparkSubFeed])
                        (implicit context: ActionPipelineContext): Seq[SnowparkSubFeed] = {
    applyTransformers(transformer, inputSubFeeds, outputSubFeeds)
  }

  override def factory: FromConfigFactory[Action] = CustomSparkAction

}

object CustomSnowparkAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomSnowparkAction = {
    extract[CustomSnowparkAction](config)
  }
}
