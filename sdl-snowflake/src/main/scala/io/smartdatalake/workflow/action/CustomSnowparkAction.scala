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

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.definitions.{Condition, ExecutionMode, SparkStreamingMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.customlogic.{CustomDfsTransformerConfig, CustomSnowparkDfsTransformerConfig}
import io.smartdatalake.workflow.action.snowparktransformer.ParsableSnowparkDfsTransformer
import io.smartdatalake.workflow.action.sparktransformer.{ParsableDfsTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.{ActionPipelineContext, SnowparkSubFeed, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanCreateSnowparkDataFrame, CanWriteDataFrame, CanWriteSnowparkDataFrame, DataObject}

class CustomSnowparkAction(override val id: ActionId,
                           inputIds: Seq[DataObjectId],
                           outputIds: Seq[DataObjectId],
                           transformer: Option[CustomSnowparkDfsTransformerConfig] = None,
                           transformers: Seq[ParsableSnowparkDfsTransformer] = Seq(),
                           override val mainInputId: Option[DataObjectId] = None,
                           override val mainOutputId: Option[DataObjectId] = None,
                           override val executionMode: Option[ExecutionMode] = None,
                           override val executionCondition: Option[Condition] = None,
                           override val metricsFailCondition: Option[String] = None,
                           override val metadata: Option[ActionMetadata] = None,
                           recursiveInputIds: Seq[DataObjectId] = Seq(),
                           override val inputIdsToIgnoreFilter: Seq[DataObjectId] = Seq()
                          )(implicit instanceRegistry: InstanceRegistry) extends SnowparkActionImpl {

  override val recursiveInputs: Seq[DataObject with CanCreateSnowparkDataFrame] =
    recursiveInputIds.map(getInputDataObject[DataObject with CanCreateSnowparkDataFrame])

  override val inputs: Seq[DataObject with CanCreateSnowparkDataFrame] =
    inputIds.map(getInputDataObject[DataObject with CanCreateSnowparkDataFrame])

  override val outputs: Seq[DataObject with CanWriteSnowparkDataFrame] =
    outputIds.map(getOutputDataObject[DataObject with CanWriteSnowparkDataFrame])

  validateConfig()

  override def transform(inputSubFeeds: Seq[SnowparkSubFeed], outputSubFeeds: Seq[SnowparkSubFeed])
                        (implicit context: ActionPipelineContext): Seq[SnowparkSubFeed] = {
    applyTransformers(transformers ++ transformer.map(_.impl), inputSubFeeds, outputSubFeeds)
  }

  override def factory: FromConfigFactory[Action] = CustomSparkAction

}
