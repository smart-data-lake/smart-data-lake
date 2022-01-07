/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.smartdatalake.SnowparkDataFrame
import io.smartdatalake.workflow.action.customlogic.SnowparkDfsTransformer
import io.smartdatalake.workflow.dataobject.{CanCreateSnowparkDataFrame, CanWriteSnowparkDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SnowparkSubFeed}

private[smartdatalake] abstract class SnowparkActionImpl extends ActionSubFeedsImpl[SnowparkSubFeed] {


  override def metricsFailCondition: Option[String] = None

  override def inputIdsToIgnoreFilter: Seq[DataObjectId] = Seq()

  override def inputs: Seq[DataObject with CanCreateSnowparkDataFrame]

  override def outputs: Seq[DataObject with CanWriteSnowparkDataFrame]

  // Create a DataFrame (if not existing) for an input SubFeed
  override protected def preprocessInputSubFeedCustomized(subFeed: SnowparkSubFeed,
                                                          ignoreFilters: Boolean,
                                                          isRecursive: Boolean)
                                                         (implicit context: ActionPipelineContext): SnowparkSubFeed = {
    val inputMap: Map[DataObjectId, DataObject with CanCreateSnowparkDataFrame] =
      (inputs ++ recursiveInputs).map(input => input.id -> input).toMap

    val input: DataObject with CanCreateSnowparkDataFrame = inputMap(subFeed.dataObjectId)

    if (subFeed.dataFrame.isEmpty) {
      subFeed.copy(dataFrame = Some(input.getSnowparkDataFrame()))
    } else {
      subFeed
    }
  }

  override def recursiveInputs: Seq[DataObject with CanCreateSnowparkDataFrame] = Seq()

  // Persist the result SubFeed
  override protected def writeSubFeed(subFeed: SnowparkSubFeed, isRecursive: Boolean)
                                     (implicit context: ActionPipelineContext): WriteSubFeedResult = {
    val output: DataObject with CanWriteSnowparkDataFrame = outputs.find(_.id == subFeed.dataObjectId)
      .getOrElse(throw new IllegalStateException(s"($id) output for subFeed ${subFeed.dataObjectId} not found"))

    output.writeSnowparkDataFrame(subFeed.dataFrame.get, isRecursive, None)
    WriteSubFeedResult(None)
  }

  // Apply the transformer defined in the action to the input DataFrames
  protected def applyTransformer(transformer: SnowparkDfsTransformer,
                                 inputSubFeeds: Seq[SnowparkSubFeed],
                                 outputSubFeeds: Seq[SnowparkSubFeed])
                                (implicit context: ActionPipelineContext): Seq[SnowparkSubFeed] = {

    val inputDfsMap: Map[String, SnowparkDataFrame] = inputSubFeeds
      .map(subFeed => (subFeed.dataObjectId.id, subFeed.dataFrame.get)).toMap

    val outputDfsMap: Map[String, SnowparkDataFrame] = transformer.transform(inputDfsMap)

    // Check that all output IDs have an associated DataFrame and copy the output SubFeeds
    outputDfsMap.map {
      case (dataObjectId, dataFrame) =>
        val outputSubFeed = outputSubFeeds.find(_.dataObjectId.id == dataObjectId)
          .getOrElse(throw ConfigurationException(s"($id) No output found for result ${dataObjectId}. " +
            s"Configured outputs are ${outputs.map(_.id.id).mkString(", ")}"))
        outputSubFeed.copy(dataFrame = Some(dataFrame))
    }.toSeq
  }
}
