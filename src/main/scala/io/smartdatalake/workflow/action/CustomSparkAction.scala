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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.action.customlogic.CustomDfsTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.SparkSession

/**
 * [[Action]] to transform data according to a custom transformer.
 * Allows to transform multiple input and output dataframes.
 *
 * @param id
 * @param inputIds input DataObject's
 * @param outputIds output DataObject's
 * @param transformer Custom Transformer to transform Seq[DataFrames]
 */
case class CustomSparkAction ( override val id: ActionObjectId,
                               inputIds: Seq[DataObjectId],
                               outputIds: Seq[DataObjectId],
                               transformer: CustomDfsTransformerConfig,
                               override val breakDataFrameLineage: Boolean = false,
                               override val persist: Boolean = false,
                               override val metadata: Option[ActionMetadata] = None
)(implicit instanceRegistry: InstanceRegistry) extends SparkSubFeedsAction {

  override val inputs: Seq[DataObject with CanCreateDataFrame] = inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val outputs: Seq[DataObject with CanWriteDataFrame] = outputIds.map(getOutputDataObject[DataObject with CanWriteDataFrame])

  /**
   * @inheritdoc
   */
  override def transform(subFeeds: Seq[SparkSubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed] = {

    // create input subfeeds if they don't exist yet
    val enrichedSubfeeds: Seq[SparkSubFeed] = enrichSubFeedsDataFrame(inputs, subFeeds)

    // Apply custom transformation to all subfeeds
    transformer.transform(enrichedSubfeeds.map( subFeed => (subFeed.dataObjectId.id, subFeed.dataFrame.get)).toMap)
      .map {
        case (dataObjectId, dataFrame) =>
          // TODO: have a flag propagatePartitionValues in action configuration - when enabled search for matching columns between dataFrame.schema and partitionValues from inputSubFeeds and propagate partitionValues
          SparkSubFeed(Some(dataFrame),dataObjectId, Seq()) // partition values not available here, will be updated next
      }.toSeq
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Action] = CustomSparkAction
}


object CustomSparkAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): CustomSparkAction = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._
    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[CustomSparkAction].value
  }
}
