/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericDataFrame

/**
 * A Transformer to use single DataFrame Transformers as multiple DataFrame Transformers.
 * This works by selecting the SubFeeds (DataFrames) the single DataFrame Transformer should be applied to.
 * All other SubFeeds will be passed through without transformation.
 * Use it in Actions working on many DataFrames at once, like CustomDataFrameAction, to reuse a
 * one-DataFrame transformer such as [[FilterTransformer]], [[BlacklistTransformer]] or
 * [[DebugTransformer]] there. The transformation is applied to each selected SubFeed separately.
 * The name and description of this transformer are taken over from the wrapped transformer.
 * The Action fails if a name in `subFeedsToApply` does not match one of the SubFeeds handed to this transformer.
 * These are the input DataObject ids if this is the first transformer of the chain, otherwise the output names
 * of the preceding transformer.
 *
 * Example: filter one of the input SubFeeds before joining them with a SQL transformation.
 * {{{
 * actions = {
 *   join-ratings {
 *     type = CustomDataFrameAction
 *     inputIds = [stg-ratings, stg-airports]
 *     outputIds = [int-ratings]
 *     transformers = [{
 *       type = DfTransformerWrapperDfsTransformer
 *       subFeedsToApply = [stg-ratings]
 *       transformer = { type = FilterTransformer, filterClause = "rating is not null" }
 *     },{
 *       type = SQLDfsTransformer
 *       code = { int-ratings = "select * from %{inputViewName_stg-ratings} join %{inputViewName_stg-airports} using (airport_id)" }
 *     }]
 *   }
 * }
 * }}}
 *
 * @param transformer Configuration for a GenericDfTransformerDef to be applied
 * @param subFeedsToApply Names of SubFeeds the transformation should be applied to. Default is an empty list,
 *                        which will apply the transformation to all subfeeds.
 */
case class DfTransformerWrapperDfsTransformer(transformer: GenericDfTransformer, subFeedsToApply: Seq[String] = Seq()) extends GenericDfsTransformer {
  override def name: String = transformer.name
  override def description: Option[String] = transformer.description
  override def transform(actionId: SdlConfigObject.ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, GenericDataFrame], executionModeResultOptions: Map[String,String], outputDataObjectIds: Seq[String])(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    val missingSubFeeds = subFeedsToApply.toSet.diff(dfs.keySet)
    assert(missingSubFeeds.isEmpty, s"($actionId) [transformation.$name] subFeedsToApply ${missingSubFeeds.mkString(", ")} not found in input dfs. Available subFeeds are ${dfs.keys.mkString(", ")}.")
    dfs.map {
      case (subFeedName,df) =>
        if (subFeedsToApply.contains(subFeedName) || subFeedsToApply.isEmpty) (subFeedName, transformer.transform(actionId, partitionValues, df, DataObjectId(subFeedName), Some(subFeedName), executionModeResultOptions))
        else (subFeedName, df)
    }
  }
  override def transformPartitionValues(actionId: SdlConfigObject.ActionId, partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    transformer.transformPartitionValues(actionId, partitionValues, executionModeResultOptions)
  }

  override def factory: FromConfigFactory[GenericDfsTransformer] = DfTransformerWrapperDfsTransformer
}

object DfTransformerWrapperDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DfTransformerWrapperDfsTransformer = {
    extract[DfTransformerWrapperDfsTransformer](config)
  }
}