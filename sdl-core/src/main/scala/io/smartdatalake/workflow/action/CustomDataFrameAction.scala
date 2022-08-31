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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{Condition, ExecutionMode, SparkStreamingMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, GenericDfsTransformerDef, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * This [[Action]] transforms data between many input and output DataObjects using DataFrames.
 * CustomDataFrameAction allows to define transformations between n input DataObjects and m output DataObjects,
 * but is is recommended to implement n:1 or 1:m transformations, as otherwise dependencies between DataObjects might not be accurate anymore.
 * The input DataFrames might be transformed using SQL or DataFrame transformations.
 * When chaining multiple transformers, output DataFrames of previous transformers are available as input DataFrames for later transformers by their corresponding name.
 *
 * @param inputIds               input DataObject's
 * @param outputIds              output DataObject's
 * @param transformer            optional custom transformation for multiple dataframes to apply
 * @param transformers list of transformations to apply. See [[spark.transformer]] for a list of included Transformers.
 *                     The transformations are applied according to the ordering of the list.
 *                     Note that all outputs of previous transformers are kept as input for next transformer,
 *                     but in the end only outputs of the last transformer are mapped to output DataObjects.
 * @param mainInputId            optional selection of main inputId used for execution mode and partition values propagation. Only needed if there are multiple input DataObject's.
 * @param mainOutputId           optional selection of main outputId used for execution mode and partition values propagation. Only needed if there are multiple output DataObject's.
 * @param executionMode          optional execution mode for this Action
 * @param executionCondition     optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition   optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                               If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 * @param recursiveInputIds      output of action that are used as input in the same action
 * @param inputIdsToIgnoreFilter optional list of input ids to ignore filter (partition values & filter clause)
 */
case class CustomDataFrameAction(override val id: ActionId,
                                 inputIds: Seq[DataObjectId],
                                 outputIds: Seq[DataObjectId],
                                 @Deprecated @deprecated("Use transformers instead.", "2.0.5")
                                 transformer: Option[CustomDfsTransformerConfig] = None,
                                 transformers: Seq[GenericDfsTransformer] = Seq(),
                                 override val breakDataFrameLineage: Boolean = false,
                                 override val persist: Boolean = false,
                                 override val mainInputId: Option[DataObjectId] = None,
                                 override val mainOutputId: Option[DataObjectId] = None,
                                 override val executionMode: Option[ExecutionMode] = None,
                                 override val executionCondition: Option[Condition] = None,
                                 override val metricsFailCondition: Option[String] = None,
                                 override val metadata: Option[ActionMetadata] = None,
                                 recursiveInputIds: Seq[DataObjectId] = Seq(),
                                 override val inputIdsToIgnoreFilter: Seq[DataObjectId] = Seq()
                             )(implicit instanceRegistry: InstanceRegistry) extends DataFrameActionImpl {

  override val recursiveInputs: Seq[DataObject with CanCreateDataFrame] = recursiveInputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val inputs: Seq[DataObject with CanCreateDataFrame] = inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val outputs: Seq[DataObject with CanWriteDataFrame] = outputIds.map(getOutputDataObject[DataObject with CanWriteDataFrame])

  if (executionMode.exists(_.isInstanceOf[SparkStreamingMode]) && (transformer.exists(_.sqlCode.nonEmpty) || transformers.exists(_.isInstanceOf[SQLDfsTransformer])))
    logger.warn("Defining custom stateful streaming operations with sqlCode is not well supported by Spark and can create strange errors or effects. Use scalaCode to be safe.")

  validateConfig()

  override def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])(implicit context: ActionPipelineContext): Seq[DataFrameSubFeed] = {
    val partitionValues = getMainPartitionValues(inputSubFeeds)
    applyTransformers(transformers ++ transformer.map(_.impl), partitionValues, inputSubFeeds, outputSubFeeds)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    applyTransformers(transformers ++ transformer.map(_.impl).toSeq, partitionValues)
  }

  private val transformerDefs: Seq[GenericDfsTransformerDef] = transformer.map(t => t.impl).toSeq ++ transformers

  override val transformerSubFeedType: Option[Type] = {
    val transformerTypeStats = transformerDefs.map(_.getSubFeedSupportedType)
      .filterNot(_ =:= typeOf[DataFrameSubFeed]) // ignore generic transformers
      .groupBy(identity).mapValues(_.size).toSeq.sortBy(_._2)
    assert(transformerTypeStats.size <= 1, s"No common transformer subFeedType type found: ${transformerTypeStats.map{case (tpe,cnt) => s"${tpe.typeSymbol.name}: $cnt"}.mkString(",")}")
    transformerTypeStats.map(_._1).headOption
  }

  override def factory: FromConfigFactory[Action] = CustomDataFrameAction

}


object CustomDataFrameAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomDataFrameAction = {
    extract[CustomDataFrameAction](config)
  }
}
