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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{Condition, ExecutionMode, SparkStreamingOnceMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.customlogic.CustomDfsTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.SparkSession

/**
 * [[Action]] to transform data according to a custom transformer.
 * Allows to transform multiple input and output dataframes.
 *
 * @param inputIds               input DataObject's
 * @param outputIds              output DataObject's
 * @param transformer            custom transformation for multiple dataframes to apply
 * @param mainInputId            optional selection of main inputId used for execution mode and partition values propagation. Only needed if there are multiple input DataObject's.
 * @param mainOutputId           optional selection of main outputId used for execution mode and partition values propagation. Only needed if there are multiple output DataObject's.
 * @param executionMode          optional execution mode for this Action
 * @param executionCondition     optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition   optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                               If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 * @param recursiveInputIds      output of action that are used as input in the same action
 * @param inputIdsToIgnoreFilter optional list of input ids to ignore filter (partition values & filter clause)
 */
case class CustomSparkAction (override val id: ActionId,
                              inputIds: Seq[DataObjectId],
                              outputIds: Seq[DataObjectId],
                              transformer: CustomDfsTransformerConfig,
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
)(implicit instanceRegistry: InstanceRegistry) extends SparkSubFeedsAction {

  // checks
  recursiveInputIds.foreach(inputId => assert(outputIds.contains(inputId), s"($id) $inputId from recursiveInputIds is not listed in outputIds of the same action."))
  inputIdsToIgnoreFilter.foreach(inputId => assert((inputIds++recursiveInputIds).contains(inputId), s"($id) $inputId from inputIdsToIgnoreFilter is not listed in inputIds of the same action."))

  override val recursiveInputs: Seq[DataObject with CanCreateDataFrame] = recursiveInputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val inputs: Seq[DataObject with CanCreateDataFrame] = inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val outputs: Seq[DataObject with CanWriteDataFrame] = outputIds.map(getOutputDataObject[DataObject with CanWriteDataFrame])

  if (executionMode.exists(_.isInstanceOf[SparkStreamingOnceMode]) && transformer.sqlCode.nonEmpty)
    logger.warn("Defining custom stateful streaming operations with sqlCode is not well supported by Spark and can create strange errors or effects. Use scalaCode to be safe.")

  override def transform(inputSubFeeds: Seq[SparkSubFeed], outputSubFeeds: Seq[SparkSubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed] = {
    val mainInput = getMainInput(inputSubFeeds)
    val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId==mainInput.id)

    // Apply custom transformation to all subfeeds
    val partitionValues = mainInputSubFeed.map(_.partitionValues).getOrElse(Seq())
    val outputDfs = transformer.transform(id, partitionValues, inputSubFeeds.map( subFeed => (subFeed.dataObjectId.id, subFeed.dataFrame.get)).toMap)
    // create output subfeeds from transformed dataframes
    outputDfs.map {
      case (dataObjectId, dataFrame) =>
        val outputSubFeed = outputSubFeeds.find(_.dataObjectId.id == dataObjectId)
          .getOrElse(throw ConfigurationException(s"($id) No output found for result ${dataObjectId}. Configured outputs are ${outputs.map(_.id.id).mkString(", ")}."))
        // get partition values from main input
        outputSubFeed.copy(dataFrame = Some(dataFrame))
    }.toSeq
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    transformer.transformPartitionValues(id, partitionValues)
  }

  override def factory: FromConfigFactory[Action] = CustomSparkAction
}


object CustomSparkAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomSparkAction = {
    extract[CustomSparkAction](config)
  }
}
