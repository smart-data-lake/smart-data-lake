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
import io.smartdatalake.definitions.{Condition, ExecutionMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.action.sparktransformer.{GenericDfTransformer, GenericDfTransformerDef}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, SubFeed}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.{Failure, Success, Try}

/**
 * [[Action]] to copy files (i.e. from stage to integration)
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param deleteDataAfterRead a flag to enable deletion of input partitions after copying.
 * @param transformer optional custom transformation to apply.
 * @param transformers optional list of transformations to apply. See [[sparktransformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
 * @param executionMode optional execution mode for this Action
 * @param executionCondition     optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 * @param saveModeOptions override and parametrize saveMode set in output DataObject configurations when writing to DataObjects.
 */
case class CopyAction(override val id: ActionId,
                      inputId: DataObjectId,
                      outputId: DataObjectId,
                      deleteDataAfterRead: Boolean = false,
                      @Deprecated @deprecated("Use transformers instead.", "2.0.5")
                      transformer: Option[CustomDfTransformerConfig] = None,
                      transformers: Seq[GenericDfTransformer] = Seq(),
                      override val breakDataFrameLineage: Boolean = false,
                      override val persist: Boolean = false,
                      override val executionMode: Option[ExecutionMode] = None,
                      override val executionCondition: Option[Condition] = None,
                      override val metricsFailCondition: Option[String] = None,
                      override val saveModeOptions: Option[SaveModeOptions] = None,
                      override val metadata: Option[ActionMetadata] = None
                     )(implicit instanceRegistry: InstanceRegistry) extends DataFrameOneToOneActionImpl {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: DataObject with CanWriteDataFrame = getOutputDataObject[DataObject with CanWriteDataFrame](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[DataObject with CanWriteDataFrame] = Seq(output)

  private val transformerDefs: Seq[GenericDfTransformerDef] = transformer.map(t => t.impl).toSeq ++ transformers

  override val transformerSubFeedSupportedTypes: Seq[Type] = transformerDefs.map(_.getSubFeedSupportedType)

  validateConfig()

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    applyTransformers(transformerDefs, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    applyTransformers(transformerDefs, partitionValues)
  }

  override def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    if (deleteDataAfterRead) input match {
      // delete input partitions if applicable
      case (partitionInput: CanHandlePartitions) if partitionInput.partitions.nonEmpty && inputSubFeed.partitionValues.nonEmpty =>
        partitionInput.deletePartitions(inputSubFeed.partitionValues)
      // otherwise delete all
      case (fileInput: FileRefDataObject) =>
        fileInput.deleteAll
      case x => throw new IllegalStateException(s"($id) input ${input.id} doesn't support deleting data")
    }
  }

  override def factory: FromConfigFactory[Action] = CopyAction
}

object CopyAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CopyAction = {
    extract[CopyAction](config)
  }
}
