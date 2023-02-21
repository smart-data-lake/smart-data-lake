/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.definitions.Condition
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericDfsTransformer
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * This [[Action]] copies data between an input and output DataObject using DataFrames.
 * The input DataObject reads the data and converts it to a DataFrame according to its definition.
 * The DataFrame might be transformed using SQL or DataFrame transformations.
 * Then the output DataObjects writes the DataFrame to the output according to its definition.
 *
 * @param inputIds input DataObjects
 * @param outputIds output DataObjects
 * @param transformers optional list of transformations to apply. See [[spark.transformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
 */
case class MLflowAction(
    override val id: ActionId,
    inputIds: Seq[DataObjectId],
    outputIds: Seq[DataObjectId],
    transformers: Seq[GenericDfsTransformer] = Seq(),
    mode: String,
    override val breakDataFrameLineage: Boolean = false,
    override val persist: Boolean = false,
    override val mainInputId: Option[DataObjectId] = None,
    override val mainOutputId: Option[DataObjectId] = None,
    override val executionMode: Option[ExecutionMode] = None,
    override val executionCondition: Option[Condition] = None,
    override val metricsFailCondition: Option[String] = None,
    override val metadata: Option[ActionMetadata] = None,
)(implicit instanceRegistry: InstanceRegistry)
    extends DataFrameActionImpl {

  // check mode
  if (mode.toLowerCase != "train" && mode.toLowerCase != "predict") {
    throw ConfigurationException(s"($id) mode can only be one of 'train' or 'predict'")
  }

  override val inputs: Seq[DataObject with CanCreateDataFrame] =
    inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val outputs: Seq[DataObject with CanWriteDataFrame] =
    outputIds.map(getOutputDataObject[DataObject with CanWriteDataFrame])

  private val transformerDefs: Seq[GenericDfsTransformer] = transformers

  override val transformerSubFeedType: Option[Type] = {
    val transformerTypeStats = transformerDefs
      .map(_.getSubFeedSupportedType)
      .filterNot(_ =:= typeOf[DataFrameSubFeed]) // ignore generic transformers
      .groupBy(identity)
      .mapValues(_.size)
      .toSeq
      .sortBy(_._2)
    assert(
      transformerTypeStats.size <= 1,
      s"No common transformer subFeedType type found: ${transformerTypeStats
        .map { case (tpe, cnt) => s"${tpe.typeSymbol.name}: $cnt" }
        .mkString(",")}"
    )
    transformerTypeStats.map(_._1).headOption
  }

  validateConfig()

  override def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])(implicit
      context: ActionPipelineContext
  ): Seq[DataFrameSubFeed] = {
    val partitionValues = getMainPartitionValues(inputSubFeeds)
    applyTransformers(transformers, partitionValues, inputSubFeeds, outputSubFeeds)
  }

  /*
  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformerDefs.foreach(_.prepare(id))
  }


  override def transformPartitionValues(
      partitionValues: Seq[PartitionValues]
  )(implicit context: ActionPipelineContext): Map[PartitionValues, PartitionValues] = {
    applyTransformers(transformerDefs, partitionValues)
  }
   */
  override def factory: FromConfigFactory[Action] = MLflowAction
}

object MLflowAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowAction = {
    extract[MLflowAction](config)
  }
}
