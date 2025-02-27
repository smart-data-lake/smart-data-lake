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

import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformerDef, SQLDfTransformer}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, SubFeed}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Implementation of logic needed to use SparkAction with only one input and one output SubFeed.
 */
abstract class DataFrameOneToOneActionImpl extends DataFrameActionImpl {

  /**
   * Input [[DataObject]] which can CanCreateDataFrame
   */
  def input: DataObject with CanCreateDataFrame

  /**
   * Output [[DataObject]] which can CanWriteDataFrame
   */
  def output: DataObject with CanWriteDataFrame

  /**
   * SubFeed types of DataFrame transformers to apply with this action
   * Override by subclasses if there are transformers.
   */
  def transformerSubFeedSupportedTypes: Seq[Type] = Seq()

  override lazy val transformerSubFeedType: Option[Type] = {
    val transformerTypeStats = transformerSubFeedSupportedTypes
      .filterNot(_ =:= typeOf[DataFrameSubFeed]) // ignore generic transformers
      .groupBy(identity).mapValues(_.size).toSeq.sortBy(_._2)
    assert(transformerTypeStats.size <= 1, s"($id) No common transformer subFeedType type found: ${transformerTypeStats.map { case (tpe, cnt) => s"${tpe.typeSymbol.name}: $cnt" }.mkString(",")}")
    transformerTypeStats.map(_._1).headOption
  }

  /**
   * Prepares list of transformers
   */
  private[smartdatalake] def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef]

  /**
   * Transform a [[SparkSubFeed]].
   * To be implemented by subclasses.
   *
   * @param inputSubFeed [[SparkSubFeed]] to be transformed
   * @param outputSubFeed [[SparkSubFeed]] to be enriched with transformed result
   * @return transformed output [[SparkSubFeed]]
   */
  def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed

  override protected final def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])(implicit context: ActionPipelineContext): Seq[DataFrameSubFeed] = {
    assert(inputSubFeeds.size == 1, s"($id) Only one inputSubFeed allowed")
    assert(outputSubFeeds.size == 1, s"($id) Only one outputSubFeed allowed")
    val transformedSubFeed = transform(inputSubFeeds.head, outputSubFeeds.head)
    Seq(transformedSubFeed)
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    assert(inputSubFeeds.size == 1, s"($id) Only one inputSubFeed allowed")
    assert(outputSubFeeds.size == 1, s"($id) Only one outputSubFeed allowed")
    if (isAsynchronousProcessStarted) return
    super.postExec(inputSubFeeds, outputSubFeeds)
    postExecSubFeed(inputSubFeeds.head, outputSubFeeds.head)
  }

  /**
   * Executes operations needed after executing an action for the SubFeed.
   * Can be implemented by sub classes.
   */
  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = ()

  /**
   * apply transformer to SubFeed
   */
  private[smartdatalake] def applyTransformers(transformers: Seq[GenericDfTransformerDef], inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val duplicateTransformerNames = transformers.groupBy(_.name).values.filter(_.size > 1).map(_.head.name)
    assert(!transformers.exists(_.isInstanceOf[SQLDfTransformer]) || duplicateTransformerNames.isEmpty, s"($id) transformers.name must be unique if SQLDfTransformer is used, but duplicate (default?) names ${duplicateTransformerNames.mkString(", ")} where detected")
    val (transformedSubFeed, _) = transformers.foldLeft((inputSubFeed, Option.empty[String])) {
      case ((subFeed, previousTransformerName), transformer) => (transformer.applyTransformation(id, subFeed, previousTransformerName, executionModeResultOptions), Some(transformer.name))
    }
    // Note that transformed partition values are set by execution mode.
    outputSubFeed.withDataFrame(transformedSubFeed.dataFrame)
  }
}
