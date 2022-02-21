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

import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.action.sparktransformer._
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed, SubFeed}
import org.apache.spark.sql.{Column, SparkSession}

/**
 * Implementation of logic needed to use SparkAction with only one input and one output SubFeed.
 */
abstract class SparkOneToOneActionImpl extends SparkActionImpl {

  /**
   * Input [[DataObject]] which can CanCreateDataFrame
   */
  def input: DataObject with CanCreateDataFrame

  /**
   * Output [[DataObject]] which can CanWriteDataFrame
   */
  def output:  DataObject with CanWriteDataFrame

  /**
   * Transform a [[SparkSubFeed]].
   * To be implemented by subclasses.
   *
   * @param inputSubFeed [[SparkSubFeed]] to be transformed
   * @param outputSubFeed [[SparkSubFeed]] to be enriched with transformed result
   * @return transformed output [[SparkSubFeed]]
   */
  def transform(inputSubFeed: SparkSubFeed, outputSubFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed

  override final def transform(inputSubFeeds: Seq[SparkSubFeed], outputSubFeeds: Seq[SparkSubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed] = {
    assert(inputSubFeeds.size == 1, s"($id) Only one inputSubFeed allowed")
    assert(outputSubFeeds.size == 1, s"($id) Only one outputSubFeed allowed")
    val transformedSubFeed = transform(inputSubFeeds.head, outputSubFeeds.head)
    Seq(transformedSubFeed)
  }

  /**
   * Combines all transformations into a list of DfTransformers
   */
  def getTransformers(transformation: Option[CustomDfTransformerConfig],
                      columnBlacklist: Option[Seq[String]],
                      columnWhitelist: Option[Seq[String]],
                      additionalColumns: Option[Map[String,String]],
                      standardizeDatatypes: Boolean,
                      additionalTransformers: Seq[DfTransformer],
                      filterClauseExpr: Option[Column] = None)
                     (implicit session: SparkSession, context: ActionPipelineContext): Seq[DfTransformer] = {
    Seq(
      transformation.map(t => t.impl),
      columnBlacklist.map(l => BlacklistTransformer(columnBlacklist = l)),
      columnWhitelist.map(l => WhitelistTransformer(columnWhitelist = l)),
      additionalColumns.map(cs => AdditionalColumnsTransformer(additionalColumns = cs)),
      filterClauseExpr.map(f => DfTransformerFunctionWrapper("filter", df => df.where(f))),
      if (standardizeDatatypes) Some(StandardizeDatatypesTransformer()) else None // currently we cast decimals away only but later we may add further type casts
    ).flatten ++ additionalTransformers
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
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
  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit

  /**
   * apply transformer to SubFeed
   */
  protected def applyTransformers(transformers: Seq[DfTransformer], inputSubFeed: SparkSubFeed, outputSubFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    val transformedSubFeed = transformers.foldLeft(inputSubFeed){
      case (subFeed, transformer) => transformer.applyTransformation(id, subFeed)
    }
    // Note that transformed partition values are set by execution mode.
    outputSubFeed.copy(dataFrame = transformedSubFeed.dataFrame)
  }
}
