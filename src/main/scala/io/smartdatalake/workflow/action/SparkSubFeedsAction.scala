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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

abstract class SparkSubFeedsAction extends Action {

  override def inputs: Seq[DataObject with CanCreateDataFrame]
  override def outputs: Seq[DataObject with CanWriteDataFrame]

  /**
   * Transform [[SparkSubFeed]]'s.
   * To be implemented by subclasses.
   *
   * @param subFeeds [[SparkSubFeed]]'s to be transformed
   * @return transformed [[SparkSubFeed]]'s
   */
  def transform(subFeeds: Seq[SparkSubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed]

  private def doTransform(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed] = {
    // convert subfeeds to SparkSubFeed type or initialize if not yet existing
    var preparedSubFeeds = subFeeds.map( SparkSubFeed.fromSubFeed )
    // break lineage if requested
    preparedSubFeeds = if (breakDataFrameLineage) preparedSubFeeds.map(_.breakLineage()) else preparedSubFeeds
    // persist if requested
    preparedSubFeeds = if (persist) preparedSubFeeds.map(_.persist) else preparedSubFeeds
    transform(preparedSubFeeds)
  }

  /**
   * Generic init implementation for Action.init
   * */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == inputs.size, s"Number of subFeed's must match number of inputs for SparkSubFeedActions (Action $id, subfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}, inputs ${inputs.map(_.id).mkString(",")})")
    doTransform(subFeeds)
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == inputs.size, s"Number of subFeed's must match number of inputs for SparkSubFeedActions (Action $id, subfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}, inputs ${inputs.map(_.id).mkString(",")})")
    //transform
    val transformedSubFeeds = doTransform(subFeeds)
    // write output
    outputs.foreach { output =>
      val subFeed = transformedSubFeeds.find(_.dataObjectId == output.id).getOrElse(throw new IllegalStateException(s"subFeed for output ${output.id} not found"))
      setSparkJobDescription(s"writing to DataObject ${subFeed.dataObjectId}")
      output.writeDataFrame(subFeed.dataFrame.get, subFeed.partitionValues)
    }
    // return
    transformedSubFeeds
  }

  /**
   * Stop propagating input DataFrame through action and instead get a new DataFrame from DataObject
   * This is needed if the input DataFrame includes many transformations from previous Actions.
   */
  def breakDataFrameLineage: Boolean = false

  /**
   * Force persisting DataFrame on Disk.
   * This helps to reduce memory needed for caching the DataFrame content and can serve as a recovery point in case an task get's lost.
   */
  def persist: Boolean = false

  /**
   * Enriches SparkSubFeeds with DataFrame if not existing
   *
   * @param inputs input data objects.
   * @param subFeeds input SubFeeds.
   */
  protected def enrichSubFeedsDataFrame(inputs: Seq[DataObject with CanCreateDataFrame], subFeeds: Seq[SparkSubFeed])(implicit session: SparkSession): Seq[SparkSubFeed] = {
    assert(inputs.size==subFeeds.size, s"Number of inputs must match number of subFeeds given for $id")
    inputs.map { input =>
      val subFeed = subFeeds.find(_.dataObjectId == input.id).getOrElse(throw new IllegalStateException(s"subFeed for input ${input.id} not found"))
      ActionHelper.enrichSubFeedDataFrame(input, subFeed)
    }
  }
}
