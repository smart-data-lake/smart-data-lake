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
package io.smartdatalake.app

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.ActionDAGRun
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed

/**
 * Mixin for SmartDataLakeBuilder subclasses that use Spark Structured Streaming.
 * Overrides the streaming lifecycle hooks introduced in SmartDataLakeBuilder with
 * Spark-specific stream management logic.
 */
trait SparkStreamingSupport extends SmartDataLakeLogger { this: SmartDataLakeBuilder =>

  override protected def stopSyncStreamingQueriesGracefully(hasAsyncActions: Boolean)(implicit context: ActionPipelineContext): Unit = {
    if (hasAsyncActions) {
      // re-throw exception if any async streaming query terminated with exception;
      // awaitAnyTermination returns immediately since onQueryTerminated already fired
      SparkSubFeed.getSparkSession(context).streams.awaitAnyTermination(1)
      // stop remaining active streaming queries gracefully
      SparkSubFeed.getSparkSession(context).streams.active.foreach(_.stop())
    }
  }

  override protected def stopAsyncStreamingQueriesGracefully()(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"stopAsyncStreamingQueriesGracefully: stopStreamingGracefully=${Environment.stopStreamingGracefully}, stopping ${SparkSubFeed.getSparkSession(context).streams.active.size} active streams")
    SparkSubFeed.getSparkSession(context).streams.active.foreach(_.stop())
  }

  override protected def awaitAndStopAsyncStreamingQueries(actionDAGRun: ActionDAGRun)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"awaitAndStopAsyncStreamingQueries: waiting for any streaming query to terminate")
    SparkSubFeed.getSparkSession(context).streams.awaitAnyTermination()
    logger.info(s"awaitAndStopAsyncStreamingQueries: awaitAnyTermination returned, active=${SparkSubFeed.getSparkSession(context).streams.active.map(_.name).mkString(",")}")
    SparkSubFeed.getSparkSession(context).streams.active.foreach(_.stop())
  }
}
