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
package io.smartdatalake.workflow.action.mlflow

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.mlflow.MLflowPythonUtil
import io.smartdatalake.workflow.SubFeed
import io.smartdatalake.workflow.action.{Action, ActionHelper, NoDataToProcessWarning}
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{DataObject, MLflowDataObject}
import io.smartdatalake.workflow.dataobject.spark.CanCreateSparkDataFrame
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.DataFrame

/**
 * Common implementation of the MLflow Actions.
 *
 * These Actions implement [[Action]] directly instead of extending [[io.smartdatalake.workflow.action.ActionSubFeedsImpl]],
 * because they mix SubFeed types: their data input and output are SparkSubFeeds, while the
 * [[MLflowDataObject]] is connected through a [[io.smartdatalake.workflow.ParameterSubFeed]].
 * [[io.smartdatalake.workflow.action.ActionSubFeedsImpl]] is parameterized on a single SubFeed type and converts
 * every input to it, which would drop the DataFrame.
 *
 * Consequently these Actions support neither execution modes, expectations, save mode options, DataFrame caching
 * nor simulation runs. Write the result to a DataObject and continue with a CopyAction if you need those.
 */
private[smartdatalake] abstract class MLflowActionImpl extends Action {

  /**
   * The MLflow DataObject holding the connection information.
   */
  def mlflow: MLflowDataObject

  override val executionMode: Option[ExecutionMode] = None // execution modes are not supported
  override val metricsFailCondition: Option[String] = None // no metrics to check so far

  /**
   * Options passed to the generated python code, see [[io.smartdatalake.util.mlflow.MLflowPythonCode]].
   * Subclasses add their own.
   */
  protected def pythonOptions: Map[String, String] = mlflow.pythonOptions

  protected def getPythonUtil(implicit context: ActionPipelineContext): MLflowPythonUtil =
    MLflowPythonUtil(id, SparkSubFeed.getSparkSession, pythonOptions)

  /**
   * Check that the SubFeeds handed over by the DAG match the input DataObjects of this Action.
   */
  protected def validateInputSubFeeds(subFeeds: Seq[SubFeed]): Unit = {
    val inputIds = inputs.map(_.id)
    val superfluous = subFeeds.map(_.dataObjectId).diff(inputIds)
    val missing = inputIds.diff(subFeeds.map(_.dataObjectId))
    assert(superfluous.isEmpty && missing.isEmpty, s"($id) input SubFeeds must match input DataObjects: " +
      s"${if (superfluous.nonEmpty) "superfluous=" + superfluous.mkString(",") + " " else ""}" +
      s"${if (missing.nonEmpty) "missing=" + missing.mkString(",") else ""}")
  }

  /**
   * Make sure a "no data to process" warning carries a skipped SubFeed per output, as the DAG expects a result for
   * every output DataObject. This is what ActionSubFeedsImpl does for the other Actions.
   */
  protected def withSkippedOutputsOnNoData(results: => Seq[SubFeed]): Seq[SubFeed] = {
    try results
    catch {
      case ex: NoDataToProcessWarning if ex.results.isEmpty =>
        throw ex.copy(results = Some(ActionHelper.createSkippedSubFeeds(outputs)))
    }
  }

  protected def getPartitionValues(subFeeds: Seq[SubFeed], dataObjectId: DataObjectId): Seq[PartitionValues] =
    subFeeds.find(_.dataObjectId == dataObjectId).map(_.partitionValues).getOrElse(Seq())

  /**
   * Get the DataFrame of an input DataObject.
   *
   * It is taken from the incoming SubFeed if that transports one, and read from the DataObject otherwise. Note that
   * reading it from the DataObject is the normal case: a DataFrame is only passed on between Actions if the
   * preceding Action has `cacheOutput` enabled.
   */
  protected def getInputDataFrame(subFeeds: Seq[SubFeed], input: DataObject with CanCreateSparkDataFrame)
                                 (implicit context: ActionPipelineContext): DataFrame = {
    val partitionValues = getPartitionValues(subFeeds, input.id)
    subFeeds.find(_.dataObjectId == input.id)
      .map(SparkSubFeed.fromSubFeed)
      .flatMap(_.dataFrame.map(_.inner))
      .getOrElse(input.getSparkDataFrame(partitionValues))
  }
}
