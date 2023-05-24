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

package io.smartdatalake.util.misc

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.definitions.{Environment, SaveModeMergeOptions}
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{RuntimeEventState, RuntimeInfo, SDLExecutionId}
import io.smartdatalake.workflow.dataobject.{CanMergeDataFrame, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

import java.sql.Timestamp

/**
 * Writes final metrics and action executions to given DataObjects.
 * The DataObjects have to implement TransactionalSparkTableDataObject and be able to merge records, e.g. JdbcTableDataObject or DeltaLakeTableDataObject
 *
 * To enable add this state listener as follows to global config section:
 *
 * stateListeners = [{
 *   className = "io.smartdatalake.util.misc.FinalMetricsLogWriter"
 *   options = {
 *     metricsLogDataObjectId = "xxx"    // id of DataObject where metrics should be written to. Define primary key as run_id, run_start_tstmp, action_id, data_object_id.
 *     actionLogDataObjectId = "xxx"     // primary or secondary key found under azure LogAnalytics workspace's 'agents management' section. Define primary key as run_id, run_start_tstmp, action_id, attempt_id.
 *   }
 * }]
 */
class FinalMetricsLogWriter(options: Map[String, StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  // get data objects
  private val metricsLogDataObjectId = DataObjectId(options("metricsLogDataObjectId").resolve())
  private val actionLogDataObjectId = DataObjectId(options("actionLogDataObjectId").resolve())
  private lazy val metricsLogDataObject = Environment.instanceRegistry.get[TransactionalTableDataObject with CanMergeDataFrame](metricsLogDataObjectId)
  private lazy val actionLogDataObject = Environment.instanceRegistry.get[TransactionalTableDataObject with CanMergeDataFrame](actionLogDataObjectId)

  // primary keys:
  private val metricsLogPrimaryKey = Seq("run_id", "run_start_tstmp", "action_id", "data_object_id")
  private val actionLogPrimaryKey = Seq("run_id", "run_start_tstmp", "action_id", "attempt_id")

  logger.info(s"instantiated: metricsLogDataObject=${metricsLogDataObjectId.id} actionLogDataObject=${actionLogDataObjectId.id}")

  override def init(context: ActionPipelineContext): Unit = {
    implicit val iContext: ActionPipelineContext = context
    implicit val spark: SparkSession = context.sparkSession

    // check primary keys set correctly
    assert(metricsLogDataObject.table.primaryKey.contains(metricsLogPrimaryKey), s"Primary key of ${metricsLogDataObject.id} must be set to '${metricsLogPrimaryKey.mkString(", ")}'")
    assert(actionLogDataObject.table.primaryKey.contains(actionLogPrimaryKey), s"Primary key of ${actionLogDataObject.id} must be set to '${actionLogPrimaryKey.mkString(", ")}'")

    // check connection and schema
    actionLogDataObject.initSparkDataFrame(createActionDf(Seq[ActionLog]()), Seq(), saveModeOptions = Some(SaveModeMergeOptions()))
    metricsLogDataObject.initSparkDataFrame(createMetricsDf(Seq[ActionLog]()), Seq(), saveModeOptions = Some(SaveModeMergeOptions()))

    logger.info(s"initialized")
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId : Option[ActionId]): Unit = {
    implicit val spark = context.sparkSession

    // only final state is saved
    if (state.isFinal) {
      logger.info(s"logging final state")
      val actionLogs = LogExtractor.getFinalLogs(state, context)

      val actionDf = createActionDf(actionLogs)
      actionLogDataObject.writeSparkDataFrame(actionDf, saveModeOptions = Some(SaveModeMergeOptions()))(context)
      logger.info(s"logged actions to ${actionLogDataObject.id}")

      val metricsDf = createMetricsDf(actionLogs)
      metricsLogDataObject.writeSparkDataFrame(metricsDf, saveModeOptions = Some(SaveModeMergeOptions()))(context)
      logger.info(s"logged metrics to ${metricsLogDataObject.id}")
    }
  }

  private def createActionDf(actionLog: Seq[ActionLog])(implicit spark: SparkSession) = {
    import spark.implicits._
    actionLog.toDF.drop($"data_object_metrics")
  }
  private def createMetricsDf(actionLog: Seq[ActionLog])(implicit spark: SparkSession) = {
    import spark.implicits._
    actionLog.toDF
      .withColumn("data_object_metrics", explode($"data_object_metrics"))
      .select($"run_id", $"run_start_tstmp", $"action_id", $"attempt_id", $"data_object_metrics.*")
  }
}

/**
 * Helper methods to extract log entries from final state.
 */
object LogExtractor extends SmartDataLakeLogger {

  def getFinalLogs(state: ActionDAGRunState, context: ActionPipelineContext): Seq[ActionLog] = {
    val finalActions = state.actionsState.filter {
      case (actionId, info) => Set(RuntimeEventState.SUCCEEDED, RuntimeEventState.FAILED, RuntimeEventState.STREAMING).contains(info.state)
    }.flatMap { // filter (flatMap...) executionId with type different from SDLExecutionId
      case (actionId, info) =>
        info.executionId match {
          case x: SDLExecutionId =>
            Some((actionId, info, x))
          case x =>
            logger.warn(s"Extracting logs from RuntimeInfo with executionId of type ${x.getClass.getSimpleName} not supported.")
            None
        }
    }
    finalActions.map {
      case (actionId, info, sdlExecutionId) => getActionLog(actionId, info, sdlExecutionId, state.finalState.get, context)
    }.toSeq
  }

  def getActionLog(actionId: ActionId, info: RuntimeInfo, executionId: SDLExecutionId, finalState: RuntimeEventState, context: ActionPipelineContext): ActionLog = {
    val duration = info.duration.map(_.getSeconds)
    val dataObjectLogs = info.results.map { result =>
      val numTasks = result.mainMetrics.get("num_tasks").map(castToLong)
      val filesWritten = result.mainMetrics.get("files_written").map(castToLong)
      val recordsWritten = result.mainMetrics.get("records_written").map(castToLong)
      val metrics = result.mainMetrics.filterKeys(!Set("num_tasks", "files_written", "records_written").contains(_))
      MetricsLog(result.subFeed.dataObjectId.id, Timestamp.valueOf(info.startTstmp.get), numTasks, filesWritten, recordsWritten, metrics.mapValues(_.toString), result.subFeed.partitionValues.map(_.getMapString))
    }
    ActionLog(executionId.runId, Timestamp.valueOf(context.runStartTime), actionId.id
      , executionId.attemptId, Timestamp.valueOf(context.attemptStartTime), finalState
      , Timestamp.valueOf(info.startTstmp.get), duration.get
      , info.state, info.dataObjectsState.map(_.toStringTuple).toMap, dataObjectLogs
    )
  }

  private def castToLong(v: Any): Long = v match {
    case x: Long => x
    case x: Int => x.toLong
    case x: Short => x.toLong
    case x: Byte => x.toLong
    case x: BigInt => x.toLong
    case _ => throw new IllegalStateException(s"$v can not be converted to Long")
  }
}

/**
 * State attributes to log for an Action execution
 * @param run_id id of the run
 * @param run_start_tstmp start time of the run the action belongs to
 * @param action_id id of the action
 * @param attempt_id attempt to execute this run.
 * @param attempt_start_tstmp start time of the attempt
 * @param start_tstmp start time of the action
 * @param duration duration of the action execution
 * @param input_data_object_state state of input DataObjects filled by DataObjectStateIncrementalMode
 */
case class ActionLog(
                      run_id: Long,
                      run_start_tstmp: Timestamp,
                      action_id: String,
                      attempt_id: Int,
                      attempt_start_tstmp: Timestamp,
                      run_state: RuntimeEventState,
                      start_tstmp: Timestamp,
                      duration: Float,
                      state: RuntimeEventState,
                      input_data_object_state: Map[String, String],
                      data_object_metrics: Seq[MetricsLog]
                    )

/**
 * State attributes to log for a DataObject execution
 * @param data_object_id id of the DataObject that the metrics belong to
 * @param start_tstmp start time of the action that wrote to the DataObject
 * @param num_tasks number of Spark tasks that have written to the DataObject
 * @param files_written number of files that were written to the DataObject
 * @param records_written number of records that were written to the DataObject
 * @param metrics other metrics collected when the DataObject was written
 * @param partition_values partitions that have been written to the DataObject
 */
case class MetricsLog(
                       data_object_id: String,
                       start_tstmp: Timestamp,
                       num_tasks: Option[Long],
                       files_written: Option[Long],
                       records_written: Option[Long],
                       metrics: Map[String, String],
                       partition_values: Seq[Map[String, String]]
                     )