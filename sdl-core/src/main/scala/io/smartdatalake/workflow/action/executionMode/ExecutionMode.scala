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
package io.smartdatalake.workflow.action.executionMode

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigHolder, FromConfigFactory, InstanceRegistry, ParsableFromConfig}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.util.dag.ExceptionSeverity.ExceptionSeverity
import io.smartdatalake.util.dag.{DAGException, ExceptionSeverity}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.dataobject._

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

/**
 * Execution mode defines how data is selected when running a data pipeline.
 * You need to select one of the subclasses by defining type, i.e.
 *
 * {{{
 * executionMode = {
 *   type = DataFrameIncrementalMode
 *   compareCol = "id"
 * }
 * }}}
 */
trait ExecutionMode extends ParsableFromConfig[ExecutionMode] with ConfigHolder with SmartDataLakeLogger {
  /**
   * Called in prepare phase to validate execution mode configuration
   */
  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    // validate apply conditions
    applyConditionsDef.foreach(_.syntaxCheck[DefaultExecutionModeExpressionData](actionId, Some("applyCondition")))
  }
  /**
   * Called in init phase before initialization. Can be used to initialize dataObjectsState, e.g. for DataObjectStateIncrementalMode
   */
  def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = ()
  /**
   * Apply execution mode. Called in init and execution phase.
   */
  def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                   , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                  (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = None
  /**
   * Called in execution phase after writing subfeed. Can be used to implement incremental processing , e.g. deleteDataAfterRead.
   */
  def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = ()
  def mainInputOutputNeeded: Boolean = false
  val applyConditionsDef: Seq[Condition] = Seq()
  val failConditionsDef: Seq[Condition] = Seq()

  /**
   * Evaluate apply conditions.
   * @return Some(true) if any apply conditions evaluates to true (or-logic), None if there are no apply conditions
   */
  final def evaluateApplyConditions(actionId: ActionId, subFeed: SubFeed)(implicit context: ActionPipelineContext): Option[Boolean] = {
    val data = DefaultExecutionModeExpressionData.from(context).copy(givenPartitionValues = subFeed.partitionValues.map(_.getMapString), isStartNode = subFeed.isDAGStart)
    if (applyConditionsDef.nonEmpty) Option(applyConditionsDef.map(_.evaluate(actionId, Some("applyCondition"), data)).max)
    else None
  }

  /**
   * Evaluate fail conditions.
   * @throws ExecutionModeFailedException if any fail condition evaluates to true
   */
  final def evaluateFailConditions[T<:Product:TypeTag](actionId: ActionId, data: T)(implicit context: ActionPipelineContext): Unit = {
    failConditionsDef.foreach(c =>
      if (c.evaluate(actionId, Some("failCondition"), data)) {
        val descriptionText = c.description.map( d => s""""$d" """).getOrElse("")
        throw ExecutionModeFailedException(actionId.id, context.phase, s"""($actionId) Execution mode failed because of failCondition ${descriptionText}expression="${c.expression}" $data""")
      }
    )
  }

  /**
   * If this execution mode should be run as asynchronous streaming process
   */
  def isAsynchronous: Boolean = false
}

trait ExecutionModeWithMainInputOutput {
  def alternativeOutputId: Option[DataObjectId] = None
  def alternativeOutput(implicit context: ActionPipelineContext): Option[DataObject] = {
    alternativeOutputId.map(context.instanceRegistry.get[DataObject](_))
  }
}

/**
 * An execution mode which just validates that partition values are given.
 * Note: For start nodes of the DAG partition values can be defined by command line, for subsequent nodes partition values are passed on from previous nodes.
 */
case class FailIfNoPartitionValuesMode() extends ExecutionMode {
  override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    // check if partition values present
    if (subFeed.partitionValues.isEmpty && !context.appConfig.isDryRun) throw new IllegalStateException(s"($actionId) Partition values are empty for mainInput ${subFeed.dataObjectId.id}")
    // return: no change of given partition values and filter
    None
  }

  override def factory: FromConfigFactory[ExecutionMode] = FailIfNoPartitionValuesMode
}

object FailIfNoPartitionValuesMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FailIfNoPartitionValuesMode = {
    extract[FailIfNoPartitionValuesMode](config)
  }
}

/**
 * An execution mode which forces processing all data from it's inputs.
 */
case class ProcessAllMode() extends ExecutionMode {
  override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    // return: reset given partition values and filter
    logger.info(s"($actionId) ProcessModeAll reset partition values")
    Some(ExecutionModeResult())
  }
  override def factory: FromConfigFactory[ExecutionMode] = ProcessAllMode
}

object ProcessAllMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ProcessAllMode = {
    extract[ProcessAllMode](config)
  }
}

/**
 * Result of execution mode application
 */
case class ExecutionModeResult( inputPartitionValues: Seq[PartitionValues] = Seq(), outputPartitionValues: Seq[PartitionValues] = Seq()
                                , filter: Option[String] = None, fileRefs: Option[Seq[FileRef]] = None, options: Map[String,String] = Map())

/**
 * Attributes definition for spark expressions used as ExecutionMode conditions.
 * @param givenPartitionValues Partition values specified with command line (start action) or passed from previous action
 * @param isStartNode True if the current action is a start node of the DAG.
 */
case class DefaultExecutionModeExpressionData( feed: String, application: String, runId: Int, attemptId: Int, referenceTimestamp: Option[Timestamp]
                                             , runStartTime: Timestamp, attemptStartTime: Timestamp
                                             , givenPartitionValues: Seq[Map[String,String]], isStartNode: Boolean) {
  override def toString: String = ProductUtil.formatObj(this)
}
object DefaultExecutionModeExpressionData {
  def from(context: ActionPipelineContext): DefaultExecutionModeExpressionData = {
    DefaultExecutionModeExpressionData(context.feed, context.application, context.executionId.runId, context.executionId.attemptId, context.referenceTimestamp.map(Timestamp.valueOf)
      , Timestamp.valueOf(context.runStartTime), Timestamp.valueOf(context.attemptStartTime), Seq(), isStartNode = false)
  }
}

case class ExecutionModeFailedException(id: NodeId, phase: ExecutionPhase, msg: String) extends DAGException(msg) {
  // don't fail in init phase, but skip action to continue with exec phase
  override val severity: ExceptionSeverity = if (phase == ExecutionPhase.Init) ExceptionSeverity.FAILED_DONT_STOP else ExceptionSeverity.FAILED
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}
