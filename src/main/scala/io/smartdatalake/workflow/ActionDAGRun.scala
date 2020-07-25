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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DAGHelper._
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{Action, RuntimeEventState, RuntimeInfo}
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration._

private[smartdatalake] case class ActionDAGEdge(override val nodeIdFrom: NodeId, override val nodeIdTo: NodeId, override val resultId: String) extends DAGEdge

private[smartdatalake] case class InitDAGNode(override val nodeId: NodeId, edges: Seq[String]) extends DAGNode {
  override def toString: NodeId = nodeId // this is displayed in ascii graph visualization
}

private[smartdatalake] case class DummyDAGResult(override val resultId: String) extends DAGResult

private[smartdatalake] class ActionEventListener(phase: ExecutionPhase) extends DAGEventListener[Action] with SmartDataLakeLogger {
  override def onNodeStart(node: Action): Unit = {
    node.addRuntimeEvent(phase, RuntimeEventState.STARTED)
    logger.info(s"(${node.id}): $phase started")
  }
  override def onNodeSuccess(results: Seq[DAGResult])(node: Action): Unit = {
    node.addRuntimeEvent(phase, RuntimeEventState.SUCCEEDED)
    logger.info(s"(${node.id}): $phase: succeeded")
  }
  override def onNodeFailure(exception: Throwable)(node: Action): Unit = {
    node.addRuntimeEvent(phase, RuntimeEventState.FAILED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
    logger.error(s"(${node.id}): $phase failed with ${exception.getClass.getSimpleName}: ${exception.getMessage}")
  }
  override def onNodeSkipped(exception: Throwable)(node: Action): Unit = {
    node.addRuntimeEvent(phase, RuntimeEventState.SKIPPED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
    logger.warn(s"(${node.id}): $phase: skipped because of ${exception.getClass.getSimpleName}: ${exception.getMessage}")
  }
}

private[smartdatalake] trait ActionMetrics {
  def getId: String
  def getOrder: Long
  def getMainInfos: Map[String,Any]
  def getAsText: String
}

private[smartdatalake] case class ActionDAGRun(dag: DAG[Action], runId: Int, attemptId: Int, partitionValues: Seq[PartitionValues], parallelism: Int, initialSubFeeds: Seq[SubFeed], stateStore: Option[ActionDAGRunStateStore[_]]) extends SmartDataLakeLogger {

  private def createScheduler(parallelism: Int = 1) = Scheduler.fixedPool(s"dag-$runId", parallelism)

  private def run[R<:DAGResult](phase: ExecutionPhase, parallelism: Int = 1)(operation: (DAGNode, Seq[R]) => Seq[R])(implicit session: SparkSession, context: ActionPipelineContext): Seq[R] = {
    implicit val scheduler: SchedulerService = createScheduler(parallelism)
    val task = dag.buildTaskGraph[R](new ActionEventListener(phase))(operation)
    val futureResult = task.runToFuture(scheduler)

    // wait for result
    val result = Await.result(futureResult, Duration.Inf)
    scheduler.shutdown

    // collect all root exceptions
    val dagExceptions = result.filter(_.isFailure).map(_.failed.get).flatMap {
      case ex: DAGException => ex.getDAGRootExceptions
      case ex => throw ex // this should not happen
    }
    val dagExceptionsToStop = dagExceptions.filter(_.severity <= ExceptionSeverity.SKIPPED)
    // log all exceptions
    dagExceptions.foreach {
      case ex if (ex.severity <= ExceptionSeverity.CANCELLED) => logger.error(s"$phase: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
      case ex => logger.warn(s"$phase: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
    }
    // log dag on error
    if (dagExceptionsToStop.nonEmpty) ActionDAGRun.logDag(s"$phase failed for ${context.application} runId=$runId attemptId=$attemptId", dag)
    // throw most severe exception
    dagExceptionsToStop.sortBy(_.severity).foreach{ throw _ }

    // extract & return subfeeds
    result.filter(_.isSuccess).map(_.get)
  }

  def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    // run prepare for every node
    context.phase = ExecutionPhase.Prepare
    run[DummyDAGResult](context.phase, parallelism) {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => DummyDAGResult(dataObjectId))
      case (node: Action, _) =>
        node.prepare
        node.outputs.map(outputDO => DummyDAGResult(outputDO.id.id))
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }
  }

  def init(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    // run init for every node
    context.phase = ExecutionPhase.Init
    val t = run[SubFeed](context.phase) {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => getInitialSubFeed(dataObjectId))
      case (node: Action, subFeeds) =>
        node.init(subFeeds)
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }
    t
  }

  def exec(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    // run exec for every node
    val stageMetricsListener = new SparkStageMetricsListener(notifyActionMetric)
    session.sparkContext.addSparkListener(stageMetricsListener)
    val result = try {
      context.phase = ExecutionPhase.Exec
      run[SubFeed](context.phase) {
        case (node: InitDAGNode, _) =>
          node.edges.map(dataObjectId => getInitialSubFeed(dataObjectId))
        case (node: Action, subFeeds) =>
          node.preExec(subFeeds)
          val resultSubFeeds = node.exec(subFeeds)
          node.postExec(subFeeds, resultSubFeeds)
          //return
          resultSubFeeds
        case x => throw new IllegalStateException(s"Unmatched case $x")
      }
    } finally {
      session.sparkContext.removeSparkListener(stageMetricsListener)
    }
    // log dag execution
    ActionDAGRun.logDag(s"exec result dag $runId", dag)

    // return
    result
  }

  private def getInitialSubFeed(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext) = {
    initialSubFeeds.find(_.dataObjectId==dataObjectId)
      .getOrElse(
        if (context.dryRun) throw new IllegalStateException(s"Initial subfeed for $dataObjectId missing for dry run.")
        else InitSubFeed(dataObjectId, partitionValues)
      )
  }

  /**
   * Collect runtime information for every action of the dag
   */
  def getRuntimeInfos: Map[ActionObjectId, RuntimeInfo] = {
    dag.getNodes.map( a => (a.id, a.getRuntimeInfo.getOrElse(RuntimeInfo(RuntimeEventState.PENDING)))).toMap
  }

  /**
   * Save state of dag to file
   */
  def saveState(isFinal: Boolean = false)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    stateStore.foreach(_.saveState(ActionDAGRunState(context.appConfig, runId, attemptId, getRuntimeInfos, isFinal)))
  }

  private class ActionEventListener(phase: ExecutionPhase)(implicit session: SparkSession, context: ActionPipelineContext) extends DAGEventListener[Action] with SmartDataLakeLogger {
    override def onNodeStart(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.STARTED)
      logger.info(s"${node.toStringShort}: $phase started")
    }
    override def onNodeSuccess(results: Seq[DAGResult])(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.SUCCEEDED, results = results.collect{ case x: SubFeed => x })
      logger.info(s"${node.toStringShort}: $phase: succeeded")
      if (phase==ExecutionPhase.Exec) saveState()
    }
    override def onNodeFailure(exception: Throwable)(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.FAILED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
      logger.error(s"${node.toStringShort}: $phase failed with ${exception.getClass.getSimpleName}: ${exception.getMessage}")
      if (phase==ExecutionPhase.Exec) saveState()
    }
    override def onNodeSkipped(exception: Throwable)(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.SKIPPED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
      logger.warn(s"${node.toStringShort}: $phase: skipped because of ${exception.getClass.getSimpleName}: ${exception.getMessage}")
      if (phase==ExecutionPhase.Exec) saveState()
    }
  }


  /**
   * Get Action count per RuntimeEventState
   */
  def getStatistics: Map[RuntimeEventState,Int] = {
    getRuntimeInfos.map(_._2.state).groupBy(identity).mapValues(_.size)
  }

  /**
   * Reset runtime state.
   * This is mainly used for testing.
   */
  def reset(): Unit = {
    dag.sortedNodes.collect { case n: Action => n }.foreach(_.reset)
  }

  def notifyActionMetric(actionId: ActionObjectId, dataObjectId: Option[DataObjectId], metrics: ActionMetrics): Unit = {
    val action = dag.getNodes
      .find(_.nodeId == actionId.id).getOrElse(throw new IllegalStateException(s"Unknown action $actionId"))
    action.onRuntimeMetrics(dataObjectId, metrics)
  }
}

object ExecutionPhase extends Enumeration {
  type ExecutionPhase = Value
  val Prepare, Init, Exec = Value
}

private[smartdatalake] object ActionDAGRun extends SmartDataLakeLogger {

  /**
   * create ActionDAGRun
   */
  def apply(actions: Seq[Action], runId: Int, attemptId: Int, partitionValues: Seq[PartitionValues] = Seq(), parallelism: Int = 1, initialSubFeeds: Seq[SubFeed] = Seq(), stateStore: Option[ActionDAGRunStateStore[_]] = None)(implicit session: SparkSession, context: ActionPipelineContext): ActionDAGRun = {

    // prepare edges: list of actions dependencies
    // this can be created by combining input and output ids between actions
    val nodeInputs = actions.map( action => (action.id, action.inputs.map(_.id)))
    val outputsToNodeMap = actions.flatMap( action => action.outputs.map( output => (output.id, action.id))).toMap
    val allEdges = nodeInputs.flatMap{ case (nodeIdTo, inputIds) => inputIds.map( inputId => (outputsToNodeMap.get(inputId), nodeIdTo, inputId))}

    // test
    val duplicateEdges = allEdges.groupBy(identity).mapValues(_.size).filter(_._2 > 1).keys
    assert( duplicateEdges.isEmpty, s"Duplicate edges found: ${duplicateEdges}" )

    // create init node from input edges
    val inputEdges = allEdges.filter(_._1.isEmpty)
    logger.info(s"input edges are $inputEdges")
    val initNodeId = "init"
    val initAction = InitDAGNode(initNodeId, inputEdges.map(_._3.id))
    val edges = allEdges.map{ case (nodeIdFromOpt, nodeIdTo, resultId) => ActionDAGEdge(nodeIdFromOpt.map(_.id).getOrElse(initNodeId), nodeIdTo.id, resultId.id)}

    // create dag
    val dag = DAG.create[Action](initAction +: actions, edges)
    logDag(s"created dag runId=$runId attemptId=$attemptId", dag)

    // enable runtime metrics
    actions.foreach(_.enableRuntimeMetrics())

    ActionDAGRun(dag, runId, attemptId, partitionValues, parallelism, initialSubFeeds, stateStore)
  }

  def logDag(msg: String, dag: DAG[_]): Unit = {
    logger.info(s"""$msg:
                   |${dag.toString}
    """.stripMargin)
  }
}
