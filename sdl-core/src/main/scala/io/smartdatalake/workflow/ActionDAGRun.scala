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

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DAGHelper._
import io.smartdatalake.workflow.ExecutionPhase.{ExecutionPhase, Value}
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{Action, RuntimeEventState, RuntimeInfo, SparkSubFeedsAction}
import io.smartdatalake.workflow.dataobject.{CanHandlePartitions, DataObject, TransactionalSparkTableDataObject}
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

private[smartdatalake] trait ActionMetrics {
  def getId: String
  def getOrder: Long
  def getMainInfos: Map[String,Any]
  def getAsText: String
}

private[smartdatalake] case class GenericMetrics(id: String, order: Long, mainInfos: Map[String, Any]) extends ActionMetrics {
  def getId: String = id
  def getOrder: Long = order
  def getMainInfos: Map[String, Any] = mainInfos
  def getAsText: String = {
    mainInfos.map{ case (k,v) => s"$k=$v" }.mkString(" ")
  }
}

private[smartdatalake] case class ActionDAGRun(dag: DAG[Action], runId: Int, attemptId: Int, partitionValues: Seq[PartitionValues], parallelism: Int, initialSubFeeds: Seq[SubFeed], stateStore: Option[ActionDAGRunStateStore[_]], stateListeners: Seq[StateListener]) extends SmartDataLakeLogger {

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
    // only stop on skipped in init phase if there are no succeeded results, otherwise we want to have those actions run.
    lazy val existsInitializedActions = dag.getNodes.exists(_.getLatestRuntimeState.contains(RuntimeEventState.INITIALIZED))
    val stopSeverity = if (phase == ExecutionPhase.Init && existsInitializedActions) ExceptionSeverity.CANCELLED else ExceptionSeverity.SKIPPED
    val dagExceptionsToStop = dagExceptions.filter(_.severity <= stopSeverity).sortBy(_.severity)
    // log all exceptions
    dagExceptions.distinct.foreach {
      case ex if ex.severity <= ExceptionSeverity.FAILED_DONT_STOP => logger.error(s"$phase: ${ex.getClass.getSimpleName}: ${ex.getMessageWithCause}")
      case ex => logger.warn(s"$phase: ${ex.getClass.getSimpleName}: ${ex.getMessageWithCause}")
    }
    // log dag on error
    if (dagExceptionsToStop.nonEmpty) ActionDAGRun.logDag(s"$phase ${dagExceptionsToStop.head.severity} for ${context.application} runId=$runId attemptId=$attemptId", dag)
    // throw most severe exception
    dagExceptionsToStop.foreach{ throw _ }

    // extract & return subfeeds
    result.filter(_.isSuccess).map(_.get)
  }

  def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    context.phase = ExecutionPhase.Prepare
    // run prepare for every node
    run[DummyDAGResult](context.phase) {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => DummyDAGResult(dataObjectId))
      case (node: Action, _) =>
        node.prepare
        node.outputs.map(outputDO => DummyDAGResult(outputDO.id.id))
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }
  }

  def init(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    context.phase = ExecutionPhase.Init
    // initialize state listeners
    stateListeners.foreach(_.init)
    // run init for every node
    val t = run[SubFeed](context.phase) {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => getInitialSubFeed(dataObjectId))
      case (node: Action, subFeeds) =>
        val deduplicatedSubFeeds = unionDuplicateSubFeeds(subFeeds ++ getRecursiveSubFeeds(node), node.id)
        node.preInit(deduplicatedSubFeeds)
        node.init(deduplicatedSubFeeds)
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }
    t
  }

  private def unionDuplicateSubFeeds(subFeeds: Seq[SubFeed], actionId: ActionId)(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    subFeeds.groupBy(_.dataObjectId).mapValues {
      subFeeds =>
        if (subFeeds.size > 1) {
          logger.info(s"($actionId) Creating union of multiple SubFeeds as input for ${subFeeds.head.dataObjectId}")
          subFeeds.reduce((s1, s2) => s1.union(s2))
        } else subFeeds.head
    }.values.toSeq
  }

  def exec(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    // run exec for every node
    val stageMetricsListener = new SparkStageMetricsListener(notifyActionMetric)
    session.sparkContext.addSparkListener(stageMetricsListener)
    val result = try {
      context.phase = ExecutionPhase.Exec
      run[SubFeed](context.phase, parallelism) {
        case (node: InitDAGNode, _) =>
          node.edges.map(dataObjectId => getInitialSubFeed(dataObjectId))
        case (node: Action, subFeeds) =>
          val deduplicatedSubFeeds = unionDuplicateSubFeeds(subFeeds ++ getRecursiveSubFeeds(node), node.id)
          node.preExec(deduplicatedSubFeeds)
          val resultSubFeeds = node.exec(deduplicatedSubFeeds)
          node.postExec(deduplicatedSubFeeds, resultSubFeeds)
          //return
          resultSubFeeds
        case x => throw new IllegalStateException(s"Unmatched case $x")
      }
    } finally {
      session.sparkContext.removeSparkListener(stageMetricsListener)
    }
    // log dag execution
    ActionDAGRun.logDag(s"exec SUCCEEDED for dag $runId", dag)

    // return
    result
  }

  private def getRecursiveSubFeeds(node: Action)(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(node.recursiveInputs.map(_.isInstanceOf[TransactionalSparkTableDataObject]).forall(_==true), "Recursive inputs only work for TransactionalSparkTableDataObjects.")
    // recursive inputs are only passed as input SubFeeds for SparkSubFeedsAction, which can take more than one input SubFeed
    // for SparkSubFeedAction (e.g. Deduplicate and HistorizeAction) which implicitly use a recursive input, the Action is responsible the get the SubFeed.
    node match {
      case _: SparkSubFeedsAction => node.recursiveInputs.map(dataObject => getInitialSubFeed(dataObject.id))
      case _ => Seq()
    }
  }

  private def getInitialSubFeed(dataObjectId: DataObjectId)(implicit session: SparkSession, context: ActionPipelineContext) = {
    initialSubFeeds.find(_.dataObjectId==dataObjectId)
      .getOrElse {
        val partitions = context.instanceRegistry.get[DataObject](dataObjectId) match {
          case dataObject: CanHandlePartitions => dataObject.partitions
          case _ => Seq()
        }
        if (context.simulation) throw new IllegalStateException(s"Initial subfeed for $dataObjectId missing for dry run.")
        else InitSubFeed(dataObjectId, partitionValues).updatePartitionValues(partitions, breakLineageOnChange = false)
      }
  }

  /**
   * Collect runtime information for every action of the dag
   */
  def getRuntimeInfos: Map[ActionId, RuntimeInfo] = {
    dag.getNodes.map( a => (a.id, a.getRuntimeInfo.getOrElse(RuntimeInfo(RuntimeEventState.PENDING)))).toMap
  }

  /**
   * Save state of dag to file and notify stateListeners
   */
  def saveState(isFinal: Boolean = false)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    val runState = ActionDAGRunState(context.appConfig, runId, attemptId, context.runStartTime, context.attemptStartTime, getRuntimeInfos, isFinal)
    stateStore.foreach(_.saveState(runState))
    stateListeners.foreach(_.notifyState(runState, context))
  }

  private class ActionEventListener(phase: ExecutionPhase)(implicit session: SparkSession, context: ActionPipelineContext) extends DAGEventListener[Action] with SmartDataLakeLogger {
    override def onNodeStart(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.STARTED)
      logger.info(s"${node.toStringShort}: $phase started")
    }
    override def onNodeSuccess(results: Seq[DAGResult])(node: Action): Unit = {
      val state = phase match {
        case ExecutionPhase.Prepare => RuntimeEventState.PREPARED
        case ExecutionPhase.Init => RuntimeEventState.INITIALIZED
        case ExecutionPhase.Exec => RuntimeEventState.SUCCEEDED
      }
      node.addRuntimeEvent(phase, state, results = results.collect{ case x: SubFeed => x })
      logger.info(s"${node.toStringShort}: $phase succeeded")
      if (phase==ExecutionPhase.Exec) saveState()
    }
    override def onNodeFailure(exception: Throwable)(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.FAILED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
      logger.warn(s"${node.toStringShort}: $phase failed with ${exception.getClass.getSimpleName}: ${exception.getMessage}")
      if (phase==ExecutionPhase.Exec) saveState()
    }
    override def onNodeSkipped(exception: Throwable)(node: Action): Unit = {
      node.addRuntimeEvent(phase, RuntimeEventState.SKIPPED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
      logger.info(s"${node.toStringShort}: $phase skipped because of ${exception.getClass.getSimpleName}: ${exception.getMessage}")
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

  def notifyActionMetric(actionId: ActionId, dataObjectId: Option[DataObjectId], metrics: ActionMetrics): Unit = {
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
  def apply(actions: Seq[Action], runId: Int, attemptId: Int, partitionValues: Seq[PartitionValues] = Seq(), parallelism: Int = 1, initialSubFeeds: Seq[SubFeed] = Seq(), stateStore: Option[ActionDAGRunStateStore[_]] = None, stateListeners: Seq[StateListener] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): ActionDAGRun = {

    // prepare edges: list of actions dependencies
    // this can be created by combining input and output ids between actions
    val nodeInputs = actions.map( action => (action.id, action.inputs.map(_.id)))
    val outputsToNodeMap = actions.flatMap( action => action.outputs.map( output => (output.id, action.id)))
      .groupBy(_._1).mapValues(_.map(_._2))
    val allEdges = nodeInputs.flatMap{ case (nodeIdTo, inputIds) => inputIds.flatMap( inputId => outputsToNodeMap.getOrElse(inputId,Seq()).map(action => (Some(action), nodeIdTo, inputId)))} ++
      nodeInputs.flatMap{ case (nodeIdTo, inputIds) => inputIds.filter( inputId => !outputsToNodeMap.contains(inputId)).map( inputId => (None, nodeIdTo, inputId))}

    // test
    val duplicateEdges = allEdges.groupBy(identity).mapValues(_.size).filter(_._2 > 1).keys
    assert( duplicateEdges.isEmpty, s"Duplicate edges found: ${duplicateEdges}" )

    // create init node from input edges
    val inputEdges = allEdges.filter(_._1.isEmpty)
    logger.info(s"input edges are $inputEdges")
    val initNodeId = "start"
    val initAction = InitDAGNode(initNodeId, inputEdges.map(_._3.id))
    val edges = allEdges.map{ case (nodeIdFromOpt, nodeIdTo, resultId) => ActionDAGEdge(nodeIdFromOpt.map(_.id).getOrElse(initNodeId), nodeIdTo.id, resultId.id)}

    // create dag
    val dag = DAG.create[Action](initAction +: actions, edges)
    logDag(s"created dag runId=$runId attemptId=$attemptId", dag)

    // enable runtime metrics
    actions.foreach(_.enableRuntimeMetrics())

    ActionDAGRun(dag, runId, attemptId, partitionValues, parallelism, initialSubFeeds, stateStore, stateListeners)
  }

  def logDag(msg: String, dag: DAG[_]): Unit = {
    logger.info(s"""$msg:
                   |${dag.toString}
    """.stripMargin)
  }
}
