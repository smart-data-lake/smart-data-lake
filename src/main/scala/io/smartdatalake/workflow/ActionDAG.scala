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

import io.smartdatalake.metrics.StageMetricsListener
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DAGHelper._
import io.smartdatalake.workflow.action.{Action, RuntimeEventState}
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

private[smartdatalake] class ActionEventListener(operation: String) extends DAGEventListener[Action] with SmartDataLakeLogger {
  override def onNodeStart(node: Action): Unit = {
    node.addRuntimeEvent(operation, RuntimeEventState.STARTED, "-")
    logger.info(s"${node.toStringShort}: $operation started")
  }
  override def onNodeSuccess(node: Action): Unit = {
    node.addRuntimeEvent(operation, RuntimeEventState.SUCCEEDED, "-")
    logger.info(s"${node.toStringShort}: $operation: succeeded")
  }
  override def onNodeFailure(exception: Throwable)(node: Action): Unit = {
    node.addRuntimeEvent(operation, RuntimeEventState.FAILED, s"${exception.getClass.getSimpleName}: ${exception.getMessage}")
    logger.error(s"${node.toStringShort}: $operation failed with")
  }
  override def onNodeSkipped(exception: Throwable)(node: Action): Unit = {
    node.addRuntimeEvent(operation, RuntimeEventState.SKIPPED, s"${exception.getClass.getSimpleName}: ${exception.getMessage}")
    logger.warn(s"${node.toStringShort}: $operation: skipped because of ${exception.getClass.getSimpleName}: ${exception.getMessage}")
  }
}

private[smartdatalake] case class ActionDAGRun(dag: DAG[Action], runId: String, partitionValues: Seq[PartitionValues], parallelism: Int) extends SmartDataLakeLogger {

  private def createScheduler(parallelism: Int = 1) = Scheduler.fixedPool(s"dag-$runId", parallelism)

  private def run[R<:DAGResult](op: String, parallelism: Int = 1)(operation: (DAGNode, Seq[R]) => Seq[R])(implicit session: SparkSession, context: ActionPipelineContext): Seq[R] = {
    val stageMetricsListener = new StageMetricsListener
    session.sparkContext.addSparkListener(stageMetricsListener)
    try {
      implicit val scheduler: SchedulerService = createScheduler(parallelism)
      val task = dag.buildTaskGraph[R](new ActionEventListener(op))(operation)
      val futureResult = task.runToFuture(scheduler)

      // wait for result
      val result = Await.result(futureResult, Duration.Inf)
      scheduler.shutdown

      // collect all root exceptions
      val dagExceptions = result.filter(_.isFailure).map(_.failed.get).flatMap {
        case ex: DAGException => ex.getDAGRootExceptions
        case ex => throw ex // this should not happen
      }
      // log all exceptions
      dagExceptions.foreach {
        case ex if (ex.severity <= ExceptionSeverity.CANCELLED) => logger.error(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
        case ex => logger.warn(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
      }
      // log dag on error
      if (dagExceptions.nonEmpty) ActionDAGRun.logDag(s"$op failed for dag $runId", dag)
      // throw most severe exception
      dagExceptions.sortBy(_.severity).foreach{ throw _ }

      // extract & return subfeeds
      result.map(_.get)
    } finally {
      session.sparkContext.removeSparkListener(stageMetricsListener)
      if (stageMetricsListener.stageMetricsCollection.nonEmpty) {
        logger.info(s"Operation '$op' stage metrics:\n - ${stageMetricsListener.stageMetricsCollection.map(_.report()).mkString("\n - ")}")
      }
    }
  }

  def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    // run prepare for every node
    run[DummyDAGResult]("prepare", parallelism) {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => DummyDAGResult(dataObjectId))
      case (node: Action, empty) =>
        node.prepare
        node.outputs.map(outputDO => DummyDAGResult(outputDO.id.id))
    }
  }

  def init(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    // run init for every node
    run[SubFeed]("init") {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => InitSubFeed(dataObjectId, partitionValues))
      case (node: Action, subFeeds) =>
        node.init(subFeeds)
    }
  }

  def exec(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    // run exec for every node
    val result = run[SubFeed]("exec") {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => InitSubFeed(dataObjectId, partitionValues))
      case (node: Action, subFeeds) =>
        node.preExec
        val resultSubFeeds = node.exec(subFeeds)
        node.postExec(subFeeds, resultSubFeeds)
        //return
        resultSubFeeds
    }

    // log dag execution
    ActionDAGRun.logDag(s"exec result dag $runId", dag)

    // return
    result
  }
}

private[smartdatalake] object ActionDAGRun extends SmartDataLakeLogger {

  /**
   * create ActionDAGRun
   */
  def apply(actions: Seq[Action], runId: String, partitionValues: Seq[PartitionValues] = Seq(), parallelism: Int = 1)(implicit session: SparkSession, context: ActionPipelineContext): ActionDAGRun = {

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
    logDag(s"created dag $runId", dag)

    ActionDAGRun(dag, runId, partitionValues, parallelism)
  }

  def logDag(msg: String, dag: DAG[_]): Unit = {
    logger.info(s"""$msg:
                   |${dag.toString}
    """.stripMargin)
  }
}
