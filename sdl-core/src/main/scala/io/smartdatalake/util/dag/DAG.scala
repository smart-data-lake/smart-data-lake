/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.dag

import com.github.mdr.ascii.graph.Graph
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import monix.eval.Task
import monix.execution.Scheduler

import scala.annotation.tailrec
import scala.reflect._
import scala.util.{Failure, Success, Try}

private[smartdatalake] object DAGHelper {
  type NodeId = String
}

private[smartdatalake] trait DAGNode {
  def nodeId: NodeId
}

private[smartdatalake] trait DAGEdge {
  def nodeIdFrom: NodeId
  def nodeIdTo: NodeId
  def resultId: String
}

private[smartdatalake] trait DAGResult {
  def resultId: String
}

private[smartdatalake] trait DAGEventListener[T <: DAGNode] {
  def onNodeStart(node: T)
  def onNodeSuccess(results: Seq[DAGResult])(node: T)
  def onNodeFailure(exception: Throwable)(node: T)
  def onNodeSkipped(exception: Throwable)(node: T)
}

/**
 * A generic directed acyclic graph (DAG) consisting of [[DAGNode]]s interconnected with directed [[DAGEdge]]s.
 *
 * This DAG can have multiple start nodes and multiple end nodes as well as disconnected parts.
 *
 * @param sortedNodes All nodes of the DAG sorted in topological order.
 * @param incomingEdgesMap A lookup table for incoming edges indexed by node id.
 * @param startNodes Starting points for DAG execution.
 * @param endNodes End points for DAG execution.
 * @tparam N: This is the DAG's main node type used for notifying the event listener. There may be other node types in the DAG for technical reasons, e.g. initialization
 */
case class DAG[N <: DAGNode : ClassTag] private(sortedNodes: Seq[DAGNode],
                                                incomingEdgesMap: Map[NodeId, Seq[DAGEdge]],
                                                startNodes: Seq[DAGNode],
                                                endNodes: Seq[DAGNode]
                                               )
  extends SmartDataLakeLogger {

  /**
   * Create text representation of the graph by using an ASCII graph layout library
   */
  def render(nodeToString: DAGNode => String): String = {
    val nodesLookup = sortedNodes.map( n => n.nodeId -> n).toMap
    val edges = incomingEdgesMap.values.flatMap {
      incomingEdges => incomingEdges.map(incomingEdge => (nodesLookup(incomingEdge.nodeIdFrom), nodesLookup(incomingEdge.nodeIdTo)))
    }
    val g = new Graph(vertices = sortedNodes.toSet, edges = edges.toList)
    val renderingStrategy = new NodeRenderingStrategy(nodeToString)
    GraphLayout.renderGraph(g, renderingStrategy)
  }

  /**
   * Build a single task that is a combination of node computations (node tasks) executed in the topological order
   * defined by the DAG.
   *
   * Monix tasks is a library for lazy cancelable futures
   *
   * @note This method does not trigger any execution but builds a complex collection of tasks and synchronization
   *       boundaries that specify a correct order of execution as defined by the DAG.
   *       The computation only runs when the returning task is scheduled for execution.
   *
   * @see https://medium.com/@sderosiaux/are-scala-futures-the-past-69bd62b9c001
   *
   * @param eventListener A instance of [[DAGEventListener]] to be notified about progress of DAG execution
   * @param operation A function that computes the result ([[DAGResult]]) for the current node,
   *                  given the result of its predecessors given.
   * @param scheduler The [[Scheduler]] to use for Tasks.
   *
   * @return
   */
  def buildTaskGraph[A <: DAGResult](eventListener: DAGEventListener[N])
                                    (operation: (DAGNode, Seq[A]) => Seq[A])
                                    (implicit scheduler: Scheduler): Task[Seq[Try[A]]] = {

    // this variable is used to stop execution on cancellation
    // using a local variable inside code of Futures is possible in Scala:
    // https://stackoverflow.com/questions/51690555/scala-treats-sharing-local-variables-in-threading-differently-from-java-how-d
    var isCancelled = false

    // get input tasks for each edge, combine them, execute operation as future task and remember it
    // this works because nodes are sorted and previous tasks therefore are prepared earlier
    val allTasksMap = sortedNodes.foldLeft(Map.empty[NodeId, Task[Try[Seq[A]]]]) {
      case (tasksAcc, node) =>
        val incomingResultsTask = collectIncomingNodeResults(tasksAcc, node)
        val currentNodeTask = incomingResultsTask.map {
          incomingResults =>
            // Now the incoming results have finished computing.
            // It's time to compute the result of the current node.
            if (isCancelled) {
              cancelledDAGResult(node, eventListener)
            } else if (incomingResults.exists(_.isFailure)) {
              // a predecessor failed
              incomingFailedResult(node, incomingResults, eventListener)
            } else {
              // compute the result for this node
              computeNodeOperationResult(node, operation, incomingResults, eventListener)
            }
        }.memoize // calculate only once per node, then remember value
        // pass on the result of the current node
        tasksAcc + (node.nodeId -> currentNodeTask)
    }

    // prepare final task (Future) by combining all (independent) endNodes in the DAG
    val endTasks = endNodes.map(n => allTasksMap(n.nodeId))
    // wait for all end tasks to complete, then return a sequence
    val flattenedResult = Task.gatherUnordered(endTasks).map(_.flatMap(trySeqToSeqTry))
    flattenedResult
      .memoize
      .doOnCancel(Task {
        logger.info("DAG execution is cancelled")
        isCancelled = true
      })
  }

  private def computeNodeOperationResult[A <: DAGResult](node: DAGNode, operation: (DAGNode, Seq[A]) => Seq[A], incomingResults: Seq[Try[A]], eventListener: DAGEventListener[N]): Try[Seq[A]] = {
    notify(node, eventListener.onNodeStart)
    val resultRaw = Try(operation(node, incomingResults.map(_.get))) // or should we use "Try( blocking { operation(node, v) })"
    val result = resultRaw match {
      case Success(r) =>
        notify(node, eventListener.onNodeSuccess(r))
        resultRaw
      case Failure(ex: TaskSkippedDontStopWarning[A @unchecked]) if ex.getResults.isDefined =>
        // Current Node is skipped, but further actions should run -> convert the failure into a success with fake results delivered by the exception
        // notify that task is skipped
        notify(node, eventListener.onNodeSkipped(ex))
        Success(ex.getResults.get)
      case Failure(ex: DAGException) if ex.severity >= ExceptionSeverity.SKIPPED =>
        // if severity is low, notify that task is skipped
        notify(node, eventListener.onNodeSkipped(ex))
        resultRaw
      case Failure(ex) =>
        // pass Failure for all other exceptions
        notify(node, eventListener.onNodeFailure(ex))
        Failure(TaskFailedException(node.nodeId, ex))
    }
    // return
    result
  }

  private def incomingFailedResult[A <: DAGResult](node: DAGNode, incomingResults: Seq[Try[A]], eventListener: DAGEventListener[N]): Try[Seq[A]] = {
    val predecessorExceptions = incomingResults.filter(_.isFailure).map(f => f.failed.get).map{
      case ex: DAGException => ex
      case ex => throw ex // this should not happen
    }
    val mostSeverePredecessorException = predecessorExceptions.minBy(_.severity)
    logger.debug(s"Task ${node.nodeId} is not executed because some predecessor had error $predecessorExceptions")
    val exception = TaskPredecessorFailureWarning(node.nodeId, mostSeverePredecessorException, predecessorExceptions)
    notify(node, eventListener.onNodeSkipped(exception))
    Failure(exception)
  }

  private def cancelledDAGResult[A <: DAGResult](node: DAGNode, eventListener: DAGEventListener[N]): Try[Seq[A]] = {
    logger.debug(s"Task ${node.nodeId} is cancelled because DAG execution is cancelled")
    val exception = TaskCancelledException(node.nodeId)
    notify(node, eventListener.onNodeSkipped(exception))
    Failure(exception)
  }

  private def notify(node: DAGNode, notifyFunc: N => Unit): Unit = {
    // we can only notify the event listener, if node is of main DAGNode type N
    // because of type erasure for generics we have to use reflection
    if (classTag[N].runtimeClass.isInstance(node)) notifyFunc(node.asInstanceOf[N])
  }

  /**
   * Create Tasks that computes incoming results in parallel and waits for the of the incoming tasks to finish.
   *
   * @return The results of the incoming tasks.
   */
  private def collectIncomingNodeResults[A <: DAGResult](tasksAcc: Map[NodeId, Task[Try[Seq[A]]]], node: DAGNode): Task[Seq[Try[A]]] = {
    val incomingTasks = incomingEdgesMap.getOrElse(node.nodeId, Seq.empty) map { incomingEdge =>
      getResultTask(tasksAcc, incomingEdge.nodeIdFrom, incomingEdge.resultId)
    }
    // Wait for results from incoming tasks to be computed and return their results
    Task.gatherUnordered(incomingTasks)
  }

  /**
   * Convert a [[Try]] of a Result-List to a List of Result-[[Try]]'s
   */
  def trySeqToSeqTry[A](trySeq: Try[Seq[A]]): Seq[Try[A]] = trySeq match {
    case Success(result) => result.map(result => Success(result))
    case Failure(ex) => Seq(Failure(ex))
  }

  /**
   * Create a task that fetches a specific [[DAGResult]] produced by the node with id `nodeId`.
   *
   * @param tasks A map of tasks that compute (future) results indexed by node.
   * @param nodeId The id of the producing node.
   * @param resultId The id of the result to search among all nodes results.
   * @tparam A The result type - supertype [[DAGResult]] ensures it has a resultId defined.
   * @return The task that computes the result specified by `nodeId` and `resultId`.
   */
  def getResultTask[A <: DAGResult](tasks: Map[NodeId, Task[Try[Seq[A]]]], nodeId: NodeId, resultId: String): Task[Try[A]] = {
    //look for already computed results of the node
    val nodeResults = tasks(nodeId)
    nodeResults map {
      resultTry => resultTry.map {
        result => result.find( _.resultId == resultId).getOrElse(throw new IllegalStateException(s"Result for incoming edge $nodeId, $resultId not found"))
      }
    }
  }

  /**
   * Get the nodes of this DAG
   * Note: we must filter nodes which are not of the preliminary node type of this DAG, e.g. InitDAGNode
   *
   * @return list of nodes of the preliminary node type of this DAG
   */
  def getNodes: Seq[N] = sortedNodes.collect { case n: N => n }
}

object DAG extends SmartDataLakeLogger {

  /**
   * Create a DAG object from DAGNodes and DAGEdges
   */
  def create[N <: DAGNode : ClassTag](nodes: Seq[DAGNode], edges: Seq[DAGEdge]): DAG[N] = {
    val incomingIds: Map[DAGNode, Seq[NodeId]] = buildIncomingIdLookupTable(nodes, edges)
    // start nodes = all nodes without incoming edges
    val startNodes = incomingIds.filter(_._2.isEmpty)

    // end nodes = all nodes without outgoing edges
    val endNodes = buildOutgoingIdLookupTable(nodes, edges).filter(_._2.isEmpty)

    // sort node IDs topologically and check there are no loops
    val sortedNodeIds = sortStep(incomingIds.map {
      case (node, inIds) => (node.nodeId, inIds)
    })
    logger.info(s"DAG node order is: ${sortedNodeIds.mkString(" -> ")}")

    //lookup table to retrieve nodes by their ID
    val nodeIdsToNodeMap = nodes.map(n => (n.nodeId, n)).toMap
    val sortedNodes = sortedNodeIds.map(nodeIdsToNodeMap)

    //lookup table to retrieve edges by ID pairs (fromId, toId)
    val edgeIdPairToEdgeMap = edges.groupBy(e => (e.nodeIdFrom, e.nodeIdTo))

    val incomingEdgesMap = incomingIds.map {
      case (node, incomingIds) => (node.nodeId, incomingIds.flatMap(incomingId => edgeIdPairToEdgeMap(incomingId,node.nodeId)))
    }

    DAG(sortedNodes, incomingEdgesMap, startNodes.keys.toSeq, endNodes.keys.toSeq)
  }

  /**
   * Create a lookup table to retrieve outgoing (target) node IDs for a node.
   */
  private def buildOutgoingIdLookupTable(nodes: Seq[DAGNode], edges: Seq[DAGEdge]): Map[DAGNode, Seq[NodeId]] = {
    val targetIDsforIncomingIDsMap = edges.groupBy(_.nodeIdFrom).mapValues(_.map(_.nodeIdTo))
    nodes.map(n => (n, targetIDsforIncomingIDsMap.getOrElse(n.nodeId, Seq()))).toMap
  }

  /**
   * Create a lookup table to retrieve incoming (source) node IDs for a node.
   */
  private def buildIncomingIdLookupTable(nodes: Seq[DAGNode], edges: Seq[DAGEdge]): Map[DAGNode, Seq[NodeId]] = {
    val incomingIDsForTargetIDMap = edges.groupBy(_.nodeIdTo).mapValues(_.map(_.nodeIdFrom).distinct)
    nodes.map(n => (n, incomingIDsForTargetIDMap.getOrElse(n.nodeId, Seq.empty))).toMap
  }

  /**
   * Sort Graph in topological order.
   *
   * Sort by recursively searching the start nodes and removing them for the next call.
   */
  private def sortStep(incomingIds: Map[NodeId, Seq[NodeId]]): Seq[NodeId] = {
    @tailrec
    def go(sortedNodes: Seq[NodeId], incomingIds: Map[NodeId, Seq[NodeId]]): Seq[NodeId] = {
      // search start nodes = nodes without incoming nodes
      val (startNodeIds, nonStartNodeIds) = incomingIds.partition(_._2.isEmpty)
      assert(startNodeIds.nonEmpty, s"Loop detected in remaining nodes ${incomingIds.keys.mkString(", ")}")
      // remove start nodes from incoming node list of remaining nodes
      val nonStartNodeIdsWithoutIncomingStartNodes: Map[NodeId, Seq[NodeId]] = nonStartNodeIds.mapValues(_.filterNot(startNodeIds.isDefinedAt))
      val newSortedNotes = sortedNodes ++ startNodeIds.keys
      if (nonStartNodeIdsWithoutIncomingStartNodes.isEmpty) newSortedNotes
      else go(newSortedNotes, nonStartNodeIdsWithoutIncomingStartNodes)
    }
    go(Seq(),incomingIds)
  }

}
