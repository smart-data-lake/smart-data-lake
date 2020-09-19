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

import java.util.concurrent.atomic.AtomicInteger

import io.smartdatalake.util.misc.PerformanceUtils.measureTime
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DAGHelper.NodeId
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

case class TestResult(override val resultId: String, path: String = "") extends DAGResult

case class TestNode(override val nodeId: NodeId) extends DAGNode {
  override def toString: NodeId = nodeId // this is displayed in ascii graph visualization
}

case class TestEgde(override val nodeIdFrom: NodeId, override val nodeIdTo: NodeId, override val resultId: String = TestEdge.defaultResultId) extends DAGEdge
object TestEdge {
  val defaultResultId = "-"
}

class TestEventListener extends DAGEventListener[TestNode] with SmartDataLakeLogger {
  // count how many nodes are currently running
  val concurrentRuns: AtomicInteger = new AtomicInteger(0)
  // record the maximum number of concurrently running noes
  val maxConcurrentRuns: AtomicInteger = new AtomicInteger(0)
  override def onNodeStart(node: TestNode): Unit = {
    concurrentRuns.incrementAndGet
    maxConcurrentRuns.set(Math.max(concurrentRuns.get,maxConcurrentRuns.get))
    logger.info(s"${node.nodeId} started (" + concurrentRuns.get +" job(s) running currently)") }
  override def onNodeFailure(exception: Throwable)(node: TestNode): Unit = { logger.error(s"${node.nodeId} failed with ${exception.getClass.getSimpleName}"); concurrentRuns.decrementAndGet }
  override def onNodeSkipped(exception: Throwable)(node: TestNode): Unit = logger.warn(s"${node.nodeId} skipped because ${exception.getClass.getSimpleName}")
  override def onNodeSuccess(result: Seq[DAGResult])(node: TestNode): Unit = { logger.info(s"${node.nodeId} succeeded"); concurrentRuns.decrementAndGet }
}

class DAGTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  before {
    execCntPerNode.clear
  }

  test("create and run dag: linear unordered") {
    val dag = DAG.create[TestNode](
      Seq(TestNode("A"), TestNode("C"), TestNode("B")),
      Seq(TestEgde("A", "B"),TestEgde("B", "C"))
    )
    println(dag.toString)
    val task = dag.buildTaskGraph[TestResult](new TestEventListener){ defaultOp(300) }
    val resultFuture = task.runToFuture
    val result = Await.result(resultFuture, 5.seconds)
      .map(_.get)
    println(result)
    assert(result.size==1)
    assert(result.head.path=="A-B-C")
  }

  test("create and run dag: split and join with parallel execution") {
    val nodes = Seq(TestNode("A"), TestNode("B"), TestNode("C"), TestNode("D"))
    val edges = Seq(TestEgde("A", "B"),TestEgde("B", "D"),TestEgde("A", "C"),TestEgde("C", "D"))
    val dag = DAG.create[TestNode](nodes, edges)
    println(dag.toString)
    val testEventListener: TestEventListener = new TestEventListener
    val task =  dag.buildTaskGraph[TestResult](testEventListener){ defaultOp() }
    val resultFuture = task.runToFuture
    val (result, tResult) = measureTime(
      Await.result(resultFuture, 10.seconds)
        .map(_.get)
    )
    println(result)
    assert(result.size==1)
    assert(result.head.path.endsWith("-D"))
    // check nodes are executed only once
    val nodesExecutedMoreThanOnce = execCntPerNode.filter{ case (_,cnt) => cnt.get > 1}
    assert(nodesExecutedMoreThanOnce.isEmpty, s"Nodes $nodesExecutedMoreThanOnce have been executed more than once")
    // check all nodes are executed
    val nodesNotExecuted = nodes.map(_.nodeId).toSet -- execCntPerNode.keySet
    assert(nodesNotExecuted.isEmpty, s"Nodes $nodesNotExecuted have not been executed")
    // check parallel execution, in this case it is not possible for more than 2 jobs to run concurrently
    logger.info("Maximum number of parallel executions was " +testEventListener.maxConcurrentRuns.get)
    assert(testEventListener.maxConcurrentRuns.get() == 2)
  }

  test("create and run dag: split and join with serialized execution") {
    val singleScheduler = Scheduler.fixedPool("fixed", 1) // scheduler with 1 thread serializes execution (only one task at the time)
    val nodes = Seq(TestNode("A"), TestNode("B"), TestNode("C"), TestNode("D"))
    val edges = Seq(TestEgde("A", "B"),TestEgde("B", "D"),TestEgde("A", "C"),TestEgde("C", "D"))
    val dag = DAG.create[TestNode](nodes, edges)
    println(dag.toString)
    val testEventListener: TestEventListener = new TestEventListener
    val task =  dag.buildTaskGraph[TestResult](testEventListener) { defaultOp(300) }
    val resultFuture = task.runToFuture(singleScheduler)
    val (result, tResult) = measureTime( Await.result(resultFuture, 10.seconds).map(_.get))
    println(result)
    assert(result.size==1)
    assert(result.head.path.endsWith("-D"))
    // check nodes are executed only once
    val nodesExecutedMoreThanOnce = execCntPerNode.filter{ case (_,cnt) => cnt.get > 1}
    assert(nodesExecutedMoreThanOnce.isEmpty, s"Nodes $nodesExecutedMoreThanOnce have been executed more than once")
    // check all nodes are executed
    val nodesNotExecuted = nodes.map(_.nodeId).toSet -- execCntPerNode.keySet
    assert(nodesNotExecuted.isEmpty, s"Nodes $nodesNotExecuted have not been executed")
    // parallel execution is not permitted in this case, as serialization is forced
    logger.info("Maximum number of parallel executions was " +testEventListener.maxConcurrentRuns.get)
    assert(testEventListener.maxConcurrentRuns.get() == 1)
  }

  test("cancel running dag: stop pending tasks") {
    val dag = DAG.create[TestNode](
      Seq(TestNode("A"), TestNode("C"), TestNode("B")),
      Seq(TestEgde("A", "B"),TestEgde("B", "C"))
    )
    println(dag.toString)
    val task = dag.buildTaskGraph[TestResult](new TestEventListener) { defaultOp(500) }
    val resultFuture = task.runToFuture
    Thread.sleep(100)
    resultFuture.cancel()
    Thread.sleep(3000) // dag would run for 3 seconds if not cancelled
    assert(execCntPerNode.keySet == Set("A"), "Only one Task should have finished execution, others are cancelled.")
  }

  test("exception in running dag: run pending tasks if not dependent") {
    val nodes = Seq(TestNode("A"), TestNode("B"), TestNode("C"), TestNode("D"), TestNode("E"))
    val edges = Seq(TestEgde("A", "B"), TestEgde("B", "C"), TestEgde("A", "D"), TestEgde("D", "E"))
    val dag = DAG.create[TestNode](nodes, edges)
    println(dag.toString)
    val opWithException = (node:DAGNode, inResults:Seq[TestResult]) => {
      node.nodeId match {
        case "B" => throw new RuntimeException("test exception on node B")
        case _ => defaultOp(500)(node,inResults)
      }
    }
    val task =  dag.buildTaskGraph[TestResult](new TestEventListener) ( opWithException )
    val resultFuture = task.runToFuture
    val resultTry = Await.result(resultFuture, 10.seconds)
    println(resultTry)
    assert(resultTry.size==2)
    // check failed task
    intercept[TaskPredecessorFailureWarning](resultTry.filter(_.isFailure).map(_.get))
    // check succeeded tasks
    val resultSucceeded = resultTry.filter(_.isSuccess).map(_.get)
    assert(resultSucceeded.head.path.endsWith("-E"))
    // check nodes are executed only once
    val nodesExecutedMoreThanOnce = execCntPerNode.filter{ case (_,cnt) => cnt.get > 1}
    assert(nodesExecutedMoreThanOnce.isEmpty, s"Nodes $nodesExecutedMoreThanOnce have been executed more than once")
    // check all nodes except node B and C are executed
    val nodesNotExecuted = nodes.map(_.nodeId).toSet -- execCntPerNode.keySet
    assert(nodesNotExecuted == Set("B","C"), s"Nodes $nodesNotExecuted have not been executed")
  }

  test("create dag: detect loop") {
    intercept[AssertionError](DAG.create(
      Seq(TestNode("A"), TestNode("C"), TestNode("B")),
      Seq(TestEgde("C", "A"),TestEgde("A", "B"),TestEgde("B", "C"))
    ))
  }

  test("create and run dag: unconnected subgraphs with parallel execution") {
    val nodes = Seq(TestNode("A"), TestNode("C"), TestNode("B"), TestNode("D"), TestNode("E"), TestNode("F"))
    val edges = Seq(TestEgde("A", "B"),TestEgde("B", "C"), TestEgde("D", "E"),TestEgde("D", "F"))
    val dag = DAG.create[TestNode](nodes, edges)
    println(dag.toString)
    val testEventListener: TestEventListener = new TestEventListener
    val task =  dag.buildTaskGraph[TestResult](testEventListener){ defaultOp() }
    val resultFuture = task.runToFuture
    val (result, tResult) = measureTime( Await.result(resultFuture, 10.seconds).map(_.get))
    println(result)
    assert(result.size==3)
    assert(result.exists( _.path.endsWith("-C")))
    assert(result.exists( _.path.endsWith("-E")))
    assert(result.exists( _.path.endsWith("-F")))
    // check nodes are executed only once
    val nodesExecutedMoreThanOnce = execCntPerNode.filter{ case (_,cnt) => cnt.get > 1}
    assert(nodesExecutedMoreThanOnce.isEmpty, s"Nodes $nodesExecutedMoreThanOnce have been executed more than once")
    // check all nodes are executed
    val nodesNotExecuted = nodes.map(_.nodeId).toSet -- execCntPerNode.keySet
    assert(nodesNotExecuted.isEmpty, s"Nodes $nodesNotExecuted have not been executed")
    // check parallel execution
    logger.info("Maximum number of parallel executions was " +testEventListener.maxConcurrentRuns.get)
    assert(testEventListener.maxConcurrentRuns.get() >= 2)
  }


  val defaultResultId = TestEdge.defaultResultId

  def defaultOp(sleepMs: Int = 1000): ((DAGNode, Seq[TestResult]) => Seq[TestResult]) = {
    (node: DAGNode, inResults: Seq[TestResult]) => {
      // execute something
      logger.debug(s"start ${node.nodeId}")
      Thread.sleep(sleepMs)
      logger.debug(s"end ${node.nodeId}")
      // count execution
      execCntPerNode.getOrElseUpdate(node.nodeId, new AtomicInteger()).getAndIncrement()
      // prepare path for this result
      val path = if (inResults.isEmpty) node.nodeId
      else if (inResults.size==1) inResults.head.path + "-" + node.nodeId
      else inResults.map( r => s"(${r.path})").mkString("&") + "-" + node.nodeId
      // return
      Seq(TestResult(defaultResultId, path))
    }
  }

  val execCntPerNode: mutable.Map[NodeId,AtomicInteger] = scala.collection.concurrent.TrieMap[String,AtomicInteger]()

}
