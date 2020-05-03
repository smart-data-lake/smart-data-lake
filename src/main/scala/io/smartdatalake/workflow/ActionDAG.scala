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

import java.time.{LocalDateTime, Duration => JavaDuration}

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.metrics.StageMetricsListener
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DAGHelper._
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{Action, RuntimeEventState, RuntimeInfo}
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Codec

private[smartdatalake] case class ActionDAGEdge(override val nodeIdFrom: NodeId, override val nodeIdTo: NodeId, override val resultId: String) extends DAGEdge

private[smartdatalake] case class InitDAGNode(override val nodeId: NodeId, edges: Seq[String]) extends DAGNode {
  override def toString: NodeId = nodeId // this is displayed in ascii graph visualization
}

private[smartdatalake] case class DummyDAGResult(override val resultId: String) extends DAGResult

private[smartdatalake] case class ActionDAGRun(dag: DAG[Action], runId: String, partitionValues: Seq[PartitionValues], parallelism: Int, statePath: Option[String]) extends SmartDataLakeLogger {

  private val stateHadoopPath = statePath.map( p => HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(p)))
  private lazy val fileSystem: FileSystem = HdfsUtil.getHadoopFs(stateHadoopPath.get)

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
      val dagExceptionsToStop = dagExceptions.filter(_.severity <= ExceptionSeverity.SKIPPED)
      // log all exceptions
      dagExceptions.foreach {
        case ex if (ex.severity <= ExceptionSeverity.CANCELLED) => logger.error(s"$op: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        case ex => logger.warn(s"$op: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
      }
      // log dag on error
      if (dagExceptionsToStop.nonEmpty) ActionDAGRun.logDag(s"$op failed for dag $runId", dag)
      // throw most severe exception
      dagExceptionsToStop.sortBy(_.severity).foreach{ throw _ }

      // extract & return subfeeds
      result.filter(_.isSuccess).map(_.get)
    } finally {
      session.sparkContext.removeSparkListener(stageMetricsListener)
      if (stageMetricsListener.stageMetricsCollection.nonEmpty) {
        logger.debug(s"Operation '$op' stage metrics:\n - ${stageMetricsListener.stageMetricsCollection.map(_.report()).mkString("\n - ")}")
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
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }
  }

  def init(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    // run init for every node
    run[SubFeed]("init") {
      case (node: InitDAGNode, _) =>
        node.edges.map(dataObjectId => InitSubFeed(dataObjectId, partitionValues))
      case (node: Action, subFeeds) =>
        node.init(subFeeds)
      case x => throw new IllegalStateException(s"Unmatched case $x")
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
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }

    // log dag execution
    ActionDAGRun.logDag(s"exec result dag $runId", dag)

    // return
    result
  }

  /**
   * Collect runtime information from for every action of the dag
   */
  def getRuntimeInfos: Map[String, RuntimeInfo] = {
    dag.getNodes.map( a => (a.id.id, a.getRuntimeInfo.getOrElse(RuntimeInfo(RuntimeEventState.PENDING)))).toMap
  }

  /**
   * Persist state of dag to file
   */
  def persistState(implicit session: SparkSession, context: ActionPipelineContext): Unit = synchronized {
    stateHadoopPath.foreach {
      p =>
        logger.info(s"persist state to filesystem location: ${p.toUri}.")
        val state = ActionDAGRunState(context.appConfig, getRuntimeInfos)
        val json = state.toJson
        val os = fileSystem.create(p, true) // overwrite if exists
        os.write(json.getBytes("UTF-8"))
        os.close()
    }
  }

  private class ActionEventListener(operation: String)(implicit session: SparkSession, context: ActionPipelineContext) extends DAGEventListener[Action] with SmartDataLakeLogger {
    override def onNodeStart(node: Action): Unit = {
      node.addRuntimeEvent(operation, RuntimeEventState.STARTED)
      logger.info(s"${node.toStringShort}: $operation started")
    }
    override def onNodeSuccess(node: Action): Unit = {
      node.addRuntimeEvent(operation, RuntimeEventState.SUCCEEDED)
      logger.info(s"${node.toStringShort}: $operation: succeeded")
      if (operation=="exec") persistState
    }
    override def onNodeFailure(exception: Throwable)(node: Action): Unit = {
      node.addRuntimeEvent(operation, RuntimeEventState.FAILED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
      logger.error(s"${node.toStringShort}: $operation failed with ${exception.getClass.getSimpleName}: ${exception.getMessage}")
      if (operation=="exec") persistState
    }
    override def onNodeSkipped(exception: Throwable)(node: Action): Unit = {
      node.addRuntimeEvent(operation, RuntimeEventState.SKIPPED, Some(s"${exception.getClass.getSimpleName}: ${exception.getMessage}"))
      logger.warn(s"${node.toStringShort}: $operation: skipped because of ${exception.getClass.getSimpleName}: ${exception.getMessage}")
      if (operation=="exec") persistState
    }
  }
}

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, actionsState: Map[String, RuntimeInfo] ) {
  def toJson: String = ActionDAGRunState.toJson(this)
}
object ActionDAGRunState {
  // json4s is used because kxbmap configs supports converting case classes to config only from verion 5.0 which isn't yet stable
  // json4s is included with spark-core
  // Note that key-type of Maps must be String in json4s (e.g. actionsState...)
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization
  def toJson(actionDAGRunState: ActionDAGRunState): String = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints) + RuntimeEventStateSerializer + DurationSerializer + LocalDateTimeSerializer
    pretty(parse(Serialization.write(actionDAGRunState)))
  }
  def fromJson(stateJson: String): ActionDAGRunState = {
    implicit val formats: Formats = DefaultFormats + RuntimeEventStateSerializer + DurationSerializer + LocalDateTimeSerializer
    parse(stateJson).extract[ActionDAGRunState]
  }
  // custom serialization for RuntimeEventState which is shorter than the default
  case object RuntimeEventStateSerializer extends CustomSerializer[RuntimeEventState](format => (
    { case JString(state) => RuntimeEventState.withName(state) },
    { case state: RuntimeEventState => JString(state.toString) }
  ))
  // custom serialization for Duration
  case object DurationSerializer extends CustomSerializer[JavaDuration](format => (
    { case JString(dur) => JavaDuration.parse(dur) },
    { case dur: JavaDuration => JString(dur.toString) }
  ))
  // custom serialization for LocalDateTime
  case object LocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => (
    { case JString(tstmp) => LocalDateTime.parse(tstmp) },
    { case tstmp: LocalDateTime => JString(tstmp.toString) }
  ))
}

private[smartdatalake] object ActionDAGRun extends SmartDataLakeLogger {

  /**
   * recover previous run state
   */
  def recoverRunState(statePath: String): ActionDAGRunState = {
    val stateHadoopPath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(statePath))
    val fileSystem: FileSystem = HdfsUtil.getHadoopFs(stateHadoopPath)
    require(fileSystem.isFile(stateHadoopPath), s"Cannot recover previous run state. $statePath doesn't exists or is not a file.")
    val is = fileSystem.open(stateHadoopPath)
    val json = scala.io.Source.fromInputStream(is)(Codec.UTF8).mkString
    ActionDAGRunState.fromJson(json)
  }

  /**
   * create ActionDAGRun
   */
  def apply(actions: Seq[Action], runId: String, partitionValues: Seq[PartitionValues] = Seq(), parallelism: Int = 1, statePath: Option[String] = None)(implicit session: SparkSession, context: ActionPipelineContext): ActionDAGRun = {

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

    ActionDAGRun(dag, runId, partitionValues, parallelism, statePath)
  }

  def logDag(msg: String, dag: DAG[_]): Unit = {
    logger.info(s"""$msg:
                   |${dag.toString}
    """.stripMargin)
  }
}
