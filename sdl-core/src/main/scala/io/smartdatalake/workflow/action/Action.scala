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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.SdlConfigObject.{ActionId, AgentId, DataObjectId}
import io.smartdatalake.config._
import io.smartdatalake.definitions._
import io.smartdatalake.util.dag.{DAGNode, TaskSkippedDontStopWarning}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.executionMode.{DataObjectStateIncrementalMode, ExecutionMode}
import io.smartdatalake.workflow.dataobject.{CanCreateIncrementalOutput, DataObject, TransactionalTableDataObject}
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.functions.expr

import java.time.LocalDateTime
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * An action defines a [[DAGNode]], that is, a transformation from input [[DataObject]]s to output [[DataObject]]s in
 * the DAG of actions.
 */
trait Action extends SdlConfigObject with ParsableFromConfig[Action] with DAGNode with SmartDataLakeLogger with AtlasExportable {

  /**
   * A unique identifier for this instance.
   */
  override val id: ActionId

  /**
   * Additional metadata for the Action
   */
  def metadata: Option[ActionMetadata]

  /**
   * Input [[DataObject]]s
   * To be implemented by subclasses
   */
  def inputs: Seq[DataObject]

  /**
   * Recursive Inputs are DataObjects that are used as Output and Input in the same or different action.
   * This is usually prohibited as it creates loops in the DAG.
   * In special cases this makes sense, i.e. when building a complex comparision/update logic.
   * Recursive inputs are allowed in the same Action if the DataObject implements TransactionalSparkTableDataObject.
   * For special cases this is to restrictive. To allow special DataObjects for recursive use within two different actions,
   * see also [[GlobalConfig.allowAsRecursiveInput]].
   *
   * Usage: add DataObjects that are used both as Output and Input as outputIds and recursiveInputIds, but do not add them as inputIds.
   */
  def recursiveInputs: Seq[DataObject] = Seq()

  /**
   * Define if recursive inputs should be prepared as input SubFeed by ActionDAG or if this is handled by the action internally.
   * Default is to prepare & expect it as input SubFeed, but this can be overriden by subclasses
   */
  private[smartdatalake] def handleRecursiveInputsAsSubFeeds: Boolean = true

  /**
   * Output [[DataObject]]s
   * To be implemented by subclasses
   */
  def outputs: Seq[DataObject]

  /**
   * Optional execution condition for this action.
   * By default, an Action is executed if all inputs are available, e.g. no input from a previous Action is skipped.
   * Override the default behaviour by specifying an executionCondition in Spark SQL expression syntax.
   * It is evaluated against the properties available in [[SubFeedsExpressionData]].
   * If true, the Action is executed, otherwise it is skipped.
   *
   * Example:
   * {{{
   *     executionCondition = {
   *       description = "execute if input stg-src1 is not skipped"
   *       expression = "!inputSubFeeds.stg-src1.isSkipped"
   *     }
   * }}}
   */
  def executionCondition: Option[Condition]

  /**
   * Optional execution mode for this action.
   */
  def executionMode: Option[ExecutionMode]

  /**
   * Optional Spark SQL condition evaluated as where-clause against dataframe of SubFeed metrics. Available columns are dataObjectId, key, value.
   * If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
   *
   * To check for skipped SubFeeds, an additional row with key='skipped' and value=true|false is created per output DataObject.
   *
   * Example - fail an action writing to output int-tgt in case there are no records written:
   * {{{
   *   dataObjectId = 'int-tgt' and key = 'no_data' and value = true"
   * }}}
   */
  def metricsFailCondition: Option[String]

  /**
   * If this Action should be run as asynchronous streaming process
   */
  private[smartdatalake] def isAsynchronous: Boolean = false

  private[smartdatalake] def isAsynchronousProcessStarted: Boolean = false

  def agentId: Option[AgentId] = None

  /**
   * Validate configuration.
   * Put validation logic here which will run on class instantiation. It has to be put into a separate method because like that
   * it can be called from the final Action case class. This makes sure that everything is initialized, e.g. abstract method "inputs" is defined.
   *
   * This must be called by every Action in initialization code of the case class.
   */
  def validateConfig(): Unit = {
    recursiveInputs.foreach { input =>
      if (outputs.exists(_.id == input.id)) {
        assert(input.isInstanceOf[TransactionalTableDataObject], s"($id) Recursive input ${input.id} is listed in outputIds but is not a TransactionalTableDataObject.")
      } else if (Environment.globalConfig.allowAsRecursiveInput.contains(input.id)) {
        logger.info(s"($id) Using ${input.id} as recursive input even though it is not listed in outputIds of the same Action, but in globalConfig.allowAsRecursiveInput")
      } else {
        assert(outputs.exists(_.id == input.id), s"($id) Recursive input ${input.id} is not listed in outputIds of the same action. If you're sure about this, mark it as exception in globalConfig.allowAsRecursiveInput.")
      }
    }
  }

  /**
   * Prepare DataObjects prerequisites.
   * In this step preconditions are prepared & tested:
   * - connections can be created
   * - needed structures exist, e.g Kafka topic or Jdbc table
   *
   * This runs during the "prepare" phase of the DAG.
   */
  def prepare(implicit context: ActionPipelineContext): Unit = {
    inputs.foreach(_.prepare)
    outputs.foreach(_.prepare) // this also includes recursiveInputs
    executionMode.foreach(_.prepare(id))

    // Make sure that data object names are still unique when replacing special characters with underscore
    // Requirement from SQL transformations because temp view names can not contain special characters
    val duplicateNames = context.instanceRegistry.getDataObjects.map {
      dataObj => ActionHelper.replaceSpecialCharactersWithUnderscore(dataObj.id.id)
    }.groupBy(identity).collect { case (x, List(_,_,_*)) => x }.toList
    require(duplicateNames.isEmpty, s"($id) The names of your DataObjects are not unique when replacing special characters with underscore. Duplicates: ${duplicateNames.mkString(",")}")

    // validate executionCondition
    executionCondition.foreach(_.syntaxCheck[SubFeedsExpressionData](id, Some("executionCondition")))

    // validate metricsFailCondition
    metricsFailCondition.foreach(c => SparkExpressionUtil.syntaxCheck[Metric,Boolean](id, Some("metricsFailCondition"), c))
  }

  /**
   * Checks before initalization of Action
   * In this step execution condition is evaluated and Action init is skipped if result is false.
   */
  def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = {
    // call execution mode hook
    executionMode.foreach(_.preInit(subFeeds,dataObjectsState))
  }

  /**
   * Evaluate and check the executionCondition in exec phase
   * @throws TaskSkippedDontStopWarning if task is skipped
   */
  private def checkExecutionCondition(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    //noinspection MapGetOrElseBoolean
    val skipMsg = executionCondition.map { c =>
      // evaluate condition if existing
      val data = SubFeedsExpressionData.fromSubFeeds(subFeeds)
      if (!c.evaluate(id, Some("executionCondition"), data)) {
        val descriptionText = c.description.map(d => s""""$d" """).getOrElse("")
        Some(s"""($id) execution skipped because of failed executionCondition ${descriptionText}expression="${c.expression}" $data""")
      } else None
    }.getOrElse {
      // default behaviour: if no executionCondition is defined, Action is executed if no input subFeed is skipped.
      val skippedSubFeeds = subFeeds.filter(_.isSkipped)
      if (skippedSubFeeds.nonEmpty) {
        Some(s"""($id) execution skipped because input subFeeds are skipped: ${subFeeds.map(_.dataObjectId).mkString(", ")}""")
      } else None
    }
    // check execution condition result
    if (skipMsg.nonEmpty) throw new TaskSkippedDontStopWarning(id.id, skipMsg.get, Some(ActionHelper.createSkippedSubFeeds(outputs)))
  }

  /**
   * Initialize Action with [[SubFeed]]'s to be processed.
   * In this step the execution mode is evaluated and the result stored for the exec phase.
   * If successful
   * - the DAG can be built
   * - Spark DataFrame lineage can be built
   *
   * @param subFeeds [[SparkSubFeed]]'s to be processed
   * @return processed [[SparkSubFeed]]'s
   */
  def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed]

  /**
   * Executes operations needed before executing an action.
   * In this step any phase on Input- or Output-DataObjects needed before the main task is executed,
   * e.g. JdbcTableDataObjects preWriteSql
   */
  def preExec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    if (isAsynchronousProcessStarted) return
    // check execution condition
    checkExecutionCondition(subFeeds)
    // init spark jobGroupId to identify metrics
    setSparkJobMetadata() // TODO: this triggers creating spark session
    // otherwise continue processing
    (inputs ++ recursiveInputs).foreach(input => input.preRead(findSubFeedPartitionValues(input.id, subFeeds)))
    outputs.foreach(_.preWrite) // Note: transformed subFeeds don't exist yet, that's why no partition values can be passed as parameters.
  }

  /**
   * Executes the main task of an action.
   * In this step the data of the SubFeed's is moved from Input- to Output-DataObjects.
   *
   * @param subFeeds [[SparkSubFeed]]'s to be processed
   * @return processed [[SparkSubFeed]]'s
   */
  def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed]

  /**
   * Executes operations needed after executing an action.
   * In this step any task on Input- or Output-DataObjects needed after the main task is executed,
   * e.g. JdbcTableDataObjects postWriteSql or CopyActions deleteInputData.
   */
  def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    if (isAsynchronousProcessStarted) return
    // evaluate metrics fail condition if defined
    metricsFailCondition.foreach( c => evaluateMetricsFailCondition(c, outputSubFeeds))
    // process postRead/Write hooks
    (inputs ++ recursiveInputs).foreach(input => input.postRead(findSubFeedPartitionValues(input.id, inputSubFeeds)))
    outputs.foreach( output => output.postWrite(findSubFeedPartitionValues(output.id, outputSubFeeds)))
  }

  /**
   * Executes operations needed to cleanup after executing an action failed (this includes NoDataToProcessWarning).
   */
  def postExecFailed(ex: Exception)(implicit context: ActionPipelineContext): Unit = {
    ex match {
      case ex: NoDataToProcessWarning if ex.results.isDefined =>
        // evaluate metrics fail condition if defined
        metricsFailCondition.foreach( c => evaluateMetricsFailCondition(c, ex.results.get))
      case _ => ()
    }
  }

  /**
   * Get potential state of input DataObjects when executionMode is DataObjectStateIncrementalMode.
   */
  def getDataObjectsState: Seq[DataObjectState] = {
    // only get state if incremental execution mode *and* action was successfully executed or skipped
    if (executionMode.exists(_.isInstanceOf[DataObjectStateIncrementalMode])
        && runtimeData.getLatestEventState.exists(state => Seq(RuntimeEventState.SUCCEEDED,RuntimeEventState.INITIALIZED).contains(state))) {
      inputs.collect {
        case input: CanCreateIncrementalOutput => input.getState.map(DataObjectState(input.id, _))
      }.flatten
    } else Seq()
  }

  /**
   * Evaluates a condition against latest metrics and throws an MetricsCheckFailed if there is a match.
   */
  private def evaluateMetricsFailCondition(condition: String, subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    val conditionEvaluator = new ExpressionEvaluator[Metric,Boolean](expr(condition))
    val metrics = subFeeds.flatMap{ subFeed =>
      val metricsRaw = subFeed.metrics.getOrElse(Map()) + ("skipped" -> subFeed.isSkipped.toString) // add additional "skipped=true|false" metric
      metricsRaw.map{
        case (k, v: Option[_]) => Metric(subFeed.dataObjectId.id, Option(k), v.map(_.toString))
        case (k, v) => Metric(subFeed.dataObjectId.id, Option(k), Option(v).map(_.toString))
      }.toSeq
    }
    metrics.filter( metric => Option(conditionEvaluator(metric)).getOrElse(false))
      .foreach( failedMetric => throw MetricsCheckFailed(s"""($id) metrics check failed: $failedMetric matched expression "$condition""""))
  }

  /**
   * provide an implementation of the DAG node id
   */
  def nodeId: String = id.id

  /**
   * Sets the util job description for better traceability in the Spark UI
   *
   * Note: This sets Spark local properties, which are propagated to the respective executor tasks.
   * We rely on this to match metrics back to Actions and DataObjects.
   * As writing to a DataObject on the Driver happens uninterrupted in the same exclusive thread, this is suitable.
   *
   * @param operation phase description (be short...)
   */
  def setSparkJobMetadata(operation: Option[String] = None)(implicit context: ActionPipelineContext) : Unit = {
    context.sparkSession.sparkContext.setJobGroup(s"${context.appConfig.appName} $id runId=${context.executionId.runId} attemptId=${context.executionId.attemptId}", operation.getOrElse("").take(255))
  }

  /**
   * Helper to find partition values for a specific DataObject in list of subFeeds
   */
  private def findSubFeedPartitionValues(dataObjectId: DataObjectId, subFeeds: Seq[SubFeed]): Seq[PartitionValues] = {
    subFeeds.find(_.dataObjectId == dataObjectId).map(_.partitionValues).getOrElse(Seq())
  }

  /**
   * Handle class cast exception when getting objects from instance registry
   */
  private def getDataObject[T <: DataObject : ClassTag : TypeTag](dataObjectId: DataObjectId, role: String)(implicit registry: InstanceRegistry): T = {
    try {
      registry.get[T](dataObjectId)
    } catch {
      case _: NoSuchElementException => throw new NoSuchElementException(s"($id) key not found in instance registry for $role: $dataObjectId")
      case TypeMismatchException(_, currentClass, expectedType) =>
        throw ConfigurationException(s"($id) $role $dataObjectId of type ${currentClass.getSimpleName} does not implement expected DataObject type $expectedType")
    }
  }
  protected def getInputDataObject[T <: DataObject: ClassTag: TypeTag](id: DataObjectId)(implicit registry: InstanceRegistry): T = getDataObject[T](id, "input")
  protected def getOutputDataObject[T <: DataObject: ClassTag: TypeTag](id: DataObjectId)(implicit registry: InstanceRegistry): T = getDataObject[T](id, "output")


  /**
   * Runtime metrics & events
   * Implementation of runtimeData can be overridden by subclasses
   */
  private[smartdatalake] val runtimeData: RuntimeData = getRuntimeDataImpl
  protected def getRuntimeDataImpl: RuntimeData = {
    SynchronousRuntimeData(Environment.runtimeDataNumberOfExecutionsToKeep)
  }

  /**
   * Adds a runtime event for this Action
   */
  def addRuntimeEvent(executionId: ExecutionId, phase: ExecutionPhase, state: RuntimeEventState, msg: Option[String] = None, results: Seq[SubFeed] = Seq(), tstmp: LocalDateTime = LocalDateTime.now): Unit = {
    runtimeData.addEvent(executionId, RuntimeEvent(tstmp, phase, state, msg, results))
  }

  /**
   * Get latest runtime state
   */
  def getLatestRuntimeEventState: Option[RuntimeEventState] = runtimeData.getLatestEventState

  /**
   * Adds a runtime metric for this Action
   */
  def addAsyncMetrics(executionId: Option[ExecutionId], dataObjectId: Option[DataObjectId], metric: ActionMetrics): Unit = {
    if (dataObjectId.isDefined) {
      if (outputs.exists(_.id == dataObjectId.get)) try {
        runtimeData.addMetric(executionId, dataObjectId.get, metric)
      } catch {
        case e: AssertionError => logger.error(s"($id) ${e.getMessage}")
      }
      else logger.warn(s"($id) Metrics received for ${dataObjectId.get} which doesn't belong to outputs ($metric")
    } else logger.debug(s"($id) Metrics received for unspecified DataObject (${metric.getId})")
    if (logger.isDebugEnabled) logger.debug(s"($id) Metrics received:\n" + metric.getAsText)
  }

  /**
   * Get summarized runtime information for a given ExecutionId.
   * @param executionId ExecutionId to get runtime information for. If empty runtime information for last ExecutionId are returned.
   */
  def getRuntimeInfo(executionId: Option[ExecutionId] = None) : Option[RuntimeInfo] = {
    runtimeData.getRuntimeInfo((inputs ++ recursiveInputs).map(_.id), outputs.map(_.id), getDataObjectsState, executionId)
  }

  /**
   * Resets the runtime state of this Action
   * This is mainly used for testing
   */
  private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    runtimeData.clear()
  }

  /**
   * This is displayed in ascii graph visualization
   */
  final override def toString: String = {
   nodeId + getRuntimeInfo().map(" "+_).getOrElse("")
  }

  final def toString(executionId: Option[ExecutionId]): String = {
    nodeId + getRuntimeInfo(executionId).map(" "+_).getOrElse("")
  }

  def toStringShort: String = {
    s"$id[${this.getClass.getSimpleName}]"
  }

  def toStringMedium: String = {
    val inputStr = inputs.map( _.toStringShort).mkString(", ")
    val outputStr = outputs.map( _.toStringShort).mkString(", ")
    s"$toStringShort Inputs: $inputStr Outputs: $outputStr"
  }

  override def atlasName: String = id.id
}

/**
 * Additional metadata for an Action
 * @param name Readable name of the Action
 * @param description Description of the content of the Action
 * @param feed Name of the feed this Action belongs to
 * @param tags Optional custom tags for this object
 */
case class ActionMetadata(
                           name: Option[String] = None,
                           description: Option[String] = None,
                           feed: Option[String] = None,
                           tags: Seq[String] = Seq()
                         )
//TODO: restrict characters possible in name and feed (feed should not contain :;% because of regex feedSel)
