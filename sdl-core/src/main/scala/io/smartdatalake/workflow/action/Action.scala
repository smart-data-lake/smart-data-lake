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
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry, ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.definitions._
import io.smartdatalake.util.dag.{DAGNode, TaskSkippedDontStopWarning}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.dataobject.{CanCreateIncrementalOutput, DataObject, TransactionalSparkTableDataObject}
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.functions.expr

import java.time.LocalDateTime
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

/**
 * An action defines a [[DAGNode]], that is, a transformation from input [[DataObject]]s to output [[DataObject]]s in
 * the DAG of actions.
 */
private[smartdatalake] trait Action extends SdlConfigObject with ParsableFromConfig[Action] with DAGNode with SmartDataLakeLogger with AtlasExportable {

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
   * Recursive Inputs are DataObjects that are used as Output and Input in the same action.
   * This is usually prohibited as it creates loops in the DAG.
   * In special cases this makes sense, i.e. when building a complex comparision/update logic.
   *
   * Usage: add DataObjects used as Output and Input as outputIds and recursiveInputIds, but not as inputIds.
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
   * execution condition for this action.
   */
  def executionCondition: Option[Condition]

  // execution condition is evaluated in init phase and result must be stored for exec phase
  protected var executionConditionResult: Option[(Boolean,Option[String])] = None

  /**
   * execution mode for this action.
   */
  def executionMode: Option[ExecutionMode]

  // execution mode is evaluated in init phase and result must be stored for exec phase
  protected var executionModeResult: Option[Try[Option[ExecutionModeResult]]] = None

  /**
   * Spark SQL condition evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
   * If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
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
      assert(outputs.exists(_.id == input.id), s"($id) Recursive input ${input.id} is not listed in outputIds of the same action.")
      assert(input.isInstanceOf[TransactionalSparkTableDataObject], s"($id) Recursive input ${input.id} is not a TransactionalSparkTableDataObjects.")
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
    reset // reset statistics, this is especially needed in unit tests when the same action is started multiple times
    inputs.foreach(_.prepare)
    outputs.foreach(_.prepare)
    executionMode.foreach(_.prepare(id))

    // Make sure that data object names are still unique when replacing special characters with underscore
    // Requirement from SQL transformations because temp view names can not contain special characters
    val duplicateNames = context.instanceRegistry.getDataObjects.map {
      dataObj => ActionHelper.replaceSpecialCharactersWithUnderscore(dataObj.id.id)
    }.groupBy(identity).collect { case (x, List(_,_,_*)) => x }.toList
    require(duplicateNames.isEmpty, s"The names of your DataObjects are not unique when replacing special characters with underscore. Duplicates: ${duplicateNames.mkString(",")}")

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
    // initialize dataObjectsState
    val unrelatedStateDataObjectIds = dataObjectsState.map(_.dataObjectId).diff(inputs.map(_.id))
    assert(unrelatedStateDataObjectIds.isEmpty, s"($id) Got state for unrelated DataObjects ${unrelatedStateDataObjectIds.mkString(", ")}")
    if (executionMode.exists(_.isInstanceOf[DataObjectStateIncrementalMode])) {
      // assert SDL is started with state
      assert(context.appConfig.statePath.isDefined, s"($id) SmartDataLakeBuilder must be started with state path set. Please specify location of state with parameter '--state-path'.")
      // set DataObjects state
      inputs.foreach {
        case input: CanCreateIncrementalOutput => input.setState(dataObjectsState.find(_.dataObjectId == input.id).map(_.state))
        case input => throw new ConfigurationException(s"($id) DataObjectStateIncrementalMode needs input data objects that implement CanCreateIncrementalOutput, but ${input.id} does not.")
      }
    } else {
      assert(dataObjectsState.isEmpty, s"($id) Got dataObjectsState but executionMode not ${classOf[DataObjectStateIncrementalMode].getSimpleName}")
    }
    // check execution condition
    checkExecutionCondition(subFeeds)
  }

  /**
   * Evaluate and check the executionCondition
   * @throws TaskSkippedDontStopWarning if no data to process
   */
  private def checkExecutionCondition(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    // evaluate execution condition in init phase or streaming iteration and store result
    if (executionConditionResult.isEmpty) {
      //noinspection MapGetOrElseBoolean
      executionConditionResult = Some(executionCondition.map { c =>
        // evaluate condition if existing
        val data = SubFeedsExpressionData.fromSubFeeds(subFeeds)
        if (!c.evaluate(id, Some("executionCondition"), data)) {
          val descriptionText = c.description.map(d => s""""$d" """).getOrElse("")
          val msg = s"""($id) execution skipped because of failed executionCondition ${descriptionText}expression="${c.expression}" $data"""
          (false, Some(msg))
        } else (true, None)
      }.getOrElse {
        // default behaviour: if no executionCondition is defined, Action is executed if no input subFeed is skipped.
        val skippedSubFeeds = subFeeds.filter(_.isSkipped)
        if (skippedSubFeeds.nonEmpty) {
          val msg = s"""($id) execution skipped because input subFeeds are skipped: ${subFeeds.map(_.dataObjectId).mkString(", ")}"""
          (false, Some(msg))
        } else (true, None)
      })
    }
    // check execution condition result
    if (!executionConditionResult.get._1 && !context.appConfig.isDryRun) {
      val outputSubFeeds = outputs.map(output => InitSubFeed(dataObjectId = output.id, partitionValues = Seq(), isSkipped = true))
      throw new TaskSkippedDontStopWarning(id.id, executionConditionResult.get._2.get, Some(outputSubFeeds))
    }
  }

  /**
   * Applies the executionMode and stores result in executionModeResult variable
   */
  protected def applyExecutionMode(mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                  , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                  (implicit context: ActionPipelineContext): Unit = {
    executionModeResult = Some(Try(
      executionMode.flatMap(_.apply(id, mainInput, mainOutput, subFeed, partitionValuesTransform))
    ).recover {
      // return skipped output subfeeds if "no data"
      case ex: NoDataToProcessWarning =>
        // create fake results (subFeeds with isSkipped=true)
        val outputSubFeeds = outputs.map(output => InitSubFeed(dataObjectId = output.id, partitionValues = Seq(), isSkipped = true))
        // Add fake results to exception. The DAG will pass the fake results to further actions, which can decide if the want to stop if input subFeeds are skipped by defining an executionCondition.
        throw NoDataToProcessWarning(ex.actionId, ex.msg, results = Some(outputSubFeeds))
    })
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
    // reset execution condition if not start Action in pipeline, because input for execution results could change between init and exec phase
    val isStartAction = subFeeds.exists(_.isInstanceOf[InitSubFeed])
    if (!isStartAction) resetExecutionResult()
    // check execution condition
    checkExecutionCondition(subFeeds)
    // throw execution mode failure exception if any
    if (executionModeResult.exists(x => x.isFailure)) executionModeResult.get.get
    // init spark jobGroupId to identify metrics
    setSparkJobMetadata()
    // otherwise continue processing
    inputs.foreach( input => input.preRead(findSubFeedPartitionValues(input.id, subFeeds)))
    outputs.foreach(_.preWrite) // Note: transformed subFeeds don't exist yet, that's why no partition values can be passed as parameters.
  }

  /**
   * Executes the main task of an action.
   * In this step the data of the SubFeed's is moved from Input- to Output-DataObjects.
   *
   * @param subFeeds [[SparkSubFeed]]'s to be processed
   * @return processed [[SparkSubFeed]]'s
   */
  //TODO should Remote/Non Remote Logic happen here?
  def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed]

  /**
   * Executes operations needed after executing an action.
   * In this step any task on Input- or Output-DataObjects needed after the main task is executed,
   * e.g. JdbcTableDataObjects postWriteSql or CopyActions deleteInputData.
   */
  def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    if (isAsynchronousProcessStarted) return
    // evaluate metrics fail condition if defined
    metricsFailCondition.foreach( c => evaluateMetricsFailCondition(c))
    // process postRead/Write hooks
    inputs.foreach( input => input.postRead(findSubFeedPartitionValues(input.id, inputSubFeeds)))
    outputs.foreach( output => output.postWrite(findSubFeedPartitionValues(output.id, outputSubFeeds)))
    // update DataObjects incremental state in DataObjectStateIncrementalMode
    if (executionMode.exists(_.isInstanceOf[DataObjectStateIncrementalMode])) {
      inputs.foreach {
        case input: CanCreateIncrementalOutput => input.setState(input.getState)
        case _ => Unit
      }
    }
  }

  /**
   * Executes operations needed to cleanup after executing an action failed.
   */
  def postExecFailed(implicit context: ActionPipelineContext): Unit = Unit

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
  private def evaluateMetricsFailCondition(condition: String)(implicit context: ActionPipelineContext): Unit = {
    val conditionEvaluator = new ExpressionEvaluator[Metric,Boolean](expr(condition))
    val metrics = {
      getRuntimeMetrics().flatMap{
        case (dataObjectId, Some(metrics)) => metrics.getMainInfos.map{ case (k,v) => Metric(dataObjectId.id, Some(k), Some(v.toString))}.toSeq
        case (dataObjectId, _) => Seq(Metric(dataObjectId.id, None, None))
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
  private def findSubFeedPartitionValues(dataObjectId: DataObjectId, subFeeds: Seq[SubFeed]): Seq[PartitionValues] = subFeeds.find(_.dataObjectId == dataObjectId).map(_.partitionValues).get

  /**
   * Handle class cast exception when getting objects from instance registry
   */
  private def getDataObject[T <: DataObject](dataObjectId: DataObjectId, role: String)(implicit registry: InstanceRegistry, ct: ClassTag[T], tt: TypeTag[T]): T = {
    val dataObject = registry.get[T](dataObjectId)
    try {
      // force class cast on generic type (otherwise the ClassCastException is thrown later)
      ct.runtimeClass.cast(dataObject).asInstanceOf[T]
    } catch {
      case _: ClassCastException =>
        val objClass = dataObject.getClass.getSimpleName
        val expectedClass = tt.tpe.toString.replaceAll(classOf[DataObject].getPackage.getName+".", "")
        throw ConfigurationException(s"$toStringShort needs $expectedClass as $role but $dataObjectId is of type $objClass")
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
  def addRuntimeMetrics(executionId: Option[ExecutionId], dataObjectId: Option[DataObjectId], metric: ActionMetrics): Unit = {
    if (dataObjectId.isDefined) {
      if (outputs.exists(_.id == dataObjectId.get)) try {
        runtimeData.addMetric(executionId, dataObjectId.get, metric)
      } catch {
        case e: LateArrivingMetricException => logger.error(s"($id) ${e.msg}")
        case e: AssertionError => logger.error(s"($id) ${e.getMessage}")
      }
      else logger.warn(s"($id) Metrics received for ${dataObjectId.get} which doesn't belong to outputs ($metric")
    } else logger.debug(s"($id) Metrics received for unspecified DataObject (${metric.getId})")
    if (logger.isDebugEnabled) logger.debug(s"($id) Metrics received:\n" + metric.getAsText)
  }

  /**
   * Get the latest metrics for all DataObjects and a given SDLExecutionId.
   * @param executionId ExecutionId to get metrics for. If empty metrics for last ExecutionId are returned.
   */
  def getRuntimeMetrics(executionId: Option[ExecutionId] = None): Map[DataObjectId, Option[ActionMetrics]] = {
    outputs.map(dataObject => (dataObject.id, runtimeData.getMetrics(dataObject.id, executionId))).toMap
  }

  /**
   * Get summarized runtime information for a given ExecutionId.
   * @param executionId ExecutionId to get runtime information for. If empty runtime information for last ExecutionId are returned.
   */
  def getRuntimeInfo(executionId: Option[ExecutionId] = None) : Option[RuntimeInfo] = {
    runtimeData.getRuntimeInfo(outputs.map(_.id), getDataObjectsState, executionId)
  }

  /**
   * Resets the runtime state of this Action
   * This is mainly used for testing
   */
  private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    runtimeData.clear()
    resetExecutionResult()
  }

  /**
   * Resets execution results of this Action for repeated execution
   */
  private[smartdatalake] def resetExecutionResult(): Unit = {
    executionConditionResult = None
    executionModeResult = None
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
