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

package io.smartdatalake.workflow.action

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.metrics.MetricsUtil.{orderMetrics, orderMetricsDefault}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{CanHandlePartitions, DataObject}

import java.time.Duration
import scala.collection.SortedSet
import scala.reflect.runtime.universe._

/**
 * Implementation of SubFeed handling.
 * This is a generic implementation that supports many input and output SubFeeds.
 *
 * @tparam S SubFeed type this Action is designed for.
 */
abstract class ActionSubFeedsImpl[S <: SubFeed : TypeTag] extends Action {

  // Hook to override main input/output in sub classes
  def mainInputId: Option[DataObjectId] = None
  def mainOutputId: Option[DataObjectId] = None

  // Hook to ignore filters for specific inputs
  def inputIdsToIgnoreFilter: Seq[DataObjectId] = Seq()

  /**
   * put configuration validation checks here
   */
  override def validateConfig(): Unit = {
    super.validateConfig()
    // check inputIdsToIgnoreFilters
    inputIdsToIgnoreFilter.foreach(inputId => assert((inputs ++ recursiveInputs).exists(_.id == inputId), s"($id) $inputId from inputIdsToIgnoreFilter must be listed in inputIds of the same action."))
  }

  // prepare main input / output
  // this must be lazy because inputs / outputs is evaluated later in subclasses
  // Note: we don't yet decide for a main input as inputs might be skipped at runtime, but we can already create a prioritized list.
  protected lazy val prioritizedMainInputCandidates: Seq[DataObject] = getMainDataObjectCandidates(mainInputId, inputs, "input")
  private[smartdatalake] lazy val mainOutput: DataObject = getMainDataObjectCandidates(mainOutputId, outputs, "output").head
  private[smartdatalake] def getMainInput(inputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): DataObject = {
    // take first data object which has as SubFeed which is not skipped
    prioritizedMainInputCandidates.find(dataObject => !inputSubFeeds.find(_.dataObjectId == dataObject.id).get.isSkipped || context.appConfig.isDryRun)
      .getOrElse(prioritizedMainInputCandidates.head) // otherwise just take first candidate
  }
  private[smartdatalake] def getMainPartitionValues(inputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    val mainInput = getMainInput(inputSubFeeds)
    val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId==mainInput.id)
    mainInputSubFeed.map(_.partitionValues).getOrElse(Seq())
  }

  // helper data structures
  private[smartdatalake] lazy val inputMap = (inputs ++ recursiveInputs).map(i => i.id -> i).toMap
  private lazy val outputMap = outputs.map(i => i.id -> i).toMap

  private[smartdatalake] def subFeedConverter: SubFeedConverter[S]

  def prepareInputSubFeeds(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): (Seq[S],Seq[S]) = {
    val mainInput = getMainInput(subFeeds)
    val mainSubFeed = subFeeds.find(_.dataObjectId == mainInput.id).get
    // convert subfeeds to this Actions SubFeed type or initialize if not yet existing
    var inputSubFeeds: Seq[S] = subFeeds.map { subFeed =>
      val partitionValues = if (mainSubFeed.partitionValues.nonEmpty) Some(mainSubFeed.partitionValues) else None
      updateInputPartitionValues(inputMap(subFeed.dataObjectId), subFeedConverter.fromSubFeed(subFeed), partitionValues)
    }
    val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId == mainInput.id).get
    // create output subfeeds with transformed partition values from main input
    var outputSubFeeds: Seq[S] = outputs.map(output =>
      updateOutputPartitionValues(output, subFeedConverter.get(mainInputSubFeed.toOutput(output.id)), Some(transformPartitionValues))
    )
    // apply execution mode only in exec phase
    if (context.isExecPhase) {
      // apply execution mode
      val executionModeResult = try {
        executionMode.flatMap(_.apply(id, mainInput, mainOutput, mainInputSubFeed, transformPartitionValues))
      } catch {
        // throw exception with skipped output subfeeds if "no data"
        case ex: NoDataToProcessWarning if ex.results.isEmpty => throw ex.copy(results = Some(ActionHelper.createSkippedSubFeeds(outputs)))
      }
      // apply execution mode result
      executionModeResult match { // throws exception if execution mode is Failure
        case Some(result) =>
          inputSubFeeds = inputSubFeeds.map { subFeed =>
            updateInputPartitionValues(inputMap(subFeed.dataObjectId), subFeedConverter.get(subFeed.applyExecutionModeResultForInput(result, mainInput.id)))
          }
          outputSubFeeds = outputSubFeeds.map(subFeed =>
            // we need to transform inputPartitionValues again to outputPartitionValues so that partition values from partitions not existing in mainOutput are not lost.
            updateOutputPartitionValues(outputMap(subFeed.dataObjectId), subFeedConverter.get(subFeed.applyExecutionModeResultForOutput(result)), Some(transformPartitionValues))
          )
          executionModeResultOptions = result.options
        case _ => ()
      }
    }
    inputSubFeeds = inputSubFeeds.map{ subFeed =>
      // prepare input SubFeed
      val ignoreFilter = inputIdsToIgnoreFilter.contains(subFeed.dataObjectId)
      val isRecursive = recursiveInputs.exists(_.id == subFeed.dataObjectId)
      // reset potentially skipped SubFeeds to deliver data as well.
      val reactivatedSubFeed = subFeed.clearSkipped().asInstanceOf[S]
      preprocessInputSubFeedCustomized(reactivatedSubFeed, ignoreFilter, isRecursive)
    }
    outputSubFeeds = outputSubFeeds.map(subFeed => addRunIdPartitionIfNeeded(outputMap(subFeed.dataObjectId), subFeed))
    (inputSubFeeds, outputSubFeeds)
  }

  // Keep execution mode result in a variable for now.
  // TODO: this should be a property of the SubFeed. Like that it is passed to the Action and its Input/Output DataObjects.
  protected var executionModeResultOptions: Map[String,String] = Map()

  def postprocessOutputSubFeeds(subFeeds: Seq[S], inputSubFeeds: Seq[S])(implicit context: ActionPipelineContext): Seq[S] = {
    // assert all outputs have a subFeed
    outputs.foreach{ output =>
        subFeeds.find(_.dataObjectId == output.id).getOrElse(throw new IllegalStateException(s"($id) subFeed for output ${output.id} not found"))
    }
    // validate & update subfeeds
    subFeeds.map { subFeed =>
      outputMap.getOrElse(subFeed.dataObjectId, throw ConfigurationException(s"($id) No output found for result ${subFeed.dataObjectId}. Configured outputs are ${outputs.map(_.id.id).mkString(", ")}."))
      postprocessOutputSubFeedCustomized(subFeed, inputSubFeeds)
    }
  }

  protected def convertToOutputSubFeed(subFeed: S): S = subFeed

  def writeOutputSubFeeds(subFeeds: Seq[S])(implicit context: ActionPipelineContext): Seq[S] = {
    // write and collect all SubFeeds until there is a TaskFailedException, then collect SubFeed without writing.
    // This way metrics from successfully written SubFeeds can be preserved and enriched in TaskFailedException.
    val (outputSubFeeds,taskFailedException,noDataWarning) = outputs.foldLeft((Seq[S](),Option.empty[TaskFailedException],Option.empty[NoDataToProcessWarning])) {
      case ((outputSubFeeds, taskFailedException, noDataWarning), output) =>
        // find SubFeed for output and write it
        val subFeed = subFeeds.find(_.dataObjectId == output.id).getOrElse(throw new IllegalStateException(s"($id) subFeed for output ${output.id} not found"))
        // process this subFeed if there was no failure in previous subFeeds
        if (taskFailedException.isEmpty) {
          logWritingStarted(subFeed)
          val isRecursiveInput = recursiveInputs.exists(_.id == subFeed.dataObjectId)
          try {
            val (outputSubFeed, d) = PerformanceUtils.measureDuration {
              writeSubFeed(subFeed, isRecursiveInput)
            }
            logWritingFinished(outputSubFeed, d)
            (outputSubFeeds :+ outputSubFeed, taskFailedException, noDataWarning)
          } catch {
            // remember NoDataToProcessWarning on main output for later
            case ex: NoDataToProcessWarning if mainOutputId.isEmpty || mainOutputId.contains(output.id) =>
              logNoData(subFeed, isMainSubFeed = true)
              (outputSubFeeds ++ ex.results.toSeq.flatten.map(_.setSkipped().asInstanceOf[S]), taskFailedException, Some(ex))
            // ignore NoDataToProcessWarning if not main output
            case ex: NoDataToProcessWarning =>
              logNoData(subFeed, isMainSubFeed = false)
              (outputSubFeeds ++ ex.results.toSeq.flatten.map(_.setSkipped().asInstanceOf[S]), taskFailedException, noDataWarning)
            // remember taskedFailedException for next iteration and ignore processing of further feeds.
            case ex: TaskFailedException =>
              ex.results.flatMap(_.headOption.flatMap(_.metrics)).foreach{ metrics =>
                val metricsLog = orderMetricsDefault(metrics).map( x => x._1+"="+x._2).mkString(" ")
                logger.warn(s"($id) failed writing to ${subFeed.dataObjectId.id}: " + metricsLog)
              }
              (outputSubFeeds ++ ex.results.toSeq.flatten.map(_.asInstanceOf[S]), Some(ex), noDataWarning)
          }
        } else (outputSubFeeds :+ convertToOutputSubFeed(subFeed), taskFailedException, noDataWarning)
    }
    // if there is a TaskFailedException, enrich it with all results and throw it.
    taskFailedException.foreach(ex => throw ex.copy(results = Some(outputSubFeeds)))
    // if there is a NoDataToProcessWarning, enrich it with all results and throw it.
    noDataWarning.foreach(ex => throw ex.copy(results = Some(outputSubFeeds)))
    // return processed SubFeeds
    outputSubFeeds
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    // check main input/output by triggering lazy values
    prioritizedMainInputCandidates
    mainOutput
  }

  private def validateInputSubFeeds(subFeeds: Seq[SubFeed]): Unit = {
    val inputIds = if (handleRecursiveInputsAsSubFeeds) (inputs ++ recursiveInputs).map(_.id) else inputs.map(_.id)
    val superfluousSubFeeds = subFeeds.map(_.dataObjectId).diff(inputIds)
    val missingSubFeeds = inputIds.diff(subFeeds.map(_.dataObjectId))
    assert(superfluousSubFeeds.isEmpty && missingSubFeeds.isEmpty, s"($id) input SubFeeds must match input DataObjects: superfluous=${superfluousSubFeeds.mkString(",")} missing=${missingSubFeeds.mkString(",")})")
  }

  override final def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = try {
    validateInputSubFeeds(subFeeds)
    // prepare
    var (inputSubFeeds, outputSubFeeds) = prepareInputSubFeeds(subFeeds)
    // transform
    outputSubFeeds = transform(inputSubFeeds, outputSubFeeds)
    // update partition values to output's partition columns and update dataObjectId
    outputSubFeeds = postprocessOutputSubFeeds(outputSubFeeds, inputSubFeeds)
    // return
    outputSubFeeds
  } catch {
    // throw exception with skipped output subfeeds if "no data"
    case ex: NoDataToProcessWarning if ex.results.isEmpty =>
      logger.warn(s"($id) throwing NoDataToProcessWarning in init phase is not a good practice as it prevents checking schema for subsequent actions")
      throw ex.copy(results = Some(ActionHelper.createSkippedSubFeeds(outputs)))
  }

  override final def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = try {
    require(context.isExecPhase, throw new IllegalStateException(s"context.phase=${context.phase} but should be Exec for executing action!"))
    validateInputSubFeeds(subFeeds)
    if (isAsynchronousProcessStarted) return outputs.map(output => SparkSubFeed(None, output.id, Seq())) // empty output subfeeds if asynchronous action started
    // prepare
    var (inputSubFeeds, outputSubFeeds) = prepareInputSubFeeds(subFeeds)
    try {
      // transform
      outputSubFeeds = transform(inputSubFeeds, outputSubFeeds)
      // check and adapt output SubFeeds
      outputSubFeeds = postprocessOutputSubFeeds(outputSubFeeds, inputSubFeeds)
      // write output
      outputSubFeeds = writeOutputSubFeeds(outputSubFeeds)
      // return
      outputSubFeeds
    } catch {
      case ex: NoDataToProcessWarning => throw ex // pass on to outer exception handler
      case ex: TaskFailedException => throw ex // pass on to outer exception handler
      case ex: Exception => throw TaskFailedException(id.id, ex, Some(outputSubFeeds.collect{case x: SubFeed => x}))
    }
  } catch {
    // throw exception with skipped output subfeeds if "no data"
    case ex: NoDataToProcessWarning =>
      val subFeeds = ex.results.toSeq.flatten
      val allSubFeeds = outputs.map(o => subFeeds.find(_.dataObjectId == o.id)
        .getOrElse(ActionHelper.createSkippedSubFeed(o)))
      throw ex.copy(results = Some(allSubFeeds))
  }

  override def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    if (isAsynchronousProcessStarted) return
    super.postExec(inputSubFeeds, outputSubFeeds)
    val mainInput = getMainInput(inputSubFeeds)
    val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId == mainInput.id).get
    val mainOutputSubFeed = outputSubFeeds.find(_.dataObjectId == mainOutput.id).get
    executionMode.foreach(_.postExec(id, mainInput, mainOutput, mainInputSubFeed, mainOutputSubFeed))
  }

  protected def logWritingStarted(subFeed: S)(implicit context: ActionPipelineContext): Unit = {
    // sort partition values for logging
    val sortedPartitionValues = mainOutput match {
      case output: CanHandlePartitions => PartitionValues.sort(output.partitions, subFeed.partitionValues)
      case _ => subFeed.partitionValues
    }
    logger.info(s"($id) start writing to ${subFeed.dataObjectId}" + (if (subFeed.partitionValues.nonEmpty) s", partitionValues ${sortedPartitionValues.mkString(" ")}" else ""))
  }

  protected def logWritingFinished(subFeed: SubFeed, duration: Duration)(implicit context: ActionPipelineContext): Unit = {
    val metrics = subFeed.metrics.getOrElse(Map())
    val metricsLog = orderMetrics(metrics, SortedSet("count", "records_written", "num_tasks"))
      .map( x => x._1+"="+x._2).mkString(" ")
    logger.info(s"($id) finished writing to ${subFeed.dataObjectId.id}: job_duration=$duration " + metricsLog)
  }
  protected def logNoData(subFeed: SubFeed, isMainSubFeed: Boolean)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) got NoDataToProcessWarning when writing to ${subFeed.dataObjectId.id}. ${if (isMainSubFeed) "As this is the main output, Action will be set to skipped." else "As this is not the main output, Action will not be set to skipped."}")
  }

  private def getMainDataObjectCandidates(mainId: Option[DataObjectId], dataObjects: Seq[DataObject], inputOutput: String): Seq[DataObject] = {
    if (mainId.isDefined) {
      // if mainInput is defined -> return only that DataObject
      Seq(dataObjects.find(_.id == mainId.get).getOrElse(throw ConfigurationException(s"($id) main${inputOutput}Id ${mainId.get} not found in ${inputOutput}s")))
    } else {
      // prioritize DataObjects by number of partition columns
      dataObjects.sortBy {
        case x: CanHandlePartitions if !inputIdsToIgnoreFilter.contains(x.id) => x.partitions.size
        case _ => 0
      }.reverse
    }
  }

  /**
   * Updates the partition values of a SubFeed to the partition columns of the given input data object:
   * - remove not existing columns from the partition values
   */
  private[smartdatalake] def updateInputPartitionValues(dataObject: DataObject, subFeed: S, partitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): S = {
    dataObject match {
      case partitionedDO: CanHandlePartitions =>
        // remove superfluous partitionValues
        subFeed.updatePartitionValues(partitionedDO.partitions, newPartitionValues = partitionValues).asInstanceOf[S]
      case _ =>
        subFeed.clearPartitionValues().asInstanceOf[S]
    }
  }

  /**
   * Updates the partition values of a SubFeed to the partition columns of the given output data object:
   * - transform partition values
   * - add run_id_partition value if needed
   * - removing not existing columns from the partition values.
   */
  private def updateOutputPartitionValues(dataObject: DataObject, subFeed: S, partitionValuesTransform: Option[Seq[PartitionValues] => Map[PartitionValues,PartitionValues]] = None)(implicit context: ActionPipelineContext): S = {
    dataObject match {
      case partitionedDO: CanHandlePartitions =>
        // transform partition values
        val newPartitionValues = partitionValuesTransform.map(fn => fn(subFeed.partitionValues).values.toSeq.distinct)
          .getOrElse(subFeed.partitionValues)
        // remove superfluous partitionValues
        subFeed.updatePartitionValues(partitionedDO.partitions, breakLineageOnChange = false, newPartitionValues = Some(newPartitionValues)).asInstanceOf[S]
      case _ =>
        subFeed.clearPartitionValues(breakLineageOnChange = false).asInstanceOf[S]
    }
  }

  private def addRunIdPartitionIfNeeded(dataObject: DataObject, subFeed: S)(implicit context: ActionPipelineContext): S = {
    dataObject match {
      case partitionedDO: CanHandlePartitions =>
        if (partitionedDO.partitions.contains(Environment.runIdPartitionColumnName)) {
          val newPartitionValues = if (subFeed.partitionValues.nonEmpty) subFeed.partitionValues.map(_.addKey(Environment.runIdPartitionColumnName, context.executionId.runId.toString))
          else Seq(PartitionValues(Map(Environment.runIdPartitionColumnName -> context.executionId.runId.toString)))
          subFeed.updatePartitionValues(partitionedDO.partitions, breakLineageOnChange = false, newPartitionValues = Some(newPartitionValues)).asInstanceOf[S]
        } else subFeed
      case _ => subFeed
    }
  }

  protected def validatePartitionValuesExisting(dataObject: DataObject with CanHandlePartitions, subFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    // Existing partitions can only be checked if Action is at start of the DAG or if we are in Exec phase (previous Actions have been executed)
    if (subFeed.partitionValues.nonEmpty && (context.isExecPhase || subFeed.isDAGStart) && !subFeed.isSkipped) {
      // filter partition value with keys that are a valid init of partition columns -> otherwise it can not be checked if the partition exists
      val inits = dataObject.partitions.inits.map(_.toSet)
      val validInitPartitionValues = subFeed.partitionValues.filter(pv => inits.contains(pv.keys))
      val expectedPartitions = dataObject.filterExpectedPartitionValues(validInitPartitionValues)
      val missingPartitionValues = if (expectedPartitions.nonEmpty) PartitionValues.checkExpectedPartitionValues(dataObject.listPartitions, expectedPartitions) else Seq()
      assert(missingPartitionValues.isEmpty, s"($id) partitions ${missingPartitionValues.mkString(", ")} missing for ${dataObject.id}")
    }
  }

  /**
   * Implement additional preprocess logic for SubFeeds before transformation
   * Can be implemented by subclass.
   * @param ignoreFilter If filters should be ignored for this feed
   * @param isRecursive If subfeed is recursive (input & output)
   */
  protected def preprocessInputSubFeedCustomized(subFeed: S, ignoreFilter: Boolean, isRecursive: Boolean)(implicit context: ActionPipelineContext): S = subFeed

  /**
   * Implement additional processing logic for SubFeeds after transformation.
   * Can be implemented by subclass.
   */
  protected def postprocessOutputSubFeedCustomized(subFeed: S, inputSubFeeds: Seq[S])(implicit context: ActionPipelineContext): S = subFeed

  /**
   * Transform partition values.
   * Can be implemented by subclass.
   */
  private[smartdatalake] def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = PartitionValues.oneToOneMapping(partitionValues)

  /**
   * Transform subfeed content
   * To be implemented by subclass.
   */
  protected def transform(inputSubFeeds: Seq[S], outputSubFeeds: Seq[S])(implicit context: ActionPipelineContext): Seq[S]

  /**
   * Write subfeed data to output.
   * To be implemented by subclass.
   * @param isRecursive If subfeed is recursive (input & output)
   * @return false if there was no data to process, otherwise true.
   */
  protected def writeSubFeed(subFeed: S, isRecursive: Boolean)(implicit context: ActionPipelineContext): S

}
object ActionSubFeedsImpl {
  type MetricsMap = Map[String, Any]
}

case class SubFeedExpressionData(partitionValues: Seq[Map[String,String]], isDAGStart: Boolean, isSkipped: Boolean, metrics: Map[String,String])
case class SubFeedsExpressionData(inputSubFeeds: Map[String, SubFeedExpressionData])
object SubFeedsExpressionData {
  def fromSubFeeds(subFeeds: Seq[SubFeed]): SubFeedsExpressionData = {
    SubFeedsExpressionData(subFeeds.map(subFeed => (subFeed.dataObjectId.id, SubFeedExpressionData(subFeed.partitionValues.map(_.getMapString), subFeed.isDAGStart, subFeed.isSkipped, subFeed.metrics.getOrElse(Map()).mapValues(_.toString).toMap))).toMap)
  }
}