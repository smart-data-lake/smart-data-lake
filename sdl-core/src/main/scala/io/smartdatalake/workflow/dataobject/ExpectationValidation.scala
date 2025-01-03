/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.metrics.MetricsUtil.orderMetrics
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.PushPredicateThroughTolerantCollectMetricsRuleObject.pushDownTolerantMetricsMarker
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.spark.SparkColumn
import io.smartdatalake.workflow.dataobject.ExpectationValidation.defaultExpectations
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.ExpectationScope
import io.smartdatalake.workflow.dataobject.expectation._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import java.util.UUID
import scala.reflect.runtime.universe.Type

/**
 * A trait that allows for optional constraint validation and expectation evaluation and validation on write when implemented by a [[DataObject]].
 *
 * An expectation validation means that the evaluated metric value is compared against a condition set the the user as `expectation` attribute. This could be for instance {{{<value> <= 20}}}
 */
private[smartdatalake] trait ExpectationValidation {
  this: DataObject with SmartDataLakeLogger =>

  /**
   * List of constraint definitions to validate on write, see [[Constraint]] for details.
   * Constraints are expressions defined on row-level and validated during evaluation of the DataFrame.
   * If validation fails an exception is thrown and further processing is stopped.
   * Note that this is done while evaluating the DataFrame when writing to the DataObject. It doesn't need a separate action on the DataFrame.
   * If a constraint validation for a row fails, it will throw an exception and abort writing to the DataObject.
   */
  // TODO: can we avoid Spark retries when validation exceptions are thrown in Spark tasks?
  def constraints: Seq[Constraint]

  /**
   * List expectation definitions to evaluate on write, see [[Expectation]] for details.
   * Expectations are aggregation expressions defined on dataset-level and evaluated on every write.
   * By default their result is logged with level info (ok) and error (failed), but this can be customized to be logged as warning.
   * In case of failed expectations logged as error, an exceptions is thrown and further processing is stopped.
   * Note that the exception is thrown after writing to the DataObject is finished.
   *
   * The following expectations names are reserved to create default metrics and should not be used:
   * - count
   */
  def expectations: Seq[Expectation]

  /**
   * Add constraints validation and metrics collection for Expectations with scope=Job to DataFrame.
   *
   * @param defaultExpectationsOnly if true only default exepctations, e.g. count, is added to the DataFrame, and no constraints are validated.
   *                                Set defaultExpectationsOnly=true for input DataObjects which are also written by SDLB, as constraints and expectations are then validated on write.
   * @param pushDownTolerant
   * @param additionalJobAggExpressionColumns
   */
  def setupConstraintsAndJobExpectations(df: GenericDataFrame, defaultExpectationsOnly: Boolean = false, pushDownTolerant: Boolean = false, additionalJobAggExpressionColumns: Seq[GenericColumn] = Seq(), forceGenericObservation: Boolean = false)(implicit context: ActionPipelineContext): (GenericDataFrame, Seq[DataFrameObservation]) = {
    // add constraint validation column
    val dfConstraints = if (defaultExpectationsOnly) df else setupConstraintsValidation(df)
    // setup job expectations as DataFrame observation
    val jobExpectations = expectations.filter(_.scope == ExpectationScope.Job)
    val (dfJobExpectations, observations) = {
      implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
      // prepare aggregation columns
      val defaultAggColumns = defaultExpectations.flatMap(_.getAggExpressionColumns(this.id))
      val jobAggColumns = (if (defaultExpectationsOnly) Seq() else jobExpectations).flatMap(_.getAggExpressionColumns(this.id)) ++ additionalJobAggExpressionColumns
      // try to calculate defaultAggColumns always as non-generic observation, so that there is a SparkObservation to catch additional input and custom Spark observations
      val forceGenericObservationCons = forceGenericObservation || jobExpectations.exists(!_.calculateAsJobDataFrameObservation)
      val normalAggColumns = defaultAggColumns ++ (if (!forceGenericObservationCons) jobAggColumns else Seq())
      val forceGenericAggColumns = if (forceGenericObservationCons) jobAggColumns else Seq()
      val (dfObserved, normalObservation) = setupObservation(dfConstraints, normalAggColumns, context.isExecPhase, pushDownTolerant, forceGenericObservation = false)
      val genericObservation = if (forceGenericAggColumns.nonEmpty) Some(setupObservation(dfConstraints, forceGenericAggColumns, context.isExecPhase, pushDownTolerant, forceGenericObservation = true)._2) else None
      // if there are now two GenericCalculatedObservations, combine them to avoid duplicate query execution
      val observations = (normalObservation, genericObservation) match {
        case (o1: GenericCalculatedObservation, Some(o2: GenericCalculatedObservation)) => Seq(GenericCalculatedObservation(o1.df, (o1.aggregateColumns ++ o2.aggregateColumns): _*))
        case (o1, o2) => Seq(Some(o1), o2).flatten
      }
      (dfObserved, observations)
    }
    (dfJobExpectations, observations)
  }

  /**
   * Collect metrics for expectations with scope = JobPartition
   */
  def getScopeJobPartitionAggMetrics(subFeedType: Type, dfJob: Option[GenericDataFrame], partitionValues: Seq[PartitionValues], expectationsToValidate: Seq[BaseExpectation])(implicit context: ActionPipelineContext): Map[String, _] = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedType)
    import ExpectationValidation._
    val aggExpressions = expectationsToValidate.filter(_.scope == ExpectationScope.JobPartition)
      .flatMap(e => e.getAggExpressionColumns(this.id))
    if (aggExpressions.nonEmpty) {
      this match {
        case partitionedDataObject: DataObject with CanHandlePartitions if partitionedDataObject.partitions.nonEmpty =>
          if (dfJob.isEmpty) throw ConfigurationException(s"($id) Can not calculate metrics for Expectations with scope=JobPartition on read")
          logger.info(s"($id) collecting aggregate column metrics ${aggExpressions.map(_.getName.getOrElse("<unnamed>")).distinct.mkString(", ")} for expectations with scope JobPartition")
          val dfMetrics = dfJob.get.groupBy(partitionedDataObject.partitions.map(functions.col)).agg(deduplicate(aggExpressions))
          val rawMetrics = dfMetrics.collect.map(row => dfMetrics.schema.columns.zip(row.toSeq).toMap)
          val metrics = rawMetrics.flatMap { rawMetrics =>
            val partitionValuesStr = partitionedDataObject.partitions.map(rawMetrics).map(_.toString).mkString(partitionDelimiter)
            rawMetrics.filterKeys(!partitionedDataObject.partitions.contains(_))
              .map { case (name, value) => (name + partitionDelimiter + partitionValuesStr, value) }
          }
          metrics.toMap
        case _ => throw new IllegalStateException(s"($id) Expectation with scope = JobPartition defined for unpartitioned DataObject")
      }
    } else Map()
  }

  /**
   * Collect metrics for expectations with scope = All
   */
  def getScopeAllAggMetrics(dfAll: GenericDataFrame, expectationsToValidate: Seq[BaseExpectation], existingMetrics: MetricsMap)(implicit context: ActionPipelineContext): Map[String, _] = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfAll.subFeedType)
    val aggExpressions = expectationsToValidate.filter(x => x.scope == ExpectationScope.All).flatMap(_.getAggExpressionColumns(this.id))
      .filter(!_.getName.exists(existingMetrics.contains))
    calculateMetrics(dfAll, aggExpressions, ExpectationScope.All)
  }

  def calculateMetrics(df: GenericDataFrame, aggExpressions: Seq[GenericColumn], scope: ExpectationScope): Map[String, _] = {
    if (aggExpressions.nonEmpty) {
      logger.info(s"($id) collecting aggregate column metrics ${aggExpressions.map(_.getName.getOrElse("<unnamed>")).distinct.mkString(", ")} for expectations with scope $scope")
      val dfMetrics = df.agg(deduplicate(aggExpressions))
      dfMetrics.collect.map(row => dfMetrics.schema.columns.zip(row.toSeq).toMap).head
        .mapValues(v => Option(v).getOrElse(None)).toMap // if value is null convert to None
    } else Map()
  }

  def deduplicate(aggExpressions: Seq[GenericColumn]): Seq[GenericColumn] = {
    aggExpressions.groupBy(_.getName).map(_._2.head).toSeq // remove potential duplicates
  }

  def validateExpectations(subFeedType: Type, dfJob: Option[GenericDataFrame], dfAll: GenericDataFrame, partitionValues: Seq[PartitionValues], scopeJobAndInputMetrics: Map[String, _], additionalExpectations: Seq[BaseExpectation] = Seq(), enrichmentFunc: Map[String, _] => Map[String, _], scopeJobOnly: Boolean = false, loggerContext: String)(implicit context: ActionPipelineContext): (Map[String, _], Seq[ExpectationValidationException]) = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedType)
    val expectationsToValidate = (expectations ++ additionalExpectations)
      .filter(e => !scopeJobOnly || e.scope == ExpectationScope.Job)
    if (logger.isDebugEnabled()) logger.debug(s"($id) validating $loggerContext expectations ${expectationsToValidate.map(e => s"${e.name}/${e.scope}").mkString(" ")}. Got scopeJobAndInputMetrics: ${scopeJobAndInputMetrics.mapValues(_.toString).map { case (k, v) => s"$k=$v" }.mkString(" ")}")
    // collect metrics with scope = JobPartition
    val scopeJobPartitionMetrics = getScopeJobPartitionAggMetrics(subFeedType, dfJob, partitionValues, expectationsToValidate)
    // collect metrics with scope = All
    val scopeAllMetrics = getScopeAllAggMetrics(dfAll, expectationsToValidate, scopeJobAndInputMetrics)
    // collect custom metrics
    val customMetrics = expectationsToValidate.flatMap(e => e.getCustomMetrics(this.id, if (e.scope == ExpectationScope.All) Some(dfAll) else dfJob))
    // enrich metrics
    val metrics = enrichmentFunc(scopeJobAndInputMetrics ++ scopeJobPartitionMetrics ++ scopeAllMetrics ++ customMetrics)
    // evaluate expectations using dummy ExpressionData
    if (logger.isDebugEnabled) logger.debug(s"($id) initial metrics before validation: ${metrics.map { case (k, v) => s"$k=$v" }.mkString(" ")}")
    val defaultExpressionData = DefaultExpressionData.from(context, Seq())
    val (expectationValidationCols, updatedMetrics) = expectationsToValidate.foldLeft(Seq[(BaseExpectation, SparkColumn)](), metrics) {
      case ((cols, metrics), expectation) =>
        val (newCols, updatedMetrics) = expectation.getValidationErrorColumn(this.id, metrics, partitionValues)
        (cols ++ newCols.map(c => (expectation, c)), updatedMetrics)
    }
    if (logger.isDebugEnabled) logger.debug(s"($id) updated metrics before validation: ${metrics.map { case (k, v) => s"$k=$v" }.mkString(" ")}")
    // the evaluation of expectations is made using Spark expressions
    val validationResults = expectationValidationCols.map {
      case (expectation, col) =>
        val errorMsg = SparkExpressionUtil.evaluate[DefaultExpressionData, String](this.id, Some("expectations"), col.inner, defaultExpressionData)
        (expectation, errorMsg)
    }.toMap.filter(_._2.nonEmpty).mapValues(_.get) // keep only failed results
    // log all failed results (before throwing exception)
    validationResults
      .foreach(result => result._1.failedSeverity match {
        case ExpectationSeverity.Warn => logger.warn(s"($id) ${result._2}")
        case ExpectationSeverity.Error => logger.error(s"($id) ${result._2}")
      })
    // throw exception on error, but log metrics before
    val errors = validationResults.filterKeys(_.failedSeverity == ExpectationSeverity.Error)
    if (errors.nonEmpty) logger.error(s"($id) Expectation validation failed with metrics " + orderMetrics(updatedMetrics).map { case (k, v) => s"$k=$v" }.mkString(" "))
    val exceptions = errors.map(result => ExpectationValidationException(result._2)).toSeq
    // return consolidated and updated metrics
    (updatedMetrics, exceptions)
  }

  private def setupConstraintsValidation(df: GenericDataFrame): GenericDataFrame = {
    if (constraints.nonEmpty) {
      implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
      import functions._
      // use primary key if defined
      val pkCols = Some(this).collect { case tdo: TableDataObject => tdo }.flatMap(_.table.primaryKey)
      // as alternative search all columns with simple datatype
      val dfSimpleCols = df.schema.filter(_.dataType.isSimpleType).columns
      // add validation as additional column
      val validationErrorColumns = constraints.map(_.getValidationExceptionColumn(this.id, pkCols, dfSimpleCols))
      val dfErrors = df
        .withColumn("_validation_errors", array_construct_compact(validationErrorColumns: _*))
      // use column in where condition to avoid elimination by optimizer before dropping the column again.
      dfErrors
        .where(size(col("_validation_errors")) < lit(constraints.size + 1)) // this is always true - but we want to force evaluating column "_validation_errors" to throw exceptions
        .drop("_validation_errors")
    } else df
  }

  protected def forceGenericObservation = false // can be overridden by subclass

  private def setupObservation(df: GenericDataFrame, expectationColumns: Seq[GenericColumn], isExecPhase: Boolean, pushDownTolerant: Boolean = false, forceGenericObservation: Boolean = false): (GenericDataFrame, DataFrameObservation) = {
    val observationName = this.id.id + "#" + UUID.randomUUID() + (if (pushDownTolerant) pushDownTolerantMetricsMarker else "")
    val (dfObserved, observation) = df.setupObservation(observationName, expectationColumns, isExecPhase, this.forceGenericObservation || forceGenericObservation)
    (dfObserved, observation)
  }
}

object ExpectationValidation {
  private[smartdatalake] final val partitionDelimiter = "#"
  private[smartdatalake] val defaultExpectations = Seq(SQLExpectation(name = "count", aggExpression = "count(*)"))
}