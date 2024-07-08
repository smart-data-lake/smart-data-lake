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

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.PushPredicateThroughTolerantCollectMetricsRuleObject.tolerantMetricsMarker
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.spark.SparkColumn
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.ExpectationScope
import io.smartdatalake.workflow.dataobject.ExpectationValidation.defaultExpectations
import io.smartdatalake.workflow.dataobject.expectation.{BaseExpectation, Expectation, ExpectationScope, ExpectationSeverity, ExpectationValidationException, SQLExpectation}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import java.util.UUID

/**
 * A trait that allows for optional constraint validation and expectation evaluation on write when implemented by a [[DataObject]].
 */
private[smartdatalake] trait ExpectationValidation { this: DataObject with SmartDataLakeLogger =>

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

  def setupConstraintsAndJobExpectations(df: GenericDataFrame, defaultExpectationsOnly: Boolean = false, predicateTolerant: Boolean = false, additionalJobAggExpressionColumns: Seq[GenericColumn] = Seq())(implicit context: ActionPipelineContext): (GenericDataFrame, DataFrameObservation) = {
    // add constraint validation column
    val dfConstraints = if (defaultExpectationsOnly) df else setupConstraintsValidation(df)
    // setup job expectations as DataFrame observation
    val jobExpectations = expectations.filter(_.scope == ExpectationScope.Job)
    val (dfJobExpectations, observation) = {
      implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
      val expectationColumns = (defaultExpectations ++ (if (defaultExpectationsOnly) Seq() else jobExpectations)).flatMap(_.getAggExpressionColumns(this.id)) ++ additionalJobAggExpressionColumns
      setupObservation(dfConstraints, expectationColumns, context.isExecPhase, predicateTolerant)
    }
    // add caching if there are expectations with scope != job
    if (expectations.exists(_.scope != ExpectationScope.Job)) dfJobExpectations.cache
    (dfJobExpectations, observation)
  }

  /**
   * Collect metrics for expectations with scope = JobPartition
   */
  def getScopeJobPartitionAggMetrics(dfAll: GenericDataFrame, partitionValues: Seq[PartitionValues], expectationsToValidate: Seq[BaseExpectation])(implicit context: ActionPipelineContext): Map[String,_] = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfAll.subFeedType)
    import ExpectationValidation._
    val aggExpressions = expectationsToValidate.filter(_.scope == ExpectationScope.JobPartition)
      .flatMap(e => e.getAggExpressionColumns(this.id))
    if (aggExpressions.nonEmpty) {
      this match {
        case partitionedDataObject: DataObject with CanHandlePartitions if partitionedDataObject.partitions.nonEmpty =>
          logger.info(s"($id) collecting aggregate column metrics for expectations with scope JobPartition")
          val dfMetrics = dfAll.groupBy(partitionedDataObject.partitions.map(functions.col)).agg(deduplicate(aggExpressions))
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
  def getScopeAllAggMetrics(dfAll: GenericDataFrame, expectationsToValidate: Seq[BaseExpectation])(implicit context: ActionPipelineContext): Map[String,_] = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfAll.subFeedType)
    val aggExpressions = expectationsToValidate.filter(x => x.scope == ExpectationScope.All).flatMap(_.getAggExpressionColumns(this.id))
    if (aggExpressions.nonEmpty) {
      logger.info(s"($id) collecting aggregate column metrics for expectations with scope All")
      val dfMetrics = dfAll.agg(deduplicate(aggExpressions))
      dfMetrics.collect.map(row => dfMetrics.schema.columns.zip(row.toSeq).toMap).head
    } else Map()
  }

  def extractMetrics(df: GenericDataFrame, aggExpressions: Seq[GenericColumn], scope: ExpectationScope): Map[String,_] = {
    if (aggExpressions.nonEmpty) {
      logger.info(s"($id) collecting aggregate column metrics for expectations with scope $scope")
      val dfMetrics = df.agg(deduplicate(aggExpressions))
      dfMetrics.collect.map(row => dfMetrics.schema.columns.zip(row.toSeq).toMap).head
    } else Map()
  }

  def deduplicate(aggExpressions: Seq[GenericColumn]): Seq[GenericColumn] = {
    aggExpressions.groupBy(_.getName).map(_._2.head).toSeq // remove potential duplicates
  }

  def validateExpectations(dfJob: GenericDataFrame, dfAll: GenericDataFrame, partitionValues: Seq[PartitionValues], scopeJobAndInputMetrics: Map[String, _], additionalExpectations: Seq[BaseExpectation] = Seq(), enrichmentFunc: Map[String,_] => Map[String,_])(implicit context: ActionPipelineContext): (Map[String, _], Seq[ExpectationValidationException]) = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfJob.subFeedType)
    val expectationsToValidate = expectations ++ additionalExpectations
    // collect metrics with scope = JobPartition
    val scopeJobPartitionMetrics = getScopeJobPartitionAggMetrics(dfJob, partitionValues, expectationsToValidate)
    // collect metrics with scope = All
    val scopeAllMetrics = getScopeAllAggMetrics(dfAll, expectationsToValidate)
    // collect custom metrics
    val customMetrics = expectationsToValidate.flatMap(e => e.getCustomMetrics(this.id, if (e.scope==ExpectationScope.All) dfAll else dfJob))
    // enrich metrics
    val metrics = enrichmentFunc(scopeJobAndInputMetrics ++ scopeJobPartitionMetrics ++ scopeAllMetrics ++ customMetrics)
    // evaluate expectations using dummy ExpressionData
    val defaultExpressionData = DefaultExpressionData.from(context, Seq())
    val (expectationValidationCols, updatedMetrics) = expectationsToValidate.foldLeft(Seq[(BaseExpectation,SparkColumn)](), metrics) {
      case ((cols, metrics), expectation) =>
        val (newCols, updatedMetrics) = expectation.getValidationErrorColumn(this.id, metrics, partitionValues)
        (cols ++ newCols.map(c => (expectation,c)), updatedMetrics)
    }
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
    if (errors.nonEmpty) logger.error(s"($id) Expectation validation failed with metrics "+updatedMetrics.map{case(k,v) => s"$k=$v"}.mkString(" "))
    val exceptions = errors.map(result => ExpectationValidationException(result._2)).toSeq
    // return consolidated and updated metrics
    (updatedMetrics, exceptions)
  }

  private def setupConstraintsValidation(df: GenericDataFrame): GenericDataFrame = {
    if (constraints.nonEmpty) {
      implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
      import functions._
      // use primary key if defined
      val pkCols = Some(this).collect{case tdo: TableDataObject => tdo}.flatMap(_.table.primaryKey)
      // as alternative search all columns with simple datatype
      val dfSimpleCols = df.schema.filter(_.dataType.isSimpleType).columns
      // add validation as additional column
      val validationErrorColumns = constraints.map(_.getValidationExceptionColumn(this.id, pkCols, dfSimpleCols))
      val dfErrors = df
        .withColumn("_validation_errors", array_construct_compact(validationErrorColumns: _*))
      // use column in where condition to avoid elimination by optimizer before dropping the column again.
      dfErrors
        .where(size(col("_validation_errors")) < lit(constraints.size+1)) // this is always true - but we want to force evaluating column "_validation_errors" to throw exceptions
        .drop("_validation_errors")
    } else df
  }

  protected def forceGenericObservation = false // can be overridden by subclass
  private def setupObservation(df: GenericDataFrame, expectationColumns: Seq[GenericColumn], isExecPhase: Boolean, predicateTolerant: Boolean = false): (GenericDataFrame, DataFrameObservation) = {
    val observationName = this.id.id + "#" + UUID.randomUUID() + (if (predicateTolerant) tolerantMetricsMarker else "")
    val (dfObserved, observation) = df.setupObservation(observationName, expectationColumns, isExecPhase, forceGenericObservation)
    (dfObserved, observation)
  }
}

object ExpectationValidation {
  private[smartdatalake] final val partitionDelimiter = "#"
  private[smartdatalake] val defaultExpectations = Seq(SQLExpectation(name = "count", aggExpression = "count(*)" ))
}