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
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame, Observation}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}

import java.util.UUID
import scala.reflect.runtime.universe.typeOf

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
   * Map of expectation name and definition to evaluate on write, see [[Expectation]] for details.
   * Expectations are aggregation expressions defined on dataset-level and evaluated on every write.
   * By default their result is logged with level info (ok) and error (failed), but this can be customized to be logged as warning.
   * In case of failed expectations logged as error, an exceptions is thrown and further processing is stopped.
   * Note that the exception is thrown after writing to the DataObject is finished.
   *
   * The following expectations names are reserved to create default metrics and should not be used:
   * - count
   */
  def expectations: Seq[Expectation]

  def setupConstraintsAndExpectations(df: GenericDataFrame)(implicit context: ActionPipelineContext): (GenericDataFrame, Option[Observation]) = {
    val dfConstraints = internalSetupConstraintsValidation(df)
    internalSetupExpectations(dfConstraints, context.phase == ExecutionPhase.Exec)
  }

  private val defaultExpectations = Seq(SQLExpectation(name = "count", aggExpression = "count(*)" ))

  def validateExpectations(metrics: Map[String, _], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    // the evaluation of expectations is made with Spark expression
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(typeOf[SparkSubFeed])
    // evaluate expectations using a dummy DataFrame
    val defaultExpressionData = DefaultExpressionData.from(context, Seq())
    val validationResults = expectations.flatMap { expectation =>
      expectation.getValidationErrorColumn(this.id, metrics, partitionValues)
        .map { col =>
          val sparkCol = col.asInstanceOf[SparkColumn].inner
          val errorMsg = SparkExpressionUtil.evaluate[DefaultExpressionData, String](this.id, Some("expectations"), sparkCol, defaultExpressionData)
          (expectation, errorMsg)
        }
    }.toMap.filter(_._2.nonEmpty).mapValues(_.get) // keep only failed results
    // log all failed results (before throwing exception)
    validationResults
      .foreach(result => result._1.failedSeverity match {
        case ExpectationSeverity.Warn => logger.warn(result._2)
        case ExpectationSeverity.Error => logger.error(result._2)
      })
    // throw exception on error
    validationResults.filterKeys(_.failedSeverity == ExpectationSeverity.Error)
      .foreach(result => throw ExpectationValidationException(result._2))
  }

  private def internalSetupConstraintsValidation(df: GenericDataFrame): GenericDataFrame = {
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

  private def internalSetupExpectations(df: GenericDataFrame, isExecPhase: Boolean): (GenericDataFrame, Option[Observation]) = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    if (expectations.nonEmpty) {
      val expectationColumns = (defaultExpectations ++ expectations).flatMap(_.getAggExpressionColumns(this.id))
      val (dfObserved, observation) = df.setupObservation(this.id + "-" + UUID.randomUUID(), expectationColumns, isExecPhase)
      (dfObserved, Some(observation))
    } else (df, None)
  }
}