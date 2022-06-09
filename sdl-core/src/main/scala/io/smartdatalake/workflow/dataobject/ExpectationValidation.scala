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
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame, Observation}
import io.smartdatalake.workflow.dataobject.ExpectationScope.{ExpectationScope, Job}
import io.smartdatalake.workflow.dataobject.ExpectationSeverity.ExpectationSeverity
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
   */
  def expectations: Seq[Expectation]

  def setupConstraintsAndExpectations(df: GenericDataFrame)(implicit context: ActionPipelineContext): (GenericDataFrame, Option[Observation]) = {
    val dfConstraints = internalSetupConstraintsValidation(df)
    internalSetupExpectations(dfConstraints, context.phase == ExecutionPhase.Exec)
  }

  def validateExpectations(metrics: Map[String, _])(implicit context: ActionPipelineContext): Unit = {
    // the evaluation of expectations is made with Spark expression
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(typeOf[SparkSubFeed])
    // evaluate expectations using a dummy DataFrame
    val defaultExpressionData = DefaultExpressionData.from(context, Seq())
    val validationResults = expectations.map { expectation =>
      val value = metrics.getOrElse(expectation.name, throw new IllegalStateException(s"($id) Metric for expectation ${expectation.name} not found for validation"))
      val validationErrorExpression = expectation.getValidationErrorColumn(this.id, value).asInstanceOf[SparkColumn].inner
      val errorMsg = SparkExpressionUtil.evaluate[DefaultExpressionData, String](this.id, Some("expectations"), validationErrorExpression, defaultExpressionData)
      (expectation, errorMsg)
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
      // determine columns to log for identifying records
      val traceCols: Seq[String] = this match {
        // use primary key if defined
        case tableDataObject: TableDataObject if tableDataObject.table.primaryKey.isDefined => tableDataObject.table.primaryKey.get
        // else log all columns
        case _ => df.schema.columns
      }
      def mkStringCol(cols: Seq[String]): GenericColumn = concat(col(cols.head) +: cols.tail.flatMap(c => Seq(lit(","), col(c))):_*)
      // add validation as additional column
      val validationErrorColumns = constraints.map(_.getValidationExceptionColumn(this.id, mkStringCol(traceCols)))
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
      val expectationColumns = expectations.map(_.getAggExpressionColumn(this.id))
      val (dfObserved, observation) = df.setupObservation(this.id + "-" + UUID.randomUUID(), expectationColumns, isExecPhase)
      (dfObserved, Some(observation))
    } else (df, None)
  }
}

case class ConstraintValidationException(msg: String) extends Exception(msg)
case class ExpectationValidationException(msg: String) extends Exception(msg)

/**
 * Definition of row-level constraint to validate.
 *
 * @param expression SQL expression to evaluate on every row.
 * @param comparisonOperator Comparison operator to use for comparison of evaluated expression value against expected value. Default is '='.
 * @param expectation SQL literal to define expected value for validation. Default is true.
 */
case class Constraint(name: String, description: Option[String] = None, expression: String, comparisonOperator: String = "=", expectation: String = "true") {
  def getExpressionColumn(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions): GenericColumn = {
    try {
      functions.expr(expression)
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Constraint $name: cannot parse SQL expression '$expression'", Some(s"constraints.$name.expression"), e)
    }
  }
  def getValidationColumn(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions): GenericColumn = {
    val validationExpr = s"$expression $comparisonOperator $expectation"
    try {
      functions.expr(validationExpr)
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Constraint $name: cannot parse validation expression '$validationExpr'", Some(s"constraints.$name"), e)
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, traceInfo: GenericColumn)(implicit functions: DataFrameFunctions): GenericColumn = {
    val validationCol = getValidationColumn(dataObjectId)
    val expressionCol = getExpressionColumn(dataObjectId)
    import functions._
    try {
      when(not(validationCol), concat(lit(s"($dataObjectId) Constraint '$name' failed - actual: "), expressionCol, lit(s", expectation: $comparisonOperator $expectation, record: "), traceInfo))
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Constraint $name: cannot create validation error column", Some(s"constraints.$name"), e)
    }
  }
  def getValidationExceptionColumn(dataObjectId: DataObjectId, traceInfo: GenericColumn)(implicit functions: DataFrameFunctions): GenericColumn = {
    import functions._
    when(not(getValidationColumn(dataObjectId)), raise_error(getValidationErrorColumn(dataObjectId, traceInfo)))
  }
}

/**
 * Definition of expectation to evaluate on dataset-level.
 *
 * @param aggExpression SQL aggregate expression to evaluate on dataset, e.g. count(*).
 * @param comparisonOperator Comparison operator to use for comparison of evaluated aggregate expression value against expected value.
 * @param expectation SQL literal to define expected value for validation.
 * @param scope The aggregation scope used to evaluate the aggregate expression.
 */
case class Expectation(name: String, description: Option[String] = None, aggExpression: String, comparisonOperator: String, expectation: String, scope: ExpectationScope = Job, failedSeverity: ExpectationSeverity = ExpectationSeverity.Error ) {
  def getAggExpressionColumn(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions): GenericColumn = {
    try {
      functions.expr(aggExpression).as(name)
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse SQL aggExpression '$aggExpression'", Some(s"expectations.$name.aggExpression"), e)
    }
  }
  def getValidationColumn(dataObjectId: DataObjectId, value: Any)(implicit functions: DataFrameFunctions): GenericColumn = {
    val valueStr = value match {
      case str: String => s"'$str'"
      case x => x.toString
    }
    val validationExpr = s"$valueStr $comparisonOperator $expectation"
    try {
      functions.expr(validationExpr)
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse validation expression '$validationExpr'", Some(s"expectations.$name"), e)
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, value: Any)(implicit functions: DataFrameFunctions): GenericColumn = {
    val validationCol = getValidationColumn(dataObjectId, value)
    import functions._
    try {
      when(not(validationCol), lit(s"Expectation '$name' failed - actual: $value, expectation: $comparisonOperator $expectation"))
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot create validation error column", Some(s"expectations.$name"), e)
    }
  }
}


/**
 * Definition of aggregation scope used to evaluate an expectations aggregate expression.
 */
// TODO: implement different scopes...
object ExpectationScope extends Enumeration {
  type ExpectationScope = Value

  /**
   * Expectation is evaluated against dataset written to DataObject by the current job.
   * For Spark this can be implemented using Observations, which does not cost performance.
   */
  val Job: Value = Value("Job")

  /**
   * Expectation is evaluated against dataset written to DataObject by the current job, grouped by the partitions processed.
   */
  val JobPartition: Value = Value("JobPartition")

  /**
   * Expectation is evaluated against all data in DataObject.
   */
  val All: Value = Value("All")
}

object ExpectationSeverity extends Enumeration {
  type ExpectationSeverity = Value

  /**
   * Failure of expectation is treated as error.
   */
  val Error: Value = Value("Error")

  /**
   * Failure of expectation is treated as warning.
   */
  val Warn: Value = Value("Warn")
}