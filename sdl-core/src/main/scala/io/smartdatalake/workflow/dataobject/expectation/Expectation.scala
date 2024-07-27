/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject.expectation

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config._
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkColumn
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.ExpectationValidation
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.{ExpectationScope, Job}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity
import org.apache.spark.sql.Column

import scala.reflect.{ClassTag, classTag}

/**
 * Trait to define expectations against a dataset.
 * Expectations are checks based on aggregates over all rows of a dataset.
 * Through setting the scope of an expectation, it can be controlled if the aggregate is evaluated per job, partition or the whole content of the table.
 */
trait BaseExpectation {
  /**
   * The name of the expectation
   */
  def name: String
  /**
   * Optional detailed description of the expectation
   */
  def description: Option[String]
  /**
   * Severity if expectation fails - can be Error (default) or Warn.
   * If set to Error, execution will fail, otherwise there will be just a warning logged.
   */
  def failedSeverity: ExpectationSeverity
  /**
   * The aggregation scope used to evaluate the aggregate expression.
   * Default is 'Job', which evaluates the records transformed by the current job. This is implemented without big performance impacts on Spark.
   * Other options are 'All' and 'JobPartition', which are implemented by reading the output data again.
   */
  def scope: ExpectationScope = Job

  /**
   * Create aggregate expressions that will be evaluated on output DataObject to create metrics.
   * @return a list of aggregate expressions
   */
  def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn]

  /**
   * Define a custom evaluation of the DataObjects data and return metrics, as alternative to creating aggregate expressions (by defining getAggExpressionColumns).
   * @param df DataFrame of the processed DataObject filtered by scope.
   */
  def getCustomMetrics(dataObjectId: DataObjectId, df: Option[GenericDataFrame])(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Map[String,_] = Map()

  /**
   * Create columns to validate the expectation and return error message if failed.
   * Can cleanup metrics and return them if artifical metrics have been introduced, like for SQLFractionExpectation.
   */
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn],Map[String,_])

  /**
   * Control if metrics are calculated as DataFrame observation for Spark.
   * This can only be done for scope=Job, but implementations might be more restrictive.
   */
  def calculateAsJobDataFrameObservation: Boolean = scope == Job

  // helpers
  protected def getMetric[T: ClassTag](dataObjectId: DataObjectId, metrics: Map[String,Any], key: String): T = {
    val cls = classTag[T].runtimeClass
    metrics.getOrElse(key, throw new IllegalStateException(s"($dataObjectId) Metric '$key' for expectation '${name}' not found for validation")) match {
      case Some(x: T) => x
      case x: T => x
      case x => throw new IllegalStateException(s"($dataObjectId) Metric '$key' for expectation ${name} is '$x' instead of type ${cls.getSimpleName}")
    }
  }
}

/**
 * A trait to mark Expectations to be used in DataObjects.
 *
 * As this is the normal use of Expectations, the trait is named just Expectation and not DataObjectExpectation
 */
trait Expectation extends BaseExpectation with ParsableFromConfig[Expectation] with ConfigHolder with SmartDataLakeLogger

/**
 * A trait to mark Expectations to be used in Actions
 * ActionExpectations can measure additional metrics on input DataObjects.
 */
trait ActionExpectation extends BaseExpectation with ParsableFromConfig[ActionExpectation] with ConfigHolder with SmartDataLakeLogger {
  /**
   * Create aggregate expressions that will be evaluated on input DataObject to create metrics.
   * If the column name includes {{{#<DataObjectId>}}} as suffix, the aggregate expression will be evaluated on the corresponding DataObject input, otherwise on the main input.
   * @return a list of aggregate expressions
   */
  def getInputAggExpressionColumns(actionId: ActionId)(implicit functions: DataFrameFunctions): Seq[GenericColumn] = Seq()
}

case class ExpectationValidationException(msg: String) extends Exception(msg)

/**
 * Definition of aggregation scope used to evaluate an expectations aggregate expression.
 */
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

/**
 * Default implementation for getValidationErrorColumn for metric of type `any`.
 */
trait ExpectationOneMetricDefaultImpl { this: Expectation =>
  def expectation: Option[String]
  def getValidationColumn(dataObjectId: DataObjectId, value: Any): Option[Column] = {
    import org.apache.spark.sql.functions._
    expectation.map { expectationStr =>
      val valueStr = value match {
        case None => "null"
        case Some(x) => x.toString
        case null => "null"
        case x => x.toString
      }
      val validationExpr = s"$valueStr $expectationStr"
      try {
        expr(validationExpr)
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse validation expression '$validationExpr'", Some(s"expectations.$name"), e)
      }
    }
  }

  def getValidationErrorColumn(dataObjectId: DataObjectId, value: Any, metricName: String = name)(implicit context: ActionPipelineContext): Option[Column] = {
    import org.apache.spark.sql.functions._
    getValidationColumn(dataObjectId, value).map { validationCol =>
      try {
        when(not(validationCol), lit(s"Expectation '$metricName' failed with value:$value expectation:${expectation.get}"))
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot create validation error column", Some(s"expectations.$name"), e)
      }
    }
  }

  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String, _], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn], Map[String, _]) = {
    if (scope == ExpectationScope.JobPartition) {
      val cols = metrics.keys.filter(_.startsWith(name + ExpectationValidation.partitionDelimiter))
        .flatMap { n =>
          val value = getMetric[Any](dataObjectId, metrics, name)
          getValidationErrorColumn(dataObjectId, value, n).map(SparkColumn)
        }.toSeq
      (cols, metrics)
    } else {
      val value = getMetric[Any](dataObjectId, metrics, name)
      val col = getValidationErrorColumn(dataObjectId, value).map(SparkColumn)
      (col.toSeq, metrics)
    }
  }
}

/**
 * Default implementation for getValidationErrorColumn for metric of type `any`.
 */
trait ExpectationFractionMetricDefaultImpl { this: BaseExpectation =>
  def expectation: Option[String]
  def precision: Short = 4
  protected def roundFunc(v: Double): Double = math.round(v).toDouble
  def getValidationColumn(dataObjectId: DataObjectId, fraction: Any): Option[SparkColumn] = {
    import org.apache.spark.sql.functions._
    expectation.map { expectationStr =>
      val validationExpr = s"$fraction $expectationStr"
      try {
        SparkColumn(expr(validationExpr))
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse validation expression '$validationExpr'", Some(s"expectations.$name"), e)
      }
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, countMetric: Long, countBase: Long, metricName: String = name)(implicit context: ActionPipelineContext): (Option[Column],Any) = {
    val pct = if (countBase == 0) "null" else round(countMetric.toDouble / countBase, precision)
    val col = getValidationColumn(dataObjectId, pct).map { validationCol =>
      try {
        import org.apache.spark.sql.functions._
        when(not(validationCol.inner), lit(s"Expectation '$metricName' failed with pct:$pct expectation:${expectation.get}"))
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot create validation error column", Some(s"expectations.$name"), e)
      }
    }
    // return
    (col,pct)
  }
  def round(value: Double, precision: Short): Double = {
    val roundExp = math.pow(10,precision)
    roundFunc(value * roundExp) / roundExp
  }
}

