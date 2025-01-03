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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkColumn
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn}
import io.smartdatalake.workflow.dataobject.ExpectationValidation
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.{ExpectationScope, Job}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity
import org.apache.spark.sql.Column


/**
 * Definition of an expectation based on counting how many rows match an expression vs the number of all rows.
 * The fraction of these two counts is compared against a given expectation.
 *
 * @param countConditionExpression SQL expression returning a boolean to match the rows to count for the fraction.
 * @param globalConditionExpression SQL expression returning a boolean used as global filter, e.g. fraction row count and total row count are filtered with global filter before counting.
 * @param expectation Optional SQL comparison operator and literal to define expected percentage for validation, e.g. '= 0.9".
 *                    If no expectation is defined, the result value is just recorded in metrics.
 * @param precision Number of digits to keep when calculating fraction. Default is 4.
 * @param scope The aggregation scope used to evaluate the aggregate expression.
 *              Default is 'Job', which evaluates the records transformed by the current job. This is implemented without big performance impacts on Spark.
 *              Other options are 'All' and 'JobPartition', which are implemented by reading the output data again.
 */
case class SQLFractionExpectation(
                                   override val name: String,
                                   override val description: Option[String] = None,
                                   countConditionExpression: String,
                                   globalConditionExpression: Option[String] = None,
                                   override val expectation: Option[String] = None,
                                   override val precision: Short = 4,
                                   override val scope: ExpectationScope = Job,
                                   override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends Expectation with ExpectationFractionMetricDefaultImpl {
  private def getGlobalConditionExpression(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions) = {
    try {
      import functions._
      globalConditionExpression.map(expr).getOrElse(lit(true))
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse SQL expression '${globalConditionExpression.get}'", Some(s"expectations.$name.expression"), e)
    }
  }
  private val totalName = name+"Total"
  override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = {
    try {
      import functions._
      Seq(count(when(expr(countConditionExpression) and getGlobalConditionExpression(dataObjectId),lit(1))).as(name)) ++
        (if (globalConditionExpression.isDefined) Seq(count(when(getGlobalConditionExpression(dataObjectId),lit(1))).as(totalName)) else Seq())
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse SQL expression '$countConditionExpression'", Some(s"expectations.$name.expression"), e)
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn],Map[String,_]) = {
    import ExpectationValidation.partitionDelimiter
    val totalMetric = if (globalConditionExpression.isDefined) totalName else "count"
    if (scope == ExpectationScope.JobPartition) {
      val colsAndUpdatedMetrics = metrics.keys.filter(_.startsWith(name + partitionDelimiter)).toSeq
        .map { n =>
          val countExpectation = getMetric[Long](dataObjectId,metrics,n)
          val totalPartitionMetric = (totalMetric +: n.split(partitionDelimiter).drop(1)).mkString(partitionDelimiter)
          val countTotal = getMetric[Long](dataObjectId,metrics,totalPartitionMetric)
          val (col, pct) = getValidationErrorColumn(dataObjectId, countExpectation, countTotal, n)
          (col.map(SparkColumn), Map(n -> pct))
        }
      val cols = colsAndUpdatedMetrics.flatMap(_._1)
      val updatedMetrics = metrics.filterKeys(!_.startsWith(totalName)).toMap ++ colsAndUpdatedMetrics.map(_._2).reduce(_ ++ _)
      (cols, updatedMetrics)
    } else {
      val countExpectation = getMetric[Long](dataObjectId,metrics,name)
      val countTotal = getMetric[Long](dataObjectId,metrics,totalMetric)
      val (col, pct) = getValidationErrorColumn(dataObjectId, countExpectation, countTotal)
      val updatedMetrics = metrics.filterKeys(_ != totalName).toMap + (name -> pct)
      (col.map(SparkColumn).toSeq, updatedMetrics)
    }
  }

  override def factory: FromConfigFactory[Expectation] = SQLFractionExpectation
}

object SQLFractionExpectation extends FromConfigFactory[Expectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLFractionExpectation = {
    extract[SQLFractionExpectation](config)
  }
}