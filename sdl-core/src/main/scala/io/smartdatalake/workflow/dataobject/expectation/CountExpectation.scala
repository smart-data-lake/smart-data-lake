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
 * Definition of an expectation based on the number of records.
 *
 * @param expectation Optional SQL comparison operator and literal to define expected value for validation, e.g. '> 100000".
 *                    If no expectation is defined, the result value is is just recorded in metrics.
 * @param scope The aggregation scope used to evaluate the aggregate expression.
 *              Default is 'Job', which evaluates the records transformed by the current job. This is implemented without big performance impacts on Spark.
 *              Other options are 'All' and 'JobPartition', which are implemented by reading the output data again.
 */
case class CountExpectation(override val name: String = "count", override val description: Option[String] = None, expectation: Option[String] = None, override val scope: ExpectationScope = Job, override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends Expectation {
  override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = {
    import functions._
    if (scope == ExpectationScope.Job && name == "count") Seq() // count is measured by default for scope = Job
    else Seq(count(col("*")).as(name))
  }
  def getValidationColumn(dataObjectId: DataObjectId, count: Long): Option[SparkColumn] = {
    expectation.map { expectationStr =>
      val validationExpr = s"$count $expectationStr"
      try {
        import org.apache.spark.sql.functions._
        SparkColumn(expr(validationExpr))
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse validation expression '$validationExpr'", Some(s"expectations.$name"), e)
      }
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, countMetric: Long, metricName: String = name)(implicit context: ActionPipelineContext): Option[Column] = {
    getValidationColumn(dataObjectId, countMetric).map { validationCol =>
      try {
        import org.apache.spark.sql.functions._
        when(not(validationCol.inner), lit(s"Expectation '$metricName' failed with count:$countMetric expectation:${expectation.get}"))
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot create validation error column", Some(s"expectations.$name"), e)
      }
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn],Map[String,_]) = {
    if (scope == ExpectationScope.JobPartition) {
      val cols = metrics.keys.filter(_.startsWith(name + ExpectationValidation.partitionDelimiter))
        .flatMap { n =>
          val count = getMetric[Long](dataObjectId,metrics,n)
          getValidationErrorColumn(dataObjectId, count, n).map(SparkColumn)
        }.toSeq
      (cols, metrics)
    } else {
      val count = getMetric[Long](dataObjectId,metrics,name)
      val col = getValidationErrorColumn(dataObjectId, count).map(SparkColumn)
      (col.toSeq, metrics)
    }
  }
  override def factory: FromConfigFactory[Expectation] = CountExpectation
}

object CountExpectation extends FromConfigFactory[Expectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CountExpectation = {
    extract[CountExpectation](config)
  }
}
