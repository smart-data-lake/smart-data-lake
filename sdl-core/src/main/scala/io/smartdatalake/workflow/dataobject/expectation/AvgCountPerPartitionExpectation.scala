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
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.ExpectationScope
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity


/**
 * Definition of an expectation based on the average number of records per partitions.
 *
 * Note that the scope for evaluating this expectation is fixed to Job.
 *
 * @param expectation Optional SQL comparison operator and literal to define expected value for validation, e.g. '> 100000".
 *                    If no expectation is defined, the result value is just recorded in metrics.
 */
case class AvgCountPerPartitionExpectation(override val name: String, override val description: Option[String] = None, expectation: Option[String] = None, override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends Expectation {
  override val scope: ExpectationScope = ExpectationScope.Job
 override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = Seq() // no special aggregate needed as count is calculated by default
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
  private def getValidationErrorColumn(dataObjectId: DataObjectId, countMetric: Long, nbOfPartitionValues: Int)(implicit context: ActionPipelineContext): (Option[SparkColumn], Long) = {
    import org.apache.spark.sql.functions._
    val avgCount = countMetric / math.max(nbOfPartitionValues, 1)
    val col = getValidationColumn(dataObjectId, avgCount).map { validationCol =>
      try {
        SparkColumn(when(not(validationCol.inner), lit(s"Expectation '$name' failed with avgCount:$avgCount expectation:${expectation.get}")))
      } catch {
        case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot create validation error column", Some(s"expectations.$name"), e)
      }
    }
    (col, avgCount)
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn],Map[String,_]) = {
    // needs partition values
    if (partitionValues.nonEmpty) {
      val count: Long = metrics.getOrElse("count", throw new IllegalStateException(s"($dataObjectId) General 'count' metric for expectation ${name} not found for validation"))
        .asInstanceOf[Long]
      val (col,countAvg) = getValidationErrorColumn(dataObjectId, count, partitionValues.size)
      (col.toSeq, metrics + ("countAvgPerPartition" -> countAvg))
    } else {
      logger.warn(s"($dataObjectId) Cannot evaluate AvgCountPerPartitionExpectation '$name' as there are no partition values")
      (Seq(), metrics)
    }
  }
  override def factory: FromConfigFactory[Expectation] = AvgCountPerPartitionExpectation
}

object AvgCountPerPartitionExpectation extends FromConfigFactory[Expectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AvgCountPerPartitionExpectation = {
    extract[AvgCountPerPartitionExpectation](config)
  }
}

