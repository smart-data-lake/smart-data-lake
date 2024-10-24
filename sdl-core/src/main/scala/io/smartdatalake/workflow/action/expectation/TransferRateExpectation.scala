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

package io.smartdatalake.workflow.action.expectation

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkColumn
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity
import io.smartdatalake.workflow.dataobject.expectation.{ActionExpectation, ExpectationFractionMetricDefaultImpl, ExpectationSeverity}
import org.apache.spark.sql.Column


/**
 * Definition of expectation on transfer rate.
 * Transfer rate is calculated as the fraction of main output count over main input count.
 *
 * @param expectation Optional SQL comparison operator and literal to define expected value for validation. Default is '= 1".
 *                    Together with the result of the aggExpression evaluation on the left side, it forms the condition to validate the expectation.
 *                    If no expectation is defined, the aggExpression evaluation result is just recorded in metrics.
 * @param precision Number of digits to keep when calculating fraction. Default is 4.
 */
case class TransferRateExpectation(
                                    override val name: String = "pctTransfer",
                                    override val expectation: Option[String] = Some("= 1"),
                                    override val precision: Short = 4,
                                    override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends ActionExpectation with ExpectationFractionMetricDefaultImpl {
  override val description: Option[String] = Some("fraction of main output count over main input count")
  override def roundFunc(v: Double): Double = math.floor(v) // use floor to be more aggressive on detecting unique key violations.

  override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = {
    Seq() // no additional aggregate expressions needed
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn],Map[String,_]) = {
    val countOutput = getMetric[Long](dataObjectId,metrics,"count")
    val countInput = getMetric[Long](dataObjectId,metrics,"count#mainInput")
    val (col, pct) = getValidationErrorColumn(dataObjectId, countOutput, countInput)
    val updatedMetrics = metrics + (name -> pct)
    (col.map(SparkColumn).toSeq, updatedMetrics)
  }
  override def factory: FromConfigFactory[ActionExpectation] = TransferRateExpectation
}

object TransferRateExpectation extends FromConfigFactory[ActionExpectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TransferRateExpectation = {
    extract[TransferRateExpectation](config)
  }
}