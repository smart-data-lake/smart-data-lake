/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.ExpectationScope
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity
import io.smartdatalake.workflow.dataobject.expectation.{ActionExpectation, ExpectationFractionMetricDefaultImpl, ExpectationScope, ExpectationSeverity}


/**
 * Definition of expectation on comparing count all records of input and output table.
 * Completeness is calculated as the fraction of main output count-all over main input count-all.
 *
 * Use it to detect unintended record loss in an Action, e.g. rows dropped by a join or a filter.
 * As it needs a main input and a main output to compare, it can only be configured as `expectations`
 * of an Action (see [[ActionExpectation]]), not on a DataObject. The scope is fixed to the whole
 * table, and the fraction is rounded down, so an incomplete result is detected aggressively.
 *
 * Example:
 * {{{
 * actions = {
 *   join-departures-airports {
 *     type = CustomDataFrameAction
 *     inputIds = [stg-departures, int-airports]
 *     outputIds = [btl-departures-arrivals-airports]
 *     mainInputId = stg-departures
 *     expectations = [{
 *       type = CompletenessExpectation
 *       expectation = "> 0.95"
 *     }]
 *   }
 * }
 * }}}
 *
 * @param expectation Optional SQL comparison operator and literal to define expected value for validation. Default is '= 1".
 *                    Together with the result of the aggExpression evaluation on the left side, it forms the condition to validate the expectation.
 *                    If no expectation is defined, the aggExpression evaluation result is just recorded in metrics.
 * @param precision Number of digits to keep when calculating fraction. Default is 4.
 */
case class CompletenessExpectation(
                                    override val name: String = "pctComplete",
                                    override val expectation: Option[String] = Some("= 1"),
                                    override val precision: Short = 4,
                                    override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends ActionExpectation with ExpectationFractionMetricDefaultImpl {
  override val description: Option[String] = Some("fraction of main output count-all over main input count-all")
  override def scope: ExpectationScope = ExpectationScope.All // fixed to whole table
  override def roundFunc(v: Double): Double = math.floor(v) // use floor to be more aggressive on detecting unique key violations.

  override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = {
    import functions._
    Seq(count(col("*")).as("countAll"))
  }
  override def getInputAggExpressionColumns(actionId: ActionId)(implicit functions: DataFrameFunctions): Seq[GenericColumn] = {
    import functions._
    Seq(count(col("*")).as("countAll"))
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[GenericColumn],Map[String,_]) = {
    val countOutput = getMetric[Long](dataObjectId,metrics,"countAll")
    val countInput = getMetric[Long](dataObjectId,metrics,"countAll#mainInput")
    val (col, pct) = getValidationErrorColumnSql(dataObjectId, countOutput, countInput)
    val updatedMetrics = metrics + (name -> pct)
    (col.toSeq, updatedMetrics)
  }

  override def factory: FromConfigFactory[ActionExpectation] = CompletenessExpectation
}

object CompletenessExpectation extends FromConfigFactory[ActionExpectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CompletenessExpectation = {
    extract[CompletenessExpectation](config)
  }
}