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

package io.smartdatalake.util.spark

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.adaptive.AQEPropagateEmptyRelation
import org.apache.spark.sql.{DataFrame, DatasetHelper, SparkSessionExtensions}

/**
 * Custom extensions for the Spark Session, e.g. optimizer rules or additional plan strategies.
 */
private[smartdatalake] class SDLSparkExtension extends (SparkSessionExtensions => Unit) with SmartDataLakeLogger {
  def apply(e: SparkSessionExtensions): Unit = {

    // inserts a Strategy for using the custom operation AssertNotEmpty in Spark plans.
    e.injectPlannerStrategy(_ => AssertNotEmptyStrategy)

    // Check logical plan if it has data, otherwise throw SparkPlanNoDataWarning
    e.injectPreCBORule(SparkPlanNoDataCheckRule) // this is for checking final optimized and simplified plan, e.g. PropagateEmptyRelation rule has been executed.
    e.injectRuntimeOptimizerRule(SparkPlanNoDataCheckRule) // this is for checking runtime statistics with Spark AQE

    // Allow filter push-down over CollectMetrics for observations with special marker.
    if (Environment.enableInputDataObjectCount) e.injectOptimizerRule(PushPredicateThroughTolerantCollectMetricsRule)

    logger.info("initialized")
  }
}

object SDLSparkExtension {
  /**
   * Fail on evaluation of DataFrame if it is empty.
   */
  def assertNotEmpty(df: DataFrame): DataFrame = {
    if (Environment.enableSparkPlanNoDataCheck) DatasetHelper.modifyPlan(df, plan => AssertNotEmpty(plan))
    else df
  }

  // We use logic implemented in an existing rule to check if a LogicalPlan is empty.
  // Unfortunately this method is marked as "protected" and we need to call it dynamically.
  private val aqeIsNodeEmptyMethod = AQEPropagateEmptyRelation.getClass.getMethod("isEmpty", classOf[LogicalPlan])
  private[smartdatalake] def isPlanEmpty(logicalPlan: LogicalPlan) = {
    val leaves = logicalPlan.collectLeaves()
    leaves.forall(l => aqeIsNodeEmptyMethod.invoke(AQEPropagateEmptyRelation, l).asInstanceOf[Boolean])
  }

}

class SparkPlanNoDataWarning(msg:String) extends Exception(msg)