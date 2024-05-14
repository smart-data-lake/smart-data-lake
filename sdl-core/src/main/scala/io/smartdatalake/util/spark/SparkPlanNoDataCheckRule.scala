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
import io.smartdatalake.util.spark.SDLSparkExtension.isPlanEmpty
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.V1WriteCommand

/**
 * Check if LogicalPlan has no data for write commands and throw a warning.
 */
private[smartdatalake] case class SparkPlanNoDataCheckRule(spark: SparkSession) extends Rule[LogicalPlan] with SmartDataLakeLogger {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (Environment.enableSparkPlanNoDataCheck) {
      // evaluate "no data" for write commands
      plan match {
        case x: V1WriteCommand => evaluateNoData(plan)
        case x: V2WriteCommand => evaluateNoData(plan)
        case _ => Unit
      }
    }
    plan
  }

  /**
   * Check if logical plan is empty. If yes, an PlanCheckNoDataWarning is thrown.
   */
  private def evaluateNoData(plan: LogicalPlan): Unit = {
    logger.debug(s"evaluating plan ${plan}")
    if (isPlanEmpty(plan)) throw new SparkPlanNoDataWarning("No rows to process found in Spark logical plan. Set Environment.enableSparkPlanNoDataCheck=false to disable this check.")
  }
}
