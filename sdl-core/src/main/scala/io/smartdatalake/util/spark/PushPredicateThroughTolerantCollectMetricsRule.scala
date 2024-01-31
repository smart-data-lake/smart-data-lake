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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, Filter, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Pushes Filter operators through CollectMetrics operator if it's name is marked with suffix "!tolerant".
 * This is needed to count records read from input DataObjects correctly.
 */
private[smartdatalake] case class PushPredicateThroughTolerantCollectMetricsRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    case filter @ Filter(_, u: UnaryNode) =>
      u match {
        case m: CollectMetrics if m.name.endsWith("!tolerant") =>
          u.withNewChildren(Seq(Filter(filter.condition, u.child)))
        case _ => filter
      }
  }
}