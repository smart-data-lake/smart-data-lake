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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}


/**
 * A strategy to translate below AssertNotEmpty operator in Spark logical plan to its representation in the physical plan.
 */
object AssertNotEmptyStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[AssertNotEmptyExec] = plan match {
    case AssertNotEmpty(child) =>
      AssertNotEmptyExec(planLater(child)) :: Nil
    case _ => Nil
  }
}

/**
 * Operator to assert that a Dataset is not empty at the exact point in the plan where this operator is inserted.
 * Use SDLSparkExtension.assertNotEmpty to use it.
 *
 * Performance note: this is executing an additional rdd.isEmpty job while executing the query!
 */
private[smartdatalake] case class AssertNotEmpty(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)
}

/**
 * Implementation of AssertNotEmpty operator
 */
case class AssertNotEmptyExec(child: SparkPlan) extends UnaryExecNode {

  // Output has the same attributes as input
  override def output: Seq[Attribute] = child.output

  // Examine child's data
  override protected def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()
    if (rdd.isEmpty()) throw AssertNotEmptyFailure()
    rdd
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
}

private[smartdatalake] case class AssertNotEmptyFailure()
  extends SparkPlanNoDataWarning("No rows to process found by AssertNotEmpty operation in Spark plan. Set Environment.enableSparkPlanNoDataCheck=false to disable this check.")

