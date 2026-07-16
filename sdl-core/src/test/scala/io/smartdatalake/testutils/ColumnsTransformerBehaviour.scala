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
package io.smartdatalake.testutils

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.generic.transformer.ColumnsTransformer

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[ColumnsTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * Note: `additionalColumns` (evaluated via `ExpressionUtil`/`ExpressionEvaluatorFactory`) and window-function
 * derived-column expressions (e.g. `sum(x) over (...)`) both require a Spark expression library on the classpath,
 * see [[testAdditionalColumnsUsingContextExpression]] and [[testAdditionalDerivedColumnUsingWindowFunction]].
 */
trait ColumnsTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testAdditionalDerivedRenamedAndDroppedColumns(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val colTransformer = ColumnsTransformer(
      additionalDerivedColumns = Map("col_1_plus_col2" -> """col_1 + col_2"""),
      renamedColumns = Map("col_1" -> "new_col_1"),
      droppedColumns = Seq("col_2")
    )
    val df = Seq((1, 11), (2, 22)).toDF("col_1", "col_2")

    val transformed = colTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    val expectedSeq: Seq[(Option[Int], Option[Int])] = Seq(
      (Some(1), Some(12)),
      (Some(2), Some(24))
    )
    val expectedDf = expectedSeq.toDF("new_col_1", "col_1_plus_col2")
    assert(expectedDf.isEqual(transformed))
  }

  /**
   * Not portable to sdl-core: `additionalColumns` is evaluated via `ExpressionUtil.evaluate`, which requires a
   * `ExpressionEvaluatorFactory` (`ch.zzeekk.spark.expressions.SparkExpressionEvaluatorFactory`) provided by the
   * spark-extensions/spark-expressions-standalone library, which is not on the sdl-core classpath.
   */
  def testAdditionalColumnsUsingContextExpression(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val colTransformer = ColumnsTransformer(additionalColumns = Map("run_id" -> "runId"))
    val df = Seq((1, 11), (2, 22)).toDF("col_1", "col_2")

    val transformed = colTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    val expectedSeq: Seq[(Int, Int, Int)] = Seq((1, 11, 1), (2, 22, 1))
    val expectedDf = expectedSeq.toDF("col_1", "col_2", "run_id")
    assert(expectedDf.isEqual(transformed))
  }

  /**
   * Not portable to ScalaSubFeed: `expr` with a window function (`over (partition by ...)`) requires a real SQL
   * engine; ScalaSubFeed's expression parser has no OVER/PARTITION BY grammar and its `window` function is not implemented.
   */
  def testAdditionalDerivedColumnUsingWindowFunction(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val colTransformer = ColumnsTransformer(
      additionalDerivedColumns = Map("sum_col_1" -> """sum(col_1) over (partition by 'whatever')""")
    )
    val df = Seq((1, 11), (2, 22)).toDF("col_1", "col_2")

    val transformed = colTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    val expectedSeq: Seq[(Int, Int, Long)] = Seq((1, 11, 3L), (2, 22, 3L))
    val expectedDf = expectedSeq.toDF("col_1", "col_2", "sum_col_1")
    assert(expectedDf.isEqual(transformed))
  }
}
