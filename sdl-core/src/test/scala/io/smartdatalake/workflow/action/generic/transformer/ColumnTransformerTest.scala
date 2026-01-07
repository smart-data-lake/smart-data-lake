/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.app.DefaultSmartDataLakeBuilder
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ColumnTransformerTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._


  val sdlb = DefaultSmartDataLakeBuilder
  implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
  implicit val contextExec: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("only columns where the names match are removed") {
    // prepare

    val colTransformer = ColumnsTransformer(
      additionalColumns = Map("run_id" -> "runId"),
      additionalDerivedColumns = Map(
        "col_1_plus_col2" -> """col_1 + col_2"""
        , "sum_col_1" -> """sum(col_1) over (partition by 'whatever')"""
      ),
      renamedColumns = Map("col_1" -> "new_col_1"),
      droppedColumns = Seq("col_2")

    )
    val df = SparkDataFrame(Seq(
      (1, 11),
      (2, 22)).toDF("col_1", "col_2"))

    // execute
    val transformed = colTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
    val resultDF = transformed.asInstanceOf[SparkDataFrame]
    val expectedSeq: Seq[(Option[Int], Option[Int], Option[Int], Option[Int])] = Seq(
      (Some(1), Some(1), Some(12), Some(3)),
      (Some(2), Some(1), Some(24), Some(3))
    )
    val expectedDf = SparkDataFrame(expectedSeq.toDF("new_col_1", "run_id", "col_1_plus_col2", "sum_col_1"))
    resultDF.inner.show(false)
    assert(expectedDf.collect == resultDF.collect)
  }
}