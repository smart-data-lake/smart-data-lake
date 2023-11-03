/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ConvertNullValuesTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("exclusive include- or excludeColumns") {

    // prepare
    val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("column1"), excludeColumns = Seq("column2"))
    val df = SparkDataFrame(Seq((1, 1), (2, 2)).toDF("column1", "column2"))

    // execute
    val thrown = intercept[IllegalArgumentException] {
      convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
    }

    // check
    assert(thrown.isInstanceOf[IllegalArgumentException])
    assert(thrown.getMessage == "requirement failed: Conflicting parameters. Please use either includeColumns or excludeColumns, as simultaneous application is not supported.")
  }

  test("default values") {

    // prepare
    val convertNullValuesTransformer = ConvertNullValuesTransformer()
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-1), Some(3.0)), (Some("na"), Some(2), Some(-1.0)), (Some("na"), Some(-1), Some(-1.0)))
    val df = SparkDataFrame(initSeq.toDF("column1", "column2", "column3"))
    val resultDf = SparkDataFrame(resultSeq.toDF("column1", "column2", "column3"))

    // execute
    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformedDf.collect == resultDf.collect)

  }

}
