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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class StandardizeColNamesTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("dots in column names are removed") {
    val colNamesTransformer = StandardizeColNamesTransformer()
    val df = SparkDataFrame(Seq((1, 1), (2, 2)).toDF("one.dot", "two.do.ts"))

    val transformed = colNamesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("onedot", "twodots"))
  }

  test("blanks in column names are replaces") {
    val colNamesTransformer = StandardizeColNamesTransformer(replaceNonStandardSQLNameCharsWithUnderscores = true)
    val df = SparkDataFrame(Seq((1, 1), (2, 2)).toDF("one dot", "two-do-ts", "value of property!in$$"))

    val transformed = colNamesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("one_dot", "two_do_ts", "value_of_property_in_"))
  }

}
