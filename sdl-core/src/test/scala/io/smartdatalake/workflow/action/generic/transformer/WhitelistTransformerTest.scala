/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

class WhitelistTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("only columns where the names match are whitelisted") {
    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("column1", "column3"))
    val df = SparkDataFrame(Seq((1, 1), (2, 2)).toDF("column1", "column2"))

    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column1"))
  }

  test("column whitelisting is case insensitive") {
    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("coLumn1"))
    val df = SparkDataFrame(Seq(1, 2).toDF("column1"))

    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column1"))
  }

}
