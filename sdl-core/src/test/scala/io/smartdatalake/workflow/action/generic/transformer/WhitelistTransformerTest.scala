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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.FunSuite

class WhitelistTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("only columns where the names match are whitelisted") {
    // prepare
    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("column1", "column3"))
    val df = SparkDataFrame(Seq((1, 1), (2, 2)).toDF("column1", "column2"))

    // execute
    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformed.schema.columns == Seq("column1"))
  }

  test("column whitelisting is case insensitive per default") {
    // prepare
    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("coLumn1"))
    val df = SparkDataFrame(Seq(1, 2).toDF("column1"))

    // execute
    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformed.schema.columns == Seq("column1"))
  }

  test("column whitelisting is case sensitive if Environment.caseSensitive=true") {
    // prepare
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)
    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("ColumN1", "blop"))
    val df = SparkDataFrame(Seq((1, 1), (2, 2)).toDF("column1", "ColumN1"))

    // execute
    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformed.schema.columns == Seq("ColumN1"))

    // cleanup
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("column whitelisting throws no error if whitelisted column has dots") {
    // prepare
    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("column.1"))
    val df = SparkDataFrame(Seq((1), (2)).toDF("column.1"))

    // execute
    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformed.schema.columns == Seq("column.1"))
  }
}
