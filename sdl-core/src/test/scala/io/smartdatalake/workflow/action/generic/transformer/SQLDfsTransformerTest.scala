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
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.CustomDataFrameAction
import io.smartdatalake.workflow.connection.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.{JdbcTableDataObject, Table}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class SQLDfsTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  val con1 = JdbcTableConnection("con1", url = "123", driver = "driver") // dummy
  instanceRegistry.register(con1)

  val srcTable1 = Table(Some("default"), "src1")
  val srcDO1 = JdbcTableDataObject("src1", table = srcTable1, connectionId = "con1")
  instanceRegistry.register(srcDO1)

  val tgtTable1 = Table(Some("default"), "tgt1")
  val tgtDO1 = JdbcTableDataObject("tgt1", table = tgtTable1, connectionId = "con1")
  instanceRegistry.register(tgtDO1)

  val action1 = CustomDataFrameAction("action1", List(srcDO1.id), List(tgtDO1.id))
  instanceRegistry.register(action1)

  val emptyDf = Seq((1,"a")).toDF("num","str")

  test("options and view name token are replaced and sql can be parsed") {
    val customTransformer = SQLDfsTransformer(code = Map(tgtDO1.id.id -> s"select %{inputViewName_src1}.num, %{option1} from %{inputViewName_src1}"))
    customTransformer.transformWithOptions(action1.id, Seq(), Map(srcDO1.id.id -> SparkDataFrame(emptyDf)), Map("option1" -> "str"))
  }

  test("legacy view name without postfix is still supported and sql can be parsed") {
    val customTransformer = SQLDfsTransformer(code = Map(tgtDO1.id.id -> s"select src1.num, %{option1} from src1"))
    customTransformer.transformWithOptions(action1.id, Seq(), Map(srcDO1.id.id -> SparkDataFrame(emptyDf)), Map("option1" -> "str"))
  }

}
