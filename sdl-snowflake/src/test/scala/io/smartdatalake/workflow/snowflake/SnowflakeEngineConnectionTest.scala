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

package io.smartdatalake.workflow.snowflake

import io.smartdatalake.config.SdlConfigObject._
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.workflow.connection.authMode.BasicAuthMode
import io.smartdatalake.workflow.dataframe.snowflake.SnowparkSubFeed
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * Tests the selection of the Snowpark engine by the engine connection. No Snowflake account is needed, as neither
 * SnowflakeConnection nor SnowflakeTableDataObject connect to Snowflake on creation.
 */
class SnowflakeEngineConnectionTest extends AnyFunSuite {

  private def createSnowflakeConnection(id: String) = SnowflakeConnection(id = id, url = "https://dummy.snowflakecomputing.com",
    warehouse = "WH", database = "DB", role = "ROLE",
    authMode = BasicAuthMode(user = StringOrSecret("user"), password = StringOrSecret("password")))

  private def setup(): InstanceRegistry = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    instanceRegistry.register(createSnowflakeConnection("sfCon"))
    instanceRegistry.register(createSnowflakeConnection("sfCon2"))
    instanceRegistry.register(SnowflakeTableDataObject("src", Table(Some("SCHEMA"), "src"), connectionId = "sfCon"))
    instanceRegistry.register(SnowflakeTableDataObject("tgt", Table(Some("SCHEMA"), "tgt"), connectionId = "sfCon"))
    instanceRegistry.register(SnowflakeTableDataObject("other", Table(Some("SCHEMA"), "other"), connectionId = "sfCon2"))
    instanceRegistry
  }

  test("SnowflakeConnection as engine connection selects the Snowpark engine") {
    implicit val instanceRegistry: InstanceRegistry = setup()
    val action = CopyAction("snowpark", "src", "tgt", engineConnectionId = Some(ConnectionId("sfCon")))
    assert(action.subFeedType =:= typeOf[SnowparkSubFeed])
  }

  test("default Spark engine connection selects the Spark engine for SnowflakeTableDataObjects") {
    implicit val instanceRegistry: InstanceRegistry = setup()
    val action = CopyAction("spark", "src", "tgt")
    assert(action.subFeedType =:= typeOf[SparkSubFeed])
  }

  test("Snowpark engine fails for a SnowflakeTableDataObject of another connection") {
    implicit val instanceRegistry: InstanceRegistry = setup()
    val action = CopyAction("snowpark", "src", "other", engineConnectionId = Some(ConnectionId("sfCon")))
    val context = SparkTestUtil.getDefaultActionPipelineContext.withAction(action)
    val dataObject = instanceRegistry.get[SnowflakeTableDataObject](DataObjectId("other"))
    val ex = intercept[ConfigurationException](dataObject.getSnowparkDataFrame()(context))
    assert(ex.getMessage.contains("sfCon2") && ex.getMessage.contains("must use its engine connection"))
  }

  test("closing a SnowflakeConnection without Snowpark session does nothing") {
    createSnowflakeConnection("sfCon").close()
  }
}
