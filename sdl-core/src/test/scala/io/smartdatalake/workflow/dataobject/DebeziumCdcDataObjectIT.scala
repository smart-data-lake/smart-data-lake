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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.connection.{DebeziumConnection, DebeziumDatabaseEngine}

object DebeziumCdcDataObjectIT extends App with SmartDataLakeLogger {

  implicit val sparkSession = TestUtil.session
  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context = ConfigToolbox.getDefaultActionPipelineContext

  val connection = DebeziumConnection(
    id = "dbzCon",
    dbEngine = DebeziumDatabaseEngine.MySql,
    hostname = sys.env("MYSQL_HOSTNAME"),
    port = sys.env("MYSQL_PORT").toInt,
    authMode = BasicAuthMode(Some(StringOrSecret(sys.env("MYSQL_USER"))), Some(StringOrSecret(sys.env("MYSQL_PASSWORD"))))
  )

  instanceRegistry.register(connection)

  val testDO = DebeziumCdcDataObject("test1", connectionId = "dbzCon", Table(Some("demo"), "test"))
  instanceRegistry.register(testDO)

  val df = testDO.getSparkDataFrame()

  assert(df.columns.contains("id") && df.columns.contains("value"))


}
