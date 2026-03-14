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
package io.smartdatalake.workflow.action

import io.smartdatalake.testutils.spark.dataset.TestToolDataset
import io.smartdatalake.testutils.{HistorizeActionBehaviour, MockSparkDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.{JdbcTableDataObject, Table}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JdbcHistorizeWithMergeActionTest extends AnyFunSuite with Matchers with SmartDataLakeLogger
  with TestToolDataset with Equality with HistorizeActionBehaviour {

  implicit val session: SparkSession = TestUtil.session

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:HistorizeWithMergeActionTest", "org.hsqldb.jdbcDriver")

  testsFor(historizeWithMergeMode(
    (id, registry) => MockSparkDataObject(id),
    (id, pks, registry) => {
      val tgtTable = Table(Some("public"), id.replaceAll("-", "_"), None, pks)
      val dataObject = JdbcTableDataObject(id, table = tgtTable, connectionId = jdbcConnection.id, allowSchemaEvolution = true)(registry)
      dataObject.dropTable(TestUtil.getDefaultActionPipelineContext(registry))
      dataObject
    },
    tgtConnection = Some(jdbcConnection)
  ))

  testsFor(historizeIncrementalPipeline(
    (id, registry) => MockSparkDataObject(id),
    (id, pks, registry) => {
      val tgtTable = Table(Some("public"), id.replaceAll("-", "_"), None, pks)
      val dataObject = JdbcTableDataObject(id, table = tgtTable, connectionId = jdbcConnection.id, allowSchemaEvolution = true)(registry)
      dataObject.dropTable(TestUtil.getDefaultActionPipelineContext(registry))
      dataObject
    },
    tgtConnection = Some(jdbcConnection)
  ))
}
