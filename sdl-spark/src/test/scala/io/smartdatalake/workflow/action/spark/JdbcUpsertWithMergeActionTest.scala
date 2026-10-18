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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.testutils.UpsertActionBehaviour
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.JdbcTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests UpsertAction with mergeModeFilter on a JDBC table, using the shared UpsertActionBehaviour.
 */
class JdbcUpsertWithMergeActionTest extends AnyFunSuite with SmartDataLakeLogger with UpsertActionBehaviour {

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcUpsertWithMergeActionTest", "org.hsqldb.jdbcDriver")

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  test("upsert load mergeModeFilter") {
    testUpsertWithMergeModeFilter(
      (id, registry) => MockSparkDataObject(id)(registry),
      (id, pks, registry) => {
        val dataObject = JdbcTableDataObject(id, table = Table(Some("public"), id.replaceAll("-", "_") + "_range", None, pks), connectionId = jdbcConnection.id)(registry)
        dataObject.dropTable(SparkTestUtil.getDefaultActionPipelineContext(registry))
        dataObject
      },
      tgtConnection = Some(jdbcConnection)
    )
  }

  test("upsert into partitioned table with mergeModeFilter and PartitionDiffMode") {
    testUpsertWithMergeModeFilterPartitioned(
      (id, partitions, registry) => MockSparkDataObject(id, partitions = partitions)(registry),
      (id, pks, partitions, registry) => {
        val dataObject = JdbcTableDataObject(id, table = Table(Some("public"), id.replaceAll("-", "_") + "_part", None, pks), connectionId = jdbcConnection.id,
          virtualPartitions = partitions)(registry)
        dataObject.dropTable(SparkTestUtil.getDefaultActionPipelineContext(registry))
        dataObject
      },
      tgtConnection = Some(jdbcConnection)
    )
  }
}
