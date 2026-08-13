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
package io.smartdatalake.workflow.sparkconnect

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.{ConfigParser, ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.{ColumnStatsType, Environment, SDLSaveMode, SaveModeGenericOptions, TableStatsType}
import io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.SparkConnectConnection
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectDataFrame, SparkConnectSubFeed}
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.dataobject.{DataObject, IcebergTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * Test for IcebergTableDataObject with the Spark Connect engine implementation (IcebergTableSparkConnectEngine).
 * Needs a Spark Connect server with Iceberg support, see [[SparkConnectTestUtil]] for how it is resolved or started.
 * Tests are cancelled (not failed) if no server is available.
 */
class IcebergTableSparkConnectEngineTest extends AnyFunSuite {

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  // IcebergTableDataObject resolves its Spark Connect session through the default engine connection
  private val connection = SparkConnectConnection(Environment.defaultEngineConnectionId, SparkConnectTestUtil.url)
  instanceRegistry.register(connection)
  implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec)

  private val catalog = SparkConnectTestUtil.icebergCatalog

  private def assumeIcebergAvailable(): Unit = {
    assume(SparkConnectTestUtil.serverAvailable, s"No Spark Connect server available at ${SparkConnectTestUtil.url}")
    assume(SparkConnectTestUtil.icebergAvailable, "No Iceberg support on Spark Connect server, see start-spark-connect.sh")
  }

  private def icebergTable(name: String, primaryKey: Option[Seq[String]] = None) =
    Table(catalog = Some(catalog), db = Some("default"), name = name, primaryKey = primaryKey)

  test("config type IcebergTableDataObject is parsed and routed to the Spark Connect engine") {
    // no server needed: parsing and engine discovery are client-side
    val config = ConfigFactory.parseString(
      """
        |id = testIcebergParse
        |type = IcebergTableDataObject
        |table = { catalog = iceberg1, db = default, name = sdlb_test_iceberg_parse }
        |""".stripMargin)
    val dataObject = ConfigParser.parseConfigObject[DataObject](config)
    assert(dataObject.isInstanceOf[IcebergTableDataObject])
    val supportedTypes = dataObject.asInstanceOf[IcebergTableDataObject].getSubFeedSupportedTypes
    assert(supportedTypes.size == 1 && supportedTypes.head =:= typeOf[SparkConnectSubFeed])
  }

  test("write and read iceberg table roundtrip with snapshot metrics") {
    assumeIcebergAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = IcebergTableDataObject("testIceberg", table = icebergTable("sdlb_test_iceberg"))
    dataObject.dropTable
    assert(!dataObject.isTableExisting)

    val metrics = dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    assert(dataObject.isTableExisting)
    assert(metrics.get("rows_inserted").contains(3L)) // iceberg snapshot summary

    val dfRead = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed])
    assert(dfRead.count == 3)
    assert(dfRead.schema.columns == Seq("id", "value"))

    // append
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((4L, "d")).toDF("id", "value")), Seq(), isRecursiveInput = false, Some(SaveModeGenericOptions(SDLSaveMode.Append)))
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 4)

    // cleanup
    dataObject.dropTable
    assert(!dataObject.isTableExisting)
  }

  test("partitioned iceberg table: overwrite partitions, list and delete partitions") {
    assumeIcebergAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = IcebergTableDataObject("testIcebergPart", table = icebergTable("sdlb_test_iceberg_part"), partitions = Seq("dt"))
    dataObject.dropTable

    val df = Seq((1L, "a", "20240101"), (2L, "b", "20240101"), (3L, "c", "20240102")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df), Seq(), isRecursiveInput = false, None) // creates the table
    assert(dataObject.listPartitions.toSet == Set(PartitionValues(Map("dt" -> "20240101")), PartitionValues(Map("dt" -> "20240102"))))

    // overwrite partition 20240101 with new data
    val df2 = Seq((4L, "d", "20240101")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df2), Seq(PartitionValues(Map("dt" -> "20240101"))), isRecursiveInput = false, None)
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 2) // partition 20240101 overwritten with 1 row, 20240102 untouched

    // delete partition
    dataObject.deletePartitions(Seq(PartitionValues(Map("dt" -> "20240102"))))
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 1)

    dataObject.dropTable
  }

  test("merge into iceberg table by primary key") {
    assumeIcebergAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = IcebergTableDataObject("testIcebergMerge", table = icebergTable("sdlb_test_iceberg_merge", Some(Seq("id"))), saveMode = SDLSaveMode.Merge)
    dataObject.dropTable

    // first write creates the table
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    // second write merges: update id=2, insert id=3
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((2L, "b2"), (3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    val result = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).asInstanceOf[SparkConnectDataFrame].inner.collect().map(r => (r.getLong(0), r.getString(1))).toSet
    assert(result == Set((1L, "a"), (2L, "b2"), (3L, "c")))

    dataObject.dropTable
  }

  test("vacuum expires snapshots after write") {
    assumeIcebergAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = IcebergTableDataObject("testIcebergVacuum", table = icebergTable("sdlb_test_iceberg_vacuum"), historyRetentionPeriod = Some(1))
    dataObject.dropTable
    // write triggers vacuum at the end
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    // explicit vacuum
    dataObject.vacuum
    dataObject.dropTable
  }

  test("getStats and getColumnStats from iceberg metadata tables") {
    assumeIcebergAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = IcebergTableDataObject("testIcebergStats", table = icebergTable("sdlb_test_iceberg_stats"))
    dataObject.dropTable
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)

    val stats = dataObject.getStats()
    assert(stats.get(TableStatsType.NumRows.toString).contains(2L))
    assert(stats.contains(TableStatsType.LastModifiedAt.toString))
    assert(stats.contains(TableStatsType.OldestSnapshotTs.toString))
    val columnStats = dataObject.getColumnStats(update = false, None)
    assert(columnStats.keys.toSet == Set("id", "value"))
    // statistics are aggregated over all data files of the table
    assert(columnStats("id").get(ColumnStatsType.Min.toString).contains(1L))
    assert(columnStats("id").get(ColumnStatsType.Max.toString).contains(2L))
    assert(columnStats("id").get(ColumnStatsType.NullCount.toString).contains(0L))

    dataObject.dropTable
  }

  test("incremental CDC read with setState/getState") {
    assumeIcebergAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = IcebergTableDataObject("testIcebergCdc", table = icebergTable("sdlb_test_iceberg_cdc", Some(Seq("id"))))
    dataObject.dropTable
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)

    // first incremental read: no snapshot state yet -> full read
    dataObject.setState(dataObject.getState)
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 2)
    val stateAfterFullRead = dataObject.getState // snapshot id to read changes from

    // append new row, recorded as a new snapshot
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, Some(SaveModeGenericOptions(SDLSaveMode.Append)))

    // incremental read from the snapshot after the full read -> only the appended row
    dataObject.setState(stateAfterFullRead)
    val dfInc = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed])
    assert(dfInc.count == 1)
    assert(dfInc.asInstanceOf[SparkConnectDataFrame].inner.collect().map(_.getLong(0)).toSeq == Seq(3L))

    dataObject.dropTable
  }
}
