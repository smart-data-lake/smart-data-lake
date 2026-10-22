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
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeGenericOptions, TableStatsType}
import io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.SparkConnectConnection
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectDataFrame, SparkConnectSubFeed}
import io.smartdatalake.workflow.dataobject.{DataObject, DeltaLakeTableDataObject}
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * Test for DeltaLakeTableDataObject with the Spark Connect engine implementation (DeltaLakeTableConnectEngine).
 * Needs a Spark Connect server with delta support, see [[SparkConnectTestUtil]] for how it is resolved or started.
 * Tests are cancelled (not failed) if no server is available.
 */
class DeltaLakeTableSparkConnectEngineTest extends AnyFunSuite {

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  // DeltaLakeTableDataObject resolves its Spark Connect session through the default engine connection
  private val connection = SparkConnectConnection(Environment.defaultEngineConnectionId, SparkConnectTestUtil.url)
  instanceRegistry.register(connection)
  implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec)

  private def assumeDeltaAvailable(): Unit = {
    assume(SparkConnectTestUtil.serverAvailable, s"No Spark Connect server available at ${SparkConnectTestUtil.url}")
    assume(connection.sparkSession.conf.getOption("spark.sql.extensions").exists(_.contains("DeltaSparkSessionExtension")),
      "No delta lake support on Spark Connect server, see start-spark-connect.sh")
  }

  test("config type DeltaLakeTableDataObject is parsed and routed to the Spark Connect engine") {
    // no server needed: parsing and engine discovery are client-side
    val config = ConfigFactory.parseString(
      """
        |id = testDeltaParse
        |type = DeltaLakeTableDataObject
        |table = { db = default, name = sdlb_test_delta_parse }
        |""".stripMargin)
    val dataObject = ConfigParser.parseConfigObject[DataObject](config)
    assert(dataObject.isInstanceOf[DeltaLakeTableDataObject])
    val supportedTypes = dataObject.asInstanceOf[DeltaLakeTableDataObject].getSubFeedSupportedTypes
    assert(supportedTypes.size == 1 && supportedTypes.head =:= typeOf[SparkConnectSubFeed])
  }

  test("write and read delta table roundtrip with delta operation metrics") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = DeltaLakeTableDataObject("testDelta", table = Table(Some("default"), "sdlb_test_delta"))
    dataObject.dropTable
    assert(!dataObject.isTableExisting)

    val metrics = dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    assert(dataObject.isTableExisting)
    assert(metrics.nonEmpty) // delta operation metrics from DESCRIBE HISTORY

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

  test("partitioned delta table: overwrite partitions, list and delete partitions") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = DeltaLakeTableDataObject("testDeltaPart", table = Table(Some("default"), "sdlb_test_delta_part"), partitions = Seq("dt"))
    dataObject.dropTable

    val df = Seq((1L, "a", "20240101"), (2L, "b", "20240101"), (3L, "c", "20240102")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df), Seq(), isRecursiveInput = false, None) // dynamic overwrite on new table
    assert(dataObject.listPartitions.toSet == Set(PartitionValues(Map("dt" -> "20240101")), PartitionValues(Map("dt" -> "20240102"))))

    // overwrite partition 20240101 with new data
    val df2 = Seq((4L, "d", "20240101")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df2), Seq(PartitionValues(Map("dt" -> "20240101"))), isRecursiveInput = false, None)
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 2) // partition 20240101 overwritten with 1 row, 20240102 untouched

    // delete partition
    dataObject.deletePartitions(Seq(PartitionValues(Map("dt" -> "20240102"))))
    assert(dataObject.listPartitions == Seq(PartitionValues(Map("dt" -> "20240101"))))

    dataObject.dropTable
  }

  test("merge into delta table by primary key") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = DeltaLakeTableDataObject("testDeltaMerge", table = Table(Some("default"), "sdlb_test_delta_merge", primaryKey = Some(Seq("id"))), saveMode = SDLSaveMode.Merge)
    dataObject.dropTable

    // first write creates the table
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    // second write merges: update id=2, insert id=3
    val metrics = dataObject.writeDataFrame(SparkConnectDataFrame(Seq((2L, "b2"), (3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    val result = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).asInstanceOf[SparkConnectDataFrame].inner.collect().map(r => (r.getLong(0), r.getString(1))).toSet
    assert(result == Set((1L, "a"), (2L, "b2"), (3L, "c")))
    assert(metrics.get("rows_inserted").contains(1L))
    assert(metrics.get("rows_updated").contains(1L))

    dataObject.dropTable
  }

  test("vacuum via SQL after write") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    // retentionPeriod above default retentionDurationCheck threshold of 168h
    val dataObject = DeltaLakeTableDataObject("testDeltaVacuum", table = Table(Some("default"), "sdlb_test_delta_vacuum"), retentionPeriod = Some(200))
    dataObject.dropTable
    // write triggers vacuum at the end
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    // explicit vacuum
    dataObject.vacuum
    dataObject.dropTable
  }

  test("getStats via DESCRIBE DETAIL/HISTORY, getColumnStats empty") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = DeltaLakeTableDataObject("testDeltaStats", table = Table(Some("default"), "sdlb_test_delta_stats"))
    dataObject.dropTable
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)

    val stats = dataObject.getStats()
    assert(stats.get(TableStatsType.NumRows.toString).contains(2L))
    assert(stats.contains(TableStatsType.CreatedAt.toString))
    assert(stats.contains(TableStatsType.SizeInBytesCurrent.toString))
    assert(dataObject.getColumnStats(update = false, None) == Map())

    dataObject.dropTable
  }

  test("incremental CDC read with setState/getState") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = DeltaLakeTableDataObject("testDeltaCdc", table = Table(Some("default"), "sdlb_test_delta_cdc", primaryKey = Some(Seq("id"))))
    dataObject.dropTable
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)

    // first incremental read: CDC not yet activated -> full read, activates CDC for future writes
    dataObject.setState(dataObject.getState)
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 2)
    val stateAfterActivation = dataObject.getState // getState already returns the next version to read from

    // append new row, recorded by CDC
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, Some(SaveModeGenericOptions(SDLSaveMode.Append)))

    // incremental read from version after activation -> only the appended row
    dataObject.setState(stateAfterActivation)
    val dfInc = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed])
    assert(dfInc.count == 1)
    assert(dfInc.asInstanceOf[SparkConnectDataFrame].inner.collect().map(_.getLong(0)).toSeq == Seq(3L))

    dataObject.dropTable
  }
}
