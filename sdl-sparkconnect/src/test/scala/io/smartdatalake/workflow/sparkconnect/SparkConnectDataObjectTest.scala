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

import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions}
import io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.SparkConnectConnection
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectDataFrame, SparkConnectSubFeed}
import io.smartdatalake.workflow.dataobject.SparkConnectTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * Test for SparkConnectTableDataObject.
 * Needs a Spark Connect server, see [[SparkConnectTestUtil]] for how it is resolved or started.
 * Tests are cancelled (not failed) if no server is available.
 */
class SparkConnectDataObjectTest extends AnyFunSuite {

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  private val connection = SparkConnectConnection("sparkConnectCon", SparkConnectTestUtil.url)
  instanceRegistry.register(connection)
  implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec)

  private def assumeServerAvailable(): Unit = assume(SparkConnectTestUtil.serverAvailable, s"No Spark Connect server available at ${SparkConnectTestUtil.url}")

  test("write and read table roundtrip") {
    assumeServerAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "value")

    val dataObject = SparkConnectTableDataObject("testTable", Table(Some("default"), "sdlb_test_table"), connectionId = "sparkConnectCon", saveMode = SDLSaveMode.Overwrite)
    dataObject.dropTable
    assert(!dataObject.isTableExisting)

    dataObject.writeDataFrame(SparkConnectDataFrame(df), Seq(), isRecursiveInput = false, None)
    assert(dataObject.isTableExisting)

    val dfRead = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed])
    assert(dfRead.count == 3)
    assert(dfRead.schema.columns == Seq("id", "value"))

    // append
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((4L, "d")).toDF("id", "value")), Seq(), isRecursiveInput = false, Some(io.smartdatalake.definitions.SaveModeGenericOptions(SDLSaveMode.Append)))
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 4)

    // cleanup
    dataObject.dropTable
    assert(!dataObject.isTableExisting)
  }

  test("init phase reads schema only") {
    assumeServerAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = SparkConnectTableDataObject("testTableInit", Table(Some("default"), "sdlb_test_table_init"), connectionId = "sparkConnectCon")
    dataObject.dropTable
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    val contextInit = context.copy(phase = ExecutionPhase.Init)
    val dfInit = dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed])(contextInit)
    assert(dfInit.schema.columns == Seq("id", "value"))
    assert(dfInit.count <= 1) // limited to 1 row outside exec phase
    dataObject.dropTable
  }

  test("companion functions work with default session") {
    assumeServerAvailable()
    SparkConnectSubFeed._defaultSparkSession = Some(connection.sparkSession)
    val schema = SparkConnectSubFeed.createSchemaFromDdl("id bigint, value string")
    val emptyDf = SparkConnectSubFeed.getEmptyDataFrame(schema, "testTable")
    assert(emptyDf.schema.columns == Seq("id", "value"))
    assert(emptyDf.count == 0)
    val df = SparkConnectSubFeed.createDataFrame(Seq(("a", 1), ("b", 2)), Seq("value", "id"))
    assert(df.count == 2)
  }

  // delta lake support on the server side is needed for merge and partition tests, as row-level operations
  // are not supported for plain parquet tables
  private def assumeDeltaAvailable(): Unit = {
    assumeServerAvailable()
    assume(connection.sparkSession.conf.getOption("spark.sql.extensions").exists(_.contains("DeltaSparkSessionExtension")),
      "No delta lake support on Spark Connect server, see start-spark-connect.sh")
  }

  test("partitioned table: write, list, overwrite and delete partitions") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = SparkConnectTableDataObject("testTablePart", Table(Some("default"), "sdlb_test_table_part"),
      partitions = Seq("dt"), format = Some("delta"), connectionId = "sparkConnectCon", saveMode = SDLSaveMode.Overwrite)
    dataObject.dropTable

    val df = Seq((1L, "a", "20240101"), (2L, "b", "20240101"), (3L, "c", "20240102")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df), Seq(), isRecursiveInput = false, None)
    assert(dataObject.listPartitions.toSet == Set(PartitionValues(Map("dt" -> "20240101")), PartitionValues(Map("dt" -> "20240102"))))

    // overwrite partition 20240101 with new data
    val df2 = Seq((4L, "d", "20240101")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df2), Seq(PartitionValues(Map("dt" -> "20240101"))), isRecursiveInput = false, None)
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 2) // partition 20240101 overwritten with 1 row, 20240102 untouched

    // overwrite without partition values is a dynamic partition overwrite, as SparkConnectConnection sets
    // spark.sql.sources.partitionOverwriteMode=dynamic by default (like SparkClassicConnection).
    // Note that with partitionOverwriteMode=static this would throw a ProcessingLogicException instead,
    // to protect from unintentionally deleting all partition data.
    val df3 = Seq((5L, "e", "20240101"), (6L, "f", "20240101")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df3), Seq(), isRecursiveInput = false, None)
    assert(dataObject.getDataFrame(Seq(), typeOf[SparkConnectSubFeed]).count == 3) // partition 20240101 overwritten with 2 rows, 20240102 untouched

    // delete partition
    dataObject.deletePartitions(Seq(PartitionValues(Map("dt" -> "20240102"))))
    assert(dataObject.listPartitions == Seq(PartitionValues(Map("dt" -> "20240101"))))

    dataObject.dropTable
  }

  test("partitioned table: dynamic partition overwrite") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = SparkConnectTableDataObject("testTablePartDyn", Table(Some("default"), "sdlb_test_table_part_dyn"),
      partitions = Seq("dt"), format = Some("delta"), options = Map("partitionOverwriteMode" -> "dynamic"),
      connectionId = "sparkConnectCon", saveMode = SDLSaveMode.Overwrite)
    dataObject.dropTable

    val df = Seq((1L, "a", "20240101"), (2L, "b", "20240102")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df), Seq(), isRecursiveInput = false, None)

    // overwrite only the partitions contained in the DataFrame
    val df2 = Seq((3L, "c", "20240102")).toDF("id", "value", "dt")
    dataObject.writeDataFrame(SparkConnectDataFrame(df2), Seq(), isRecursiveInput = false, None)
    val result = dataObject.getSparkConnectDataFrame().inner.collect().map(r => (r.getLong(0), r.getString(1), r.getString(2))).toSet
    assert(result == Set((1L, "a", "20240101"), (3L, "c", "20240102")))

    dataObject.dropTable
  }

  test("merge into table by primary key") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = SparkConnectTableDataObject("testTableMerge", Table(Some("default"), "sdlb_test_table_merge", primaryKey = Some(Seq("id"))),
      format = Some("delta"), connectionId = "sparkConnectCon", saveMode = SDLSaveMode.Merge)
    dataObject.dropTable

    // first write creates the table
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a"), (2L, "b")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    // second write merges: update id=2, insert id=3
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((2L, "b2"), (3L, "c")).toDF("id", "value")), Seq(), isRecursiveInput = false, None)
    val result = dataObject.getSparkConnectDataFrame().inner.collect().map(r => (r.getLong(0), r.getString(1))).toSet
    assert(result == Set((1L, "a"), (2L, "b2"), (3L, "c")))

    dataObject.dropTable
  }

  test("merge with delete condition") {
    assumeDeltaAvailable()
    val session = connection.sparkSession
    import session.implicits._
    val dataObject = SparkConnectTableDataObject("testTableMergeDel", Table(Some("default"), "sdlb_test_table_merge_del", primaryKey = Some(Seq("id"))),
      format = Some("delta"), connectionId = "sparkConnectCon", saveMode = SDLSaveMode.Merge)
    dataObject.dropTable

    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a", false), (2L, "b", false)).toDF("id", "value", "deleted")), Seq(), isRecursiveInput = false, None)
    // merge with delete condition: delete id=1, update id=2
    val saveModeOptions = SaveModeMergeOptions(deleteCondition = Some("new.deleted"))
    dataObject.writeDataFrame(SparkConnectDataFrame(Seq((1L, "a", true), (2L, "b2", false)).toDF("id", "value", "deleted")), Seq(), isRecursiveInput = false, Some(saveModeOptions))
    val result = dataObject.getSparkConnectDataFrame().inner.collect().map(r => (r.getLong(0), r.getString(1))).toSet
    assert(result == Set((2L, "b2")))

    dataObject.dropTable
  }
}
