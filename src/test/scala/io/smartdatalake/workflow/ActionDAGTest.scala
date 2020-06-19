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
package io.smartdatalake.workflow

import java.nio.file.Files
import java.time.LocalDateTime

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{PartitionDiffMode, SparkStreamingOnceMode}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.action.customlogic.{CustomDfsTransformer, CustomDfsTransformerConfig}
import io.smartdatalake.workflow.dataobject.{CsvFileDataObject, HiveTableDataObject, JsonFileDataObject, ParquetFileDataObject, Table, TickTockHiveTableDataObject}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class ActionDAGTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear()
  }

  test("action dag with 2 actions in sequence") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", srcPath, table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgt1Table.db.get, tgt1Table.name )
    val tgt1Path = tempPath+s"/${tgt1Table.fullName}"
    val tgt1DO = TickTockHiveTableDataObject("tgt1", tgt1Path, table = tgt1Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgt2Table.db.get, tgt2Table.name )
    val tgt2Path = tempPath+s"/${tgt2Table.fullName}"
    val tgt2DO = HiveTableDataObject( "tgt2", tgt2Path, table = tgt2Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, l1)
    val action1 = DeduplicateAction("a", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id)
    val actions: Seq[SparkSubFeedAction] = Seq(action1, action2)
    val dag: ActionDAGRun = ActionDAGRun(actions, "test")

    // exec dag
    dag.prepare
    dag.init
    dag.exec

    // check result
    val r1 = session.table(s"${tgt2Table.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    // check metrics for HiveTableDataObject
    val action2MainMetrics = action2.getFinalMetrics(action2.outputId).get.getMainInfos
    assert(action2MainMetrics("records_written")==1)
    assert(action2MainMetrics.isDefinedAt("bytes_written"))
    assert(action2MainMetrics("num_tasks")==1)
  }

  test("action dag with 2 dependent actions from same predecessor") {
    // Action B and C depend on Action A

    // setup DataObjects
    val feed = "actionpipeline"
    val srcTableA = Table(Some("default"), "ap_input")
    HiveUtil.dropTable(session, srcTableA.db.get, srcTableA.name )
    val srcPath = tempPath+s"/${srcTableA.fullName}"
    val srcDO = HiveTableDataObject( "A", srcPath, table = srcTableA, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtATable = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtATable.db.get, tgtATable.name )
    val tgtAPath = tempPath+s"/${tgtATable.fullName}"
    val tgtADO = TickTockHiveTableDataObject("tgt_A", tgtAPath, table = tgtATable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtADO)

    val tgtBTable = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtBTable.db.get, tgtBTable.name )
    val tgtBPath = tempPath+s"/${tgtBTable.fullName}"
    val tgtBDO = HiveTableDataObject( "tgt_B", tgtBPath, table = tgtBTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtBDO)

    val tgtCTable = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtCTable.db.get, tgtCTable.name )
    val tgtCPath = tempPath+s"/${tgtCTable.fullName}"
    val tgtCDO = HiveTableDataObject( "tgt_C", tgtCPath, table = tgtCTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtCDO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTableA, srcPath, l1)
    val actions = Seq(
      DeduplicateAction("a", srcDO.id, tgtADO.id),
      CopyAction("b", tgtADO.id, tgtBDO.id),
      CopyAction("c", tgtADO.id, tgtCDO.id)
    )
    val dag = ActionDAGRun(actions, "test")

    // exec dag
    dag.prepare
    dag.init
    dag.exec

    val r1 = session.table(s"${tgtBTable.fullName}")
      .select($"rating")
      .as[Int].collect.toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    val r2 = session.table(s"${tgtCTable.fullName}")
      .select($"rating")
      .as[Int].collect.toSeq
    assert(r2.size == 1)
    assert(r2.head == 5)
  }

  test("action dag where first actions has multiple input subfeeds") {
    // Action B and C depend on Action A

    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable1 = Table(Some("default"), "input1")
    HiveUtil.dropTable(session, srcTable1.db.get, srcTable1.name )
    val srcPath1 = tempPath+s"/${srcTable1.fullName}"
    val srcDO1 = HiveTableDataObject( "src1", srcPath1, table = srcTable1, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO1)
    val srcTable2 = Table(Some("default"), "input2")
    HiveUtil.dropTable(session, srcTable2.db.get, srcTable2.name )
    val srcPath2 = tempPath+s"/${srcTable2.fullName}"
    val srcDO2 = HiveTableDataObject( "src2", srcPath2, table = srcTable2, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO2)
    val tgtTable = Table(Some("default"), "output", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtTable.db.get, tgtTable.name )
    val tgtPath = tempPath+s"/${tgtTable.fullName}"
    val tgtDO = HiveTableDataObject("tgt1", tgtPath, table = tgtTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeDataFrame(l1, Seq())
    srcDO2.writeDataFrame(l1, Seq())
    val actions = Seq(
      CustomSparkAction("a", inputIds = Seq(srcDO1.id, srcDO2.id), outputIds = Seq(tgtDO.id), CustomDfsTransformerConfig(className=Some("io.smartdatalake.workflow.action.TestDfsTransformerFilterDummy")))
    )
    val dag = ActionDAGRun(actions, "test")

    // exec dag
    dag.prepare
    dag.init
    dag.exec

    val r1 = tgtDO.getDataFrame(Seq())
      .select($"rating")
      .as[Int].collect.toSeq
    assert(r1 == Seq(5))
  }

  test("action dag with four dependencies") {
    // Action B and C depend on Action A
    // Action D depends on Action B and C (uses CustomSparkAction with multiple inputs)

    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "A", srcPath, table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)

    val tgtATable = Table(Some("default"), "tgt_a", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtATable.db.get, tgtATable.name )
    val tgtAPath = tempPath+s"/${tgtATable.fullName}"
    val tgtADO = TickTockHiveTableDataObject("tgt_A", tgtAPath, table = tgtATable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtADO)

    val tgtBTable = Table(Some("default"), "tgt_b", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtBTable.db.get, tgtBTable.name )
    val tgtBPath = tempPath+s"/${tgtBTable.fullName}"
    val tgtBDO = HiveTableDataObject( "tgt_B", tgtBPath, table = tgtBTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtBDO)

    val tgtCTable = Table(Some("default"), "tgt_c", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtCTable.db.get, tgtCTable.name )
    val tgtCPath = tempPath+s"/${tgtCTable.fullName}"
    val tgtCDO = HiveTableDataObject( "tgt_C", tgtCPath, table = tgtCTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtCDO)

    val tgtDTable = Table(Some("default"), "tgt_d", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtDTable.db.get, tgtDTable.name )
    val tgtDPath = tempPath+s"/${tgtDTable.fullName}"
    val tgtDDO = HiveTableDataObject( "tgt_D", tgtDPath, table = tgtDTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDDO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val customTransfomer = CustomDfsTransformerConfig(className = Some("io.smartdatalake.workflow.TestActionDagTransformer"))
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, l1)
    val actions = Seq(
      DeduplicateAction("A", srcDO.id, tgtADO.id),
      CopyAction("B", tgtADO.id, tgtBDO.id),
      CopyAction("C", tgtADO.id, tgtCDO.id),
      CustomSparkAction("D", List(tgtBDO.id,tgtCDO.id), List(tgtDDO.id), transformer = customTransfomer)
    )
    val dag = ActionDAGRun(actions, "test")

    // exec dag
    dag.prepare
    dag.init
    dag.exec

    val r1 = session.table(s"${tgtBTable.fullName}")
      .select($"rating")
      .as[Int].collect.toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    val r2 = session.table(s"${tgtCTable.fullName}")
      .select($"rating")
      .as[Int].collect.toSeq
    assert(r2.size == 1)
    assert(r2.head == 5)

    val r3 = session.table(s"${tgtDTable.fullName}")
      .select($"rating".cast("int"))
      .as[Int].collect.toSeq
    r3.foreach(println)
    assert(r3.size == 1)
    assert(r3.head == 10)
  }


  test("action dag with 2 actions and positive top-level partition values filter") {

    // setup DataObjects
    val feed = "actiondag"
    val srcTable = Table(Some("default"), "ap_input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject( "src1", srcPath, partitions = Seq("dt","type"), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgt1Table.db.get, tgt1Table.name )
    val tgt1Path = tempPath+s"/${tgt1Table.fullName}"
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = TickTockHiveTableDataObject( "tgt1", tgt1Path, partitions = Seq("dt","type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgt2Table.db.get, tgt2Table.name )
    val tgt2Path = tempPath+s"/${tgt2Table.fullName}"
    // second table has partition columns dt only (reduced)
    val tgt2DO = HiveTableDataObject( "tgt2", tgt2Path, partitions = Seq("dt"), table = tgt2Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt2DO)

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe","john",5) // partition 20180101 is included in partition values filter
      ,("20190101", "company", "olmo","-",10)) // partition 20190101 is not included
      .toDF("dt", "type", "lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, dfSrc, Seq("dt","type"))

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val actions = Seq(
      DeduplicateAction("a", srcDO.id, tgt1DO.id),
      CopyAction("b", tgt1DO.id, tgt2DO.id)
    )
    val dag = ActionDAGRun(actions, "test", partitionValues = Seq(PartitionValues(Map("dt"->"20180101"))))

    // exec dag
    dag.prepare
    dag.init
    dag.exec

    val r1 = session.table(s"${tgt2Table.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    val dfTgt2 = session.table(s"${tgt2Table.fullName}")
    assert(Seq("dt", "type", "lastname", "firstname", "rating").diff(dfTgt2.columns).isEmpty)
    val recordsTgt2 = dfTgt2
      .select($"rating")
      .as[Int].collect().toSeq
    assert(recordsTgt2.size == 1)
    assert(recordsTgt2.head == 5)
  }

  test("action dag file ingest - from file to dataframe") {

    val feed = "actiondag"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)

    // setup src DataObject
    val srcDO = new CsvFileDataObject( "src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(srcDO)

    // setup tgt1 CSV DataObject
    val srcSchema = srcDO.getDataFrame().head.schema // infer schema from original CSV
    val tgt1DO = new CsvFileDataObject( "tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","), schema = Some(srcSchema))
    instanceRegistry.register(tgt1DO)

    // setup tgt2 Hive DataObject
    val tgt2Table = Table(Some("default"), "ap_copy")
    HiveUtil.dropTable(session, tgt2Table.db.get, tgt2Table.name )
    val tgt2Path = tempPath+s"/${tgt2Table.fullName}"
    val tgt2DO = HiveTableDataObject( "tgt2", tgt2Path, table = tgt2Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt2DO)

    // prepare ActionPipeline
    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val action1 = FileTransferAction("fta", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("ca", tgt1DO.id, tgt2DO.id)
    val dag = ActionDAGRun(Seq(action1, action2), "test")

    // run dag
    dag.prepare
    dag.init
    dag.exec

    // read src/tgt and count
    val dfSrc = srcDO.getDataFrame()
    val srcCount = dfSrc.count
    val dfTgt1 = tgt1DO.getDataFrame()
    val dfTgt2 = tgt2DO.getDataFrame()
    dfTgt2.show(3)
    val tgtCount = dfTgt2.count
    assert(srcCount == tgtCount)
  }

  test("action dag file export - from dataframe to file") {

    val feed = "actiondag"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)

    // setup src DataObject
    val srcDO = new CsvFileDataObject( "src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(srcDO)

    // setup tgt1 Hive DataObject
    val tgt1Table = Table(Some("default"), "ap_copy")
    HiveUtil.dropTable(session, tgt1Table.db.get, tgt1Table.name )
    val tgt1Path = tempPath+s"/${tgt1Table.fullName}"
    val tgt1DO = HiveTableDataObject( "tgt1", tgt1Path, table = tgt1Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt1DO)

    // setup tgt2 CSV DataObject
    val srcSchema = srcDO.getDataFrame().head.schema // infer schema from original CSV
    val tgt2DO = new CsvFileDataObject( "tgt2", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","), schema = Some(srcSchema))
    instanceRegistry.register(tgt2DO)

    // setup tgt3 CSV DataObject
    val tgt3DO = new CsvFileDataObject( "tgt3", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","), schema = Some(srcSchema))
    instanceRegistry.register(tgt3DO)

    // prepare ActionPipeline
    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val action1 = CopyAction("ca1", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("ca2", tgt1DO.id, tgt2DO.id)
    val action3 = FileTransferAction("fta", tgt2DO.id, tgt3DO.id)
    val dag = ActionDAGRun(Seq(action1, action2, action3), "test")

    // run dag
    dag.prepare
    dag.init
    dag.exec

    // read src/tgt and count
    val dfSrc = srcDO.getDataFrame()
    val srcCount = dfSrc.count
    val dfTgt3 = tgt1DO.getDataFrame()
    val tgtCount = dfTgt3.count
    assert(srcCount == tgtCount)

    // check metrics for CsvFileDataObject
    val action2MainMetrics = action2.getFinalMetrics(action2.outputId).get.getMainInfos
    assert(action2MainMetrics("records_written")==40)
    assert(action2MainMetrics.isDefinedAt("bytes_written"))
    assert(action2MainMetrics("num_tasks")==1)
  }

  test("action dag with 2 actions in sequence and initExecutionMode=PartitionDiffMode") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", srcPath, table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgt1Table.db.get, tgt1Table.name )
    val tgt1Path = tempPath+s"/${tgt1Table.fullName}"
    val tgt1DO = TickTockHiveTableDataObject("tgt1", tgt1Path, table = tgt1Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgt2Table.db.get, tgt2Table.name )
    val tgt2Path = tempPath+s"/${tgt2Table.fullName}"
    val tgt2DO = HiveTableDataObject( "tgt2", tgt2Path, table = tgt2Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    val expectedPartitions = Seq(PartitionValues(Map("lastname"->"doe")))
    srcDO.writeDataFrame(df1, expectedPartitions)
    val actions: Seq[SparkSubFeedAction] = Seq(
      DeduplicateAction("a", srcDO.id, tgt1DO.id, initExecutionMode = Some(PartitionDiffMode()))
      , CopyAction("b", tgt1DO.id, tgt2DO.id)
    )
    val dag: ActionDAGRun = ActionDAGRun(actions, "test")

    // first dag run
    dag.prepare
    dag.init
    dag.exec

    // check
    val r1 = tgt2DO.getDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)
    assert(tgt2DO.listPartitions == expectedPartitions)

    // second dag run - skip action execution because there are no new partitions to process
    dag.prepare
    intercept[NoDataToProcessWarning](dag.init)
  }

  test("action dag with 2 actions in sequence and executionMode=SparkStreamingOnceMode") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = DataType.fromDDL("lastname string, firstname string, rating int").asInstanceOf[StructType]
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(schema))
    instanceRegistry.register(srcDO)
    val tgt1DO = JsonFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = JsonFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'), saveMode = SaveMode.Append)
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(df1, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingOnceMode(checkpointLocation = tempDir.resolve("stateA").toUri.toString)))
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingOnceMode(checkpointLocation = tempDir.resolve("stateB").toUri.toString)))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2), "test")

    // first dag run, first file processed
    dag.prepare
    dag.init
    dag.exec

    // check
    val r1 = tgt2DO.getDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    // second dag run - no data to process
    dag.reset
    dag.prepare
    dag.init
    dag.exec

    // check
    val r2 = tgt2DO.getDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 5)

    // third dag run - new data to process
    val df2 = Seq(("doe","john 2",10)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(df2, Seq())
    dag.reset
    dag.prepare
    dag.init
    dag.exec

    // check
    val r3 = tgt2DO.getDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r3.size == 2)

    // check metrics
    val action2MainMetrics = action2.getFinalMetrics(action2.outputId).get.getMainInfos
    assert(action2MainMetrics("records_written")==1)
  }
}


class TestActionDagTransformer extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._
    val dfTransformed = dfs("tgt_B")
    .union(dfs("tgt_C"))
    .groupBy($"lastname",$"firstname")
    .agg(sum($"rating").as("rating"))

    Map("tgt_D" -> dfTransformed)
  }
}

