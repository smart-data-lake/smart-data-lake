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

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.action.executionMode._
import io.smartdatalake.workflow.action.generic.transformer.{SQLDfTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfsTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}

class ActionDAGTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString
  private val defaultHadoopConf: Configuration = new Configuration()

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit var contextInit: ActionPipelineContext = _
  var contextPrep: ActionPipelineContext = _
  var contextExec: ActionPipelineContext = _

  before {
    contextInit = TestUtil.getDefaultActionPipelineContext
    contextPrep = contextInit.copy(phase = ExecutionPhase.Prepare)
    contextExec = contextInit.copy(phase = ExecutionPhase.Exec) // note that mutable Map dataFrameReuseStatistics is shared between contextInit & contextExec like this!
    instanceRegistry.clear()
  }

  test("action dag with 2 actions in sequence with state") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val statePath = tempPath+"stateTest/"
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val action1 = DeduplicateAction("a", srcDO.id, tgt1DO.id, metricsFailCondition = Some(s"dataObjectId = '${tgt1DO.id.id}' and key = 'records_written' and value = 0"))
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id)
    val actions: Seq[DataFrameOneToOneActionImpl] = Seq(action1, action2)
    val stateStore = HadoopFileActionDAGRunStateStore(statePath, contextInit.application, defaultHadoopConf)
    val dag: ActionDAGRun = ActionDAGRun(actions, stateStore = Some(stateStore))

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    assert(contextInit.dataFrameReuseStatistics((tgt1DO.id,Seq())).size == 1)
    dag.exec(contextExec)
    assert(contextExec.dataFrameReuseStatistics.forall(_._2.isEmpty))

    // check result
    val r1 = session.table(s"${tgt2Table.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    // check metrics for HiveTableDataObject
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written")==1)
    assert(action2MainMetrics.isDefinedAt("bytes_written"))
    assert(action2MainMetrics("num_tasks")==1)

    // check state: two actions succeeded
    val latestState = stateStore.getLatestStateId().get
    val previousRunState = stateStore.recoverRunState(latestState)
    val previousActionState = previousRunState.actionsState.mapValues(_.state).toMap
    val resultActionState = actions.map( a => (a.id, RuntimeEventState.SUCCEEDED)).toMap
    assert(previousActionState == resultActionState)
  }

  test("action dag with 2 actions in sequence and breakDataframeLineage=true") {
    // Note: if you set breakDataframeLineage=true, SDL doesn't pass the DataFrame to the next action.
    // Nevertheless the schema should be passed on for early validation in init phase, otherwise SDL reads the DataObject which might not yet have been created.
    // To support this SDL creates and passes on an empty dummy-DataFrame in init phase, just containing the schema, which is replaced in exec phase by the real fresh DataFrame read from the DataObject which now should be existing.
    // see also #119

    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val action1 = DeduplicateAction("a", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, breakDataFrameLineage = true)
    val actions: Seq[DataFrameOneToOneActionImpl] = Seq(action1, action2)
    val dag: ActionDAGRun = ActionDAGRun(actions)

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    assert(!contextInit.dataFrameReuseStatistics.contains((tgt1DO.id, Seq()))) // no reuse because of breakDataframeLineage
    dag.exec(contextExec)
    assert(!contextExec.dataFrameReuseStatistics.contains((tgt1DO.id, Seq())))

    // check result
    val r1 = session.table(s"${tgt2Table.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    // check metrics for HiveTableDataObject
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written")==1)
    assert(action2MainMetrics.isDefinedAt("bytes_written"))
    assert(action2MainMetrics("num_tasks")==1)
  }

  test("action dag with 2 actions in sequence where 2nd action reads different schema than produced by last action") {
    // Note: Some DataObjects remove & add columns on read (e.g. KafkaTopicDataObject, SparkFileDataObject)
    // In this cases we have to break the lineage und create a dummy DataFrame in init phase.

    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1DO = CsvFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), filenameColumn=Some("_filename"), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(tgt1DO)
    val tgt2DO = CsvFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val l1 = Seq(("doe-john", 5)).toDF("name", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, transformers = Seq(SQLDfTransformer(code = "select _filename, rating from tgt1")))
    instanceRegistry.register(Seq(action1, action2))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2))

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    assert(!contextInit.dataFrameReuseStatistics.contains((tgt1DO.id, Seq()))) // no reuse because of different schema
    dag.exec(contextExec)
    assert(!contextInit.dataFrameReuseStatistics.contains((tgt1DO.id, Seq())))

    // check result
    val dfR1 = tgt2DO.getSparkDataFrame()
    assert(dfR1.columns.toSet == Set("_filename","rating"))
    val r1 = dfR1
      .select($"rating")
      .as[String].collect().toSeq
    assert(r1 == Seq("5"))

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written") == 1)
  }

  test("action dag with 2 dependent actions from same predecessor, PartitionDiffMode and another action with no data to process") {
    // Action B and C depend on Action A

    // setup DataObjects
    val feed = "actionpipeline"
    val srcTableA = Table(Some("default"), "input_a")
    val srcADO = HiveTableDataObject( "src_A", Some(tempPath+s"/${srcTableA.fullName}"), table = srcTableA, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    srcADO.dropTable
    instanceRegistry.register(srcADO)

    val srcTableB = Table(Some("default"), "input_b")
    val srcBDO = HiveTableDataObject( "src_B", Some(tempPath+s"/${srcTableB.fullName}"), table = srcTableB, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    srcBDO.dropTable
    instanceRegistry.register(srcBDO)

    val tgtATable = Table(Some("default"), "tgt_a", None, Some(Seq("lastname","firstname")))
    val tgtADO = TickTockHiveTableDataObject("tgt_A", Some(tempPath+s"/${tgtATable.fullName}"), table = tgtATable, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    tgtADO.dropTable
    instanceRegistry.register(tgtADO)

    val tgtBTable = Table(Some("default"), "tgt_b", None, Some(Seq("lastname","firstname")))
    val tgtBDO = HiveTableDataObject( "tgt_B", Some(tempPath+s"/${tgtBTable.fullName}"), table = tgtBTable, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    tgtBDO.dropTable
    instanceRegistry.register(tgtBDO)

    val tgtCTable = Table(Some("default"), "tgt_c", None, Some(Seq("lastname","firstname")))
    val tgtCDO = HiveTableDataObject( "tgt_C", Some(tempPath+s"/${tgtCTable.fullName}"), table = tgtCTable, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    tgtCDO.dropTable
    instanceRegistry.register(tgtCDO)

    val tgtDTable = Table(Some("default"), "ap_copy3", None, Some(Seq("lastname","firstname")))
    val tgtDDO = HiveTableDataObject( "tgt_D", Some(tempPath+s"/${tgtDTable.fullName}"), table = tgtDTable, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    tgtDDO.dropTable
    instanceRegistry.register(tgtDDO)

    // prepare DAG
    val dataA = Seq(("doe","john",5),("dau","bob",3)).toDF("lastname", "firstname", "rating")
    val dataB = Seq(("doe","john",10),("dau","bob",6)).toDF("lastname", "firstname", "rating")
    srcADO.writeSparkDataFrame(dataA, Seq())
    srcBDO.writeSparkDataFrame(dataB, Seq())
    tgtADO.writeSparkDataFrame(dataA.where($"lastname"==="doe").withColumn("dl_ts_captured", current_timestamp()), Seq()) // populate tgtA with "doe", so there should be only 1 partition to process (dau)
    tgtDDO.writeSparkDataFrame(dataA, Seq()) // populate tgtD so there should be no partitions left to process
    instanceRegistry.register(DeduplicateAction("a", srcADO.id, tgtADO.id, executionMode = Some(PartitionDiffMode()), metadata = Some(ActionMetadata(feed = Some(feed)))))
    // srcB should be filtered with partition values received from tgtA. Transformer selects records from srcB, so "doe, bob, 6" should be inserted in tgtB, but "doe, john, 3" should remain.
    instanceRegistry.register(CustomDataFrameAction("b", Seq(tgtADO.id,srcBDO.id), Seq(tgtBDO.id), executionMode = Some(FailIfNoPartitionValuesMode()), metadata = Some(ActionMetadata(feed = Some(feed))),
      transformers = Seq(SQLDfsTransformer(code = Map(tgtBDO.id.id -> "select * from src_B"))), mainInputId = Some(tgtADO.id)
    ))
    instanceRegistry.register(CopyAction("c", tgtADO.id, tgtCDO.id, metadata = Some(ActionMetadata(feed = Some(feed)))))
    instanceRegistry.register(CopyAction("d", srcADO.id, tgtDDO.id, executionMode = Some(PartitionDiffMode()), metadata = Some(ActionMetadata(feed = Some(feed)))))

    // exec dag
    val sdlb = new DefaultSmartDataLakeBuilder
    val appConfig = SmartDataLakeBuilderConfig(feedSel=feed)
    sdlb.exec(appConfig, SDLExecutionId.executionId1, LocalDateTime.now(), LocalDateTime.now(), Map(), Seq(), Seq(), None, Seq(), simulation = false, globalConfig = GlobalConfig())

    val r1 = tgtBDO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.toSet == Set(6))

    val r2 = tgtCDO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.toSet == Set(3))

    val r3 = tgtDDO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r3.toSet == Set(3,5))
  }

  test("action dag where first actions has multiple input subfeeds, one should ignore filters") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable1 = Table(Some("default"), "input1")
    val srcDO1 = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable1.fullName}"), table = srcTable1, numInitialHdfsPartitions = 1)
    srcDO1.dropTable
    instanceRegistry.register(srcDO1)
    val srcTable2 = Table(Some("default"), "input2")
    val srcDO2 = HiveTableDataObject( "src2", Some(tempPath+s"/${srcTable2.fullName}"), table = srcTable2, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    srcDO2.dropTable
    instanceRegistry.register(srcDO2)
    val srcTable3 = Table(Some("default"), "input3")
    val srcDO3 = HiveTableDataObject( "src3", Some(tempPath+s"/${srcTable3.fullName}"), table = srcTable3, numInitialHdfsPartitions = 1, partitions = Seq("lastname"))
    srcDO3.dropTable
    instanceRegistry.register(srcDO3)
    val tgtTable = Table(Some("default"), "output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject("tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare DAG
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    val l2 = Seq(("xyz","john",5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l1, Seq())
    srcDO2.writeSparkDataFrame(l2.union(l1), Seq())
    srcDO3.writeSparkDataFrame(l2.union(l1), Seq())
    val action1 = CustomDataFrameAction( "a", inputIds = Seq(srcDO1.id, srcDO2.id, srcDO3.id), inputIdsToIgnoreFilter = Seq(srcDO3.id), outputIds = Seq(tgtDO.id)
                                    , transformers = Seq(ScalaClassSparkDfsTransformer(className=classOf[TestDfsUnionOfThree].getName)))
    // filter partition values lastname=xyz: src1 is not partitioned, src2 & src3 have 1 record with partition lastname=doe and 1 record with partition lastname=xyz
    val partitionValuesFilter = Seq(PartitionValues(Map("lastname" -> "doe")))
    val dag = ActionDAGRun(Seq(action1), partitionValues = partitionValuesFilter)

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // as filters are ignored, we expect both records from src3, but only one record from src2
    val r1 = tgtDO.getSparkDataFrame(Seq())
      .select($"lastname", $"firstname", $"origin")
      .as[(String,String,Int)].collect().toSet
    assert(r1 == Set(("doe","john",1),("doe","john",2),("doe","john",3),("xyz","john",3)))
  }

  test("action dag with four dependencies") {
    // Action B and C depend on Action A
    // Action D depends on Action B and C (uses CustomDataFrameAction with multiple inputs)

    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "A", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)

    val tgtATable = Table(Some("default"), "tgt_a", None, Some(Seq("lastname","firstname")))
    val tgtADO = TickTockHiveTableDataObject("tgt_A", Some(tempPath+s"/${tgtATable.fullName}"), table = tgtATable, numInitialHdfsPartitions = 1)
    tgtADO.dropTable
    instanceRegistry.register(tgtADO)

    val tgtBTable = Table(Some("default"), "tgt_b", None, Some(Seq("lastname","firstname")))
    val tgtBDO = HiveTableDataObject( "tgt_B", Some(tempPath+s"/${tgtBTable.fullName}"), table = tgtBTable, numInitialHdfsPartitions = 1)
    tgtBDO.dropTable
    instanceRegistry.register(tgtBDO)

    val tgtCTable = Table(Some("default"), "tgt_c", None, Some(Seq("lastname","firstname")))
    val tgtCDO = HiveTableDataObject( "tgt_C", Some(tempPath+s"/${tgtCTable.fullName}"), table = tgtCTable, numInitialHdfsPartitions = 1)
    tgtCDO.dropTable
    instanceRegistry.register(tgtCDO)

    val tgtDTable = Table(Some("default"), "tgt_d", None, Some(Seq("lastname","firstname")))
    val tgtDDO = HiveTableDataObject( "tgt_D", Some(tempPath+s"/${tgtDTable.fullName}"), table = tgtDTable, numInitialHdfsPartitions = 1)
    tgtDDO.dropTable
    instanceRegistry.register(tgtDDO)

    // prepare DAG
    val customTransfomer = ScalaClassSparkDfsTransformer(className = classOf[TestActionDagTransformer].getName)
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val actions = Seq(
      DeduplicateAction("A", srcDO.id, tgtADO.id),
      CopyAction("B", tgtADO.id, tgtBDO.id),
      CopyAction("C", tgtADO.id, tgtCDO.id),
      CustomDataFrameAction("D", List(tgtBDO.id,tgtCDO.id), List(tgtDDO.id), transformers = Seq(customTransfomer))
    )
    val dag = ActionDAGRun(actions)

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    assert(contextInit.dataFrameReuseStatistics((tgtADO.id, Seq())).size == 2)
    assert(contextInit.dataFrameReuseStatistics((tgtBDO.id, Seq())).size == 1)
    assert(contextInit.dataFrameReuseStatistics((tgtCDO.id, Seq())).size == 1)
    dag.exec(contextExec)
    assert(contextExec.dataFrameReuseStatistics.forall(_._2.isEmpty))

    val r1 = session.table(s"${tgtBTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    val r2 = session.table(s"${tgtCTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 5)

    val r3 = session.table(s"${tgtDTable.fullName}")
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    r3.foreach(println)
    assert(r3.size == 1)
    assert(r3.head == 10)
  }


  test("action dag with two actions writing the same DataObject") {
    // Action A and B write DataObject tgtA
    // Action C reads DataObject tgtA

    // setup DataObjects
    val srcD1 = MockDataObject("src1", partitions = Seq("lastname")).register
    val srcD2 = MockDataObject("src2", partitions = Seq("lastname")).register
    val tgtATable = Table(Some("default"), "tgt_a", None, Some(Seq("lastname","firstname")))
    val tgtADO = TickTockHiveTableDataObject("tgt_A", Some(tempPath+s"/${tgtATable.fullName}"), table = tgtATable, partitions = Seq("lastname"), numInitialHdfsPartitions = 1)
    tgtADO.dropTable
    instanceRegistry.register(tgtADO)

    val tgtCTable = Table(Some("default"), "tgt_c", None, Some(Seq("lastname","firstname")))
    val tgtCDO = HiveTableDataObject( "tgt_C", Some(tempPath+s"/${tgtCTable.fullName}"), table = tgtCTable, partitions = Seq("lastname"), numInitialHdfsPartitions = 1)
    tgtCDO.dropTable
    instanceRegistry.register(tgtCDO)

    // prepare DAG
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcD1.writeSparkDataFrame(l1)
    val l2 = Seq(("peter", "pan", 3)).toDF("lastname", "firstname", "rating")
    srcD2.writeSparkDataFrame(l2)
    val actions = Seq(
      CopyAction("A", srcD1.id, tgtADO.id, executionMode = Some(PartitionDiffMode())),
      CopyAction("B", srcD2.id, tgtADO.id),
      CopyAction("C", tgtADO.id, tgtCDO.id)
    )
    val dag = ActionDAGRun(actions)

    // exec dag 1st run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    {
      val r = tgtCDO.getSparkDataFrame()
        .select($"rating")
        .as[Int].collect().toSeq
      assert(r == Seq(5, 3))
    }

    // exec dag 2st run - action A is skipped because of PartitionDiffMode, but Action C should run nevertheless
    val l1_2 = Seq(("doe", "john", 6)).toDF("lastname", "firstname", "rating")
    srcD1.writeSparkDataFrame(l1_2)
    val l2_2 = Seq(("peter", "pan", 4)).toDF("lastname", "firstname", "rating")
    srcD2.writeSparkDataFrame(l2_2)
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    {
      val r = tgtCDO.getSparkDataFrame()
        .select($"rating")
        .as[Int].collect().toSeq
      assert(r == Seq(5, 4)) // doe is not updated...
    }
  }


  test("action dag with 2 actions and positive top-level partition values filter, ignoring executionMode=PartitionDiffMode") {

    // setup DataObjects
    val feed = "actiondag"
    val srcTable = Table(Some("default"), "ap_input")
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), partitions = Seq("dt","type"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("dt","type","lastname","firstname")))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = TickTockHiveTableDataObject( "tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), partitions = Seq("dt","type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("dt","lastname","firstname")))
    // second table has partition columns dt only (reduced)
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), partitions = Seq("dt"), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe","john",5) // partition 20180101 is included in partition values filter
      ,("20190101", "company", "olmo","-",10)) // partition 20190101 is not included
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc, Seq())

    // prepare DAG
    val actions = Seq(
      DeduplicateAction("a", srcDO.id, tgt1DO.id, executionMode=Some(PartitionDiffMode())), // PartitionDiffMode is ignored because partition values are given below as parameter
      CopyAction("b", tgt1DO.id, tgt2DO.id)
    )
    val dag = ActionDAGRun(actions, partitionValues = Seq(PartitionValues(Map("dt"->"20180101"))))

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

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

  test("action dag file ingest - from file to dataframe with schema inference") {

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
    val tgt1DO = new CsvFileDataObject( "tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(tgt1DO)

    // setup tgt2 Hive DataObject
    val tgt2Table = Table(Some("default"), "ap_copy")
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare ActionPipeline
    val action1 = FileTransferAction("fta", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("ca", tgt1DO.id, tgt2DO.id)
    val dag = ActionDAGRun(Seq(action1, action2))

    // run dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // read src/tgt and count
    val dfSrc = srcDO.getSparkDataFrame()
    val srcCount = dfSrc.count()
    val dfTgt1 = tgt1DO.getSparkDataFrame()
    val dfTgt2 = tgt2DO.getSparkDataFrame()
    val tgtCount = dfTgt2.count()
    assert(srcCount == tgtCount)
  }

  test("action dag file export - from dataframe to file") {

    val feed = "actiondag"
    val srcDir = "testSrc"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)

    // setup src DataObject
    val srcDO = new CsvFileDataObject( "src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(srcDO)

    // setup tgt1 Hive DataObject
    val tgt1Table = Table(Some("default"), "ap_copy")
    val tgt1DO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)

    // setup tgt2 CSV DataObject
    val tgt2DO = new CsvFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(tgt2DO)

    // setup tgt3 CSV DataObject
    val tgt3DO = new CsvFileDataObject( "tgt3", tempDir.resolve("tgt3").toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> ","))
    instanceRegistry.register(tgt3DO)

    // prepare ActionPipeline
    val action1 = CopyAction("ca1", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("ca2", tgt1DO.id, tgt2DO.id)
    val action3 = FileTransferAction("fta", tgt2DO.id, tgt3DO.id)
    val dag = ActionDAGRun(Seq(action1, action2, action3))

    // run dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // read src/tgt and count
    val dfSrc = srcDO.getSparkDataFrame()
    val srcCount = dfSrc.count()
    val dfTgt3 = tgt3DO.getSparkDataFrame()
    val tgtCount = dfTgt3.count()
    assert(srcCount == tgtCount)

    // check metrics for CsvFileDataObject
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written")==40)
    assert(action2MainMetrics.isDefinedAt("bytes_written"))
    assert(action2MainMetrics("num_tasks")==1)

    // check metrics for FileTransferAction
    val action3MainMetrics = TestUtil.getMetrics(action3.getRuntimeInfo().get, action3.outputId)
    assert(action3MainMetrics("files_written")==1)
  }

  test("action dag with 2 actions in sequence and executionMode=PartitionDiffMode and selectExpression") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1, expectedPartitionsCondition = Some("elements['lastname'] != 'xyz'"))
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), table = tgt2Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5),("einstein","albert",2)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(df1, Seq())
    val partitionDiffMode = PartitionDiffMode(
      applyCondition = Some("isStartNode"),
      selectExpression = Some("slice(selectedOutputPartitionValues,-1,1)"), // only one partition: last partition first
      failCondition = Some("size(selectedOutputPartitionValues) = 0 and size(outputPartitionValues) = 0")
    )
    val actions = Seq(
      DeduplicateAction("a", srcDO.id, tgt1DO.id, executionMode = Some(partitionDiffMode)),
      CopyAction("b", tgt1DO.id, tgt2DO.id)
    )
    val dag = ActionDAGRun(actions)

    // first dag run: partition lastname=einstein
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1 == Seq(2))
    assert(tgt2DO.listPartitions ==  Seq(PartitionValues(Map("lastname"->"einstein"))))

    // second dag run: partition lastname=doe
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSet
    assert(r2 == Set(2,5))
    assert(tgt2DO.listPartitions ==  Seq(PartitionValues(Map("lastname"->"doe")), PartitionValues(Map("lastname"->"einstein"))))

    // third dag run - skip action execution because there are no new partitions to process
    dag.prepare(contextPrep)
    val subFeeds = dag.init(contextInit)
    subFeeds.forall(_.isSkipped)
  }

  test( "validate expected partitions") {

    val srcTable = Table(Some("default"), "ap_input")
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", Some(srcPath), table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1, expectedPartitionsCondition = Some("elements['lastname'] != 'xyz'"))
    srcDO.dropTable

    instanceRegistry.register(srcDO)

    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    val tgt1Path = tempPath+s"/${tgt1Table.fullName}"
    val tgt1DO = HiveTableDataObject("tgt1", Some(tgt1Path), table = tgt1Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)

    val actions: Seq[DataFrameOneToOneActionImpl] = Seq(
      CopyAction("a", srcDO.id, tgt1DO.id)
    )
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(df1, Seq())

    // fail for not existing expected partition abc
    val dag1 = ActionDAGRun(actions, partitionValues = Seq(PartitionValues(Map("lastname" -> "abc"))))
    dag1.prepare(contextPrep)
    val e = intercept[TaskFailedException](dag1.exec(contextExec))
    assert(e.cause.isInstanceOf[AssertionError])

    // doesnt fail for not existing unexpected partition xyz
    val dag2 = ActionDAGRun(actions, partitionValues = Seq(PartitionValues(Map("lastname" -> "xyz"))))
    dag2.prepare(contextPrep)
    dag2.exec(contextExec)
  }

  test("action dag with 2 actions in sequence and executionMode=PartitionDiffMode alternativeOutputId") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), table = tgt2Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    // prepare data in srcDO and tgt1DO. Because of alternativeOutputId in action~a it should be processed again.
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    val expectedPartitions = Seq(PartitionValues(Map("lastname"->"doe")))
    srcDO.writeSparkDataFrame(df1, expectedPartitions)
    tgt1DO.writeSparkDataFrame(df1, expectedPartitions)
    val actions: Seq[DataFrameOneToOneActionImpl] = Seq(
      CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(alternativeOutputId = Some(tgt2DO.id))))
      , CopyAction("b", tgt1DO.id, tgt2DO.id, deleteDataAfterRead = true)
    )
    val dag: ActionDAGRun = ActionDAGRun(actions)

    // first dag run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    assert(tgt1DO.getSparkDataFrame().count() == 0) // this should be empty because of tgt2DO.deleteDataAfterRead = true
    assert(tgt1DO.listPartitions.isEmpty)
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2 == Seq(5))
    assert(tgt2DO.listPartitions == expectedPartitions)

    // second dag run - skip action execution because there are no new partitions to process
    dag.prepare(contextPrep)
    val subFeeds = dag.init(contextInit)
    subFeeds.forall(_.isSkipped)
  }

  test("action dag with 2 actions in sequence and executionMode=SparkStreamingOnceMode") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int")
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val tgt1DO = JsonFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append, jsonOptions = Some(Map("multiLine" -> "false")))
    instanceRegistry.register(tgt1DO)
    val tgt2DO = JsonFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append, jsonOptions = Some(Map("multiLine" -> "false")))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(df1, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointLocation = tempDir.resolve("stateA").toUri.toString)))
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointLocation = tempDir.resolve("stateB").toUri.toString)))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    // second dag run - no data to process
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 5)

    // third dag run - new data to process
    val df2 = Seq(("doe","john 2",10)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(df2, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    val outputSubFeeds = dag.exec(contextExec)

    // check
    val r3 = tgt2DO.getSparkDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r3.size == 2)

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written")==1)
    assert(outputSubFeeds.find(_.dataObjectId == action2.outputId).get.metrics.get("records_written")==1)
  }

  test("action dag with 2 actions in sequence, first is executionMode=SparkStreamingOnceMode, second is normal") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int")
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val tgt1DO = JsonFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append, jsonOptions = Some(Map("multiLine" -> "false")))
    instanceRegistry.register(tgt1DO)
    val tgt2DO = JsonFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'), saveMode = SDLSaveMode.OverwriteOptimized, jsonOptions = Some(Map("multiLine" -> "false")))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(df1, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointLocation = tempDir.resolve("stateA").toUri.toString)))
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id)
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r1 == Seq(5))

    // second dag run - no data to process
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r2 == Seq(5))

    // third dag run - new data to process
    val df2 = Seq(("doe","john 2",10)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(df2, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r3 = tgt2DO.getSparkDataFrame()
      .select($"rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r3.size == 2)

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written")==2) // without execution mode always the whole table is processed
  }

  test("action dag union 2 streams with executionMode=SparkStreamingOnceMode") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int")
    val src1DO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(src1DO)
    val src2DO = JsonFileDataObject( "src2", tempDir.resolve("src2").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(src2DO)
    val tgt1DO = JsonFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append, jsonOptions = Some(Map("multiLine" -> "false")))
    instanceRegistry.register(tgt1DO)

    // prepare DAG
    val data1src1 = Seq(("doe","john",5))
    val data1src2 = Seq(("einstein","albert",2))
    src1DO.writeSparkDataFrame(data1src1.toDF("lastname", "firstname", "rating"), Seq())
    src2DO.writeSparkDataFrame(data1src2.toDF("lastname", "firstname", "rating"), Seq())

    val action1 = CustomDataFrameAction( "a", Seq(src1DO.id,src2DO.id), Seq(tgt1DO.id)
                                   , executionMode = Some(SparkStreamingMode(checkpointLocation = tempDir.resolve("stateA").toUri.toString))
                                   , transformers = Seq(ScalaClassSparkDfsTransformer(className = classOf[TestStreamingTransformer].getName))
                                   )
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt1DO.getSparkDataFrame().select($"lastname",$"firstname",$"rating".cast("int")).as[(String,String,Int)].collect().toSet
    assert(r1 == (data1src1 ++ data1src2).toSet)

    // second dag run - no data to process
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r2 = tgt1DO.getSparkDataFrame().select($"lastname",$"firstname",$"rating".cast("int")).as[(String,String,Int)].collect().toSet
    assert(r2 == (data1src1 ++ data1src2).toSet)

    // third dag run - new data to process in src 2
    val data2 = Seq(("doe","john 2",10))
    src2DO.writeSparkDataFrame(data2.toDF("lastname", "firstname", "rating"), Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r3 = tgt1DO.getSparkDataFrame().select($"lastname",$"firstname",$"rating".cast("int")).as[(String,String,Int)].collect().toSet
    assert(r3 == (data1src1 ++ data1src2 ++ data2).toSet)

    // check metrics
    val action1MainMetrics = TestUtil.getMetrics(action1.getRuntimeInfo().get, action1.outputIds.head)
    assert(action1MainMetrics("records_written")==1)
  }

  test("action dag with 2 actions in sequence, first is executionMode=DataFrameIncrementalMode, second is normal") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int, tstmp timestamp").asInstanceOf[StructType]
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val tgt1DO = ParquetFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = ParquetFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df1, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(DataFrameIncrementalMode(compareCol = "tstmp")))
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id)
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1,action2))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1 == Seq(5))

    // second dag run - no data to process
    dag.reset
    dag.prepare(contextPrep)
    val subFeeds = dag.init(contextInit)
    subFeeds.forall(_.isSkipped)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2 == Seq(5))

    // third dag run - new data to process
    val df2 = Seq(("doe","john 2",10, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df2, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r3 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r3.size == 2)

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputId)
    assert(action2MainMetrics("records_written")==2) // without execution mode always the whole table is processed
  }

  test("action dag with 2 actions in sequence, first is executionMode=DataFrameIncrementalMode, second with executionCondition=true und executionMode=ProcessAllMode") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int, tstmp timestamp")
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val schema2 = StructType.fromDDL("lastname string, firstname string, address string")
    val src2DO = JsonFileDataObject( "src2", tempDir.resolve("src2").toString.replace('\\', '/'), schema = Some(SparkSchema(schema2)))
    instanceRegistry.register(src2DO)
    val tgt1DO = ParquetFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = ParquetFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df1, Seq())
    val df2 = Seq(("doe","john","waikiki beach")).toDF("lastname", "firstname", "address")
    src2DO.writeSparkDataFrame(df2, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id
      , executionMode = Some(DataFrameIncrementalMode(compareCol = "tstmp")))
    val action2 = CustomDataFrameAction("b", Seq(tgt1DO.id,src2DO.id), Seq(tgt2DO.id)
      , executionCondition = Some(Condition("true")), executionMode = Some(ProcessAllMode()) // process everything, also if predecessor skipped
      , transformers = Seq(SQLDfsTransformer(code = Map(tgt2DO.id.id -> "select * from src2 join tgt1 using (lastname, firstname)"))))
    instanceRegistry.register(Seq(action1, action2))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1,action2))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating", $"address")
      .as[(Int,String)].collect().toSet
    assert(r1 == Set((5,"waikiki beach")))

    // second dag run - no data to process in action a
    // there should be no exception and action b should run with updated data of src2 and existing data of tgt1
    val df3 = Seq(("doe","john","honolulu")).toDF("lastname", "firstname", "address")
    src2DO.writeSparkDataFrame(df3, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating", $"address")
      .as[(Int,String)].collect().toSet
    assert(r2 == Set((5,"honolulu")))

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputIds.head)
    assert(action2MainMetrics("records_written")==1)
  }

  test("action dag with 2 actions in sequence, first is executionMode=DataFrameIncrementalMode, second with executionCondition=true") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int, tstmp timestamp")
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val schema2 = StructType.fromDDL("lastname string, firstname string, address string")
    val src2DO = JsonFileDataObject( "src2", tempDir.resolve("src2").toString.replace('\\', '/'), schema = Some(SparkSchema(schema2)))
    instanceRegistry.register(src2DO)
    val tgt1DO = ParquetFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = ParquetFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    val df1 = Seq(("doe","john",5, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df1, Seq())
    val df2 = Seq(("doe","john","waikiki beach")).toDF("lastname", "firstname", "address")
    src2DO.writeSparkDataFrame(df2, Seq())

    val sqlTransformer = SQLDfsTransformer(code = Map(tgt2DO.id.id -> "select * from src2 join tgt1 using (lastname, firstname)"))
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(DataFrameIncrementalMode(compareCol = "tstmp")))
    val action2 = CustomDataFrameAction("b", Seq(tgt1DO.id,src2DO.id), Seq(tgt2DO.id), transformers = Seq(sqlTransformer), executionCondition = Some(Condition("true")))
    instanceRegistry.register(Seq(action1, action2))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1,action2))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating", $"address")
      .as[(Int,String)].collect().toSet
    assert(r1 == Set((5,"waikiki beach")))

    // second dag run - no data to process in action a
    // there should be no exception and action b should run with updated data of src2 and all data of tgt1 (as skipped SubFeed is reset)
    val df3 = Seq(("doe","john","honolulu")).toDF("lastname", "firstname", "address")
    src2DO.writeSparkDataFrame(df3, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    assert(tgt2DO.getSparkDataFrame().count() == 1)

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputIds.head)
    assert(action2MainMetrics("records_written")==1)
  }

  test("action dag with 2 actions in sequence, first is executionMode=DataFrameIncrementalMode, second with executionCondition=true and ProcessAll mode") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int, tstmp timestamp").asInstanceOf[StructType]
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val schema2 = StructType.fromDDL("lastname string, firstname string, address string").asInstanceOf[StructType]
    val src2DO = JsonFileDataObject( "src2", tempDir.resolve("src2").toString.replace('\\', '/'), schema = Some(SparkSchema(schema2)))
    instanceRegistry.register(src2DO)
    val tgt1DO = ParquetFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = ParquetFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val refTimestamp1 = LocalDateTime.now()
    val df1 = Seq(("doe","john",5, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df1, Seq())
    val df2 = Seq(("doe","john","waikiki beach")).toDF("lastname", "firstname", "address")
    src2DO.writeSparkDataFrame(df2, Seq())

    val sqlTransformer = SQLDfsTransformer(code = Map(tgt2DO.id.id -> "select * from src2 join tgt1 using (lastname, firstname)"))
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(DataFrameIncrementalMode(compareCol = "tstmp")))
    val action2 = CustomDataFrameAction("b", Seq(tgt1DO.id,src2DO.id), Seq(tgt2DO.id), transformers = Seq(sqlTransformer), executionCondition = Some(Condition("true")), executionMode = Some(ProcessAllMode()))
    instanceRegistry.register(Seq(action1, action2))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1,action2))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"rating", $"address")
      .as[(Int,String)].collect().toSet
    assert(r1 == Set((5,"waikiki beach")))

    // second dag run - no data to process in action a
    // there should be no exception and action b should run with updated data of src2 and existing data of tgt1 due to ProcessAllMode
    val df3 = Seq(("doe","john","honolulu")).toDF("lastname", "firstname", "address")
    src2DO.writeSparkDataFrame(df3, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating", $"address")
      .as[(Int,String)].collect().toSet
    assert(r2 == Set((5,"honolulu")))

    // check metrics
    val action2MainMetrics = TestUtil.getMetrics(action2.getRuntimeInfo().get, action2.outputIds.head)
    assert(action2MainMetrics("records_written")==1)
  }

  test("action dag failes because of metricsFailCondition") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int, tstmp timestamp").asInstanceOf[StructType]
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)))
    instanceRegistry.register(srcDO)
    val tgt1DO = ParquetFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df1, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metricsFailCondition = Some(s"dataObjectId = '${tgt1DO.id.id}' and value > 0"))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1))

    // first dag run, first file processed
    dag.prepare(contextPrep)
    dag.init(contextInit)
    val ex = intercept[TaskFailedException](dag.exec(contextExec))
    assert(ex.cause.isInstanceOf[MetricsCheckFailed])
  }

  test("action dag failes because of executionMode=PartitionDiffMode failCondition") {
    // setup DataObjects
    val feed = "actionpipeline"
    val tempDir = Files.createTempDirectory(feed)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int, tstmp timestamp").asInstanceOf[StructType]
    val srcDO = JsonFileDataObject( "src1", tempDir.resolve("src1").toString.replace('\\', '/'), schema = Some(SparkSchema(schema)), partitions = Seq("lastname"))
    instanceRegistry.register(srcDO)
    val tgt1DO = ParquetFileDataObject( "tgt1", tempDir.resolve("tgt1").toString.replace('\\', '/'), partitions = Seq("lastname"), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = ParquetFileDataObject( "tgt2", tempDir.resolve("tgt2").toString.replace('\\', '/'), partitions = Seq("lastname"), saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5, Timestamp.from(Instant.now))).toDF("lastname", "firstname", "rating", "tstmp")
    srcDO.writeSparkDataFrame(df1, Seq())

    // dag1 run fails in init-phase because action1 fails and there is no other action to execute
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode=Some(PartitionDiffMode(failConditions = Seq(Condition(expression = "year(runStartTime) > 2000", Some("testing"))))))
    val dag1: ActionDAGRun = ActionDAGRun(Seq(action1))
    dag1.prepare(contextPrep)
    dag1.init(contextInit)
    val ex1 = intercept[TaskFailedException](dag1.exec(contextExec))
    assert(ex1.cause.isInstanceOf[ExecutionModeFailedException])
    assert(ex1.cause.getMessage.contains("testing"))

    // dag2 run fails in exec-phase because action1 fails but there is another action to execute
    val action2 = CopyAction("b", srcDO.id, tgt2DO.id)
    val dag2: ActionDAGRun = ActionDAGRun(Seq(action1,action2))
    dag2.reset
    dag2.prepare(contextPrep)
    dag2.init(contextInit)
    val ex2 = intercept[TaskFailedException](dag2.exec(contextExec))
    assert(ex2.cause.isInstanceOf[ExecutionModeFailedException])
    assert(ex2.cause.getMessage.contains("testing"))

    // check tgt2, it should be written by dag2
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSet
    assert(r2 == Set(5))

  }

  test("dont throw exception if no output metrics on empty DataFrame") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1DO = UnpartitionedTestDataObject( "tgt1")
    instanceRegistry.register(tgt1DO)

    // prepare DAG
    val df1 = Seq[(String,String,Int)]().toDF("lastname", "firstname", "rating")
    val expectedPartitions = Seq(PartitionValues(Map("lastname"->"doe")))
    srcDO.writeSparkDataFrame(df1, expectedPartitions)
    val actions: Seq[DataFrameOneToOneActionImpl] = Seq(
      CopyAction("a", srcDO.id, tgt1DO.id)
    )
    val dag: ActionDAGRun = ActionDAGRun(actions)

    // first dag run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)
  }

  test("action dag with 2 actions in sequence and executionMode=PartitionDiffMode, second action can not handle partitions") {
    // setup DataObjects
    val feed = "actionpipeline"
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname","firstname")))
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2DO = UnpartitionedTestDataObject( "tgt2")
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    val expectedPartitions = Seq(PartitionValues(Map("lastname"->"doe")))
    srcDO.writeSparkDataFrame(df1, expectedPartitions)
    val actions: Seq[DataFrameOneToOneActionImpl] = Seq(
      CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode()))
      , CopyAction("b", tgt1DO.id, tgt2DO.id)
    )
    val dag: ActionDAGRun = ActionDAGRun(actions)

    // first dag run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // second dag run - skip action execution because there are no new partitions to process
    dag.prepare(contextPrep)
    dag.init(contextInit)
    val results = dag.exec(contextExec)
    assert(results.head.isSkipped)
  }

  test("action dag with 2 actions in sequence and output from second action as recursive input for first action") {
    // setup DataObjects
    val srcDO1 = MockDataObject("src1")
    instanceRegistry.register(srcDO1)
    val recDO = MockDataObject("rec1", schemaMin = Some(SparkSchema(StructType(Seq(StructField("lastname",StringType), StructField("role",StringType))))))
    recDO.dropTable
    instanceRegistry.register(recDO)
    val tgt1DO = MockDataObject("tgt1")
    instanceRegistry.register(tgt1DO)

    // initialize global config (used for validating allowAsRecursiveInput)
    Environment._globalConfig = GlobalConfig()
    // not allowed if not present in allowAsRecursiveInput
    intercept[AssertionError](CustomDataFrameAction("a", Seq(srcDO1.id), Seq(tgt1DO.id), recursiveInputIds = Seq(recDO.id)))
    // now add it to the list
    Environment._globalConfig = Environment._globalConfig.copy(allowAsRecursiveInput = Seq(recDO.id))

    // prepare DAG
    val df1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(df1)
    recDO.writeSparkDataFrame(df1.select($"lastname", lit("tester").as("role")))
    val actions = Seq(
      CustomDataFrameAction("a", Seq(srcDO1.id), Seq(tgt1DO.id), recursiveInputIds = Seq(recDO.id),
        transformers = Seq(SQLDfsTransformer(code = Map(tgt1DO.id.id -> "select * from src1 left join rec1 using (lastname)")))
      ),
      CustomDataFrameAction("b", Seq(tgt1DO.id), Seq(recDO.id),
        transformers = Seq(SQLDfsTransformer(code = Map(recDO.id.id -> "select * from tgt1")))
      )
    )
    instanceRegistry.register(actions)

    val dag = ActionDAGRun(actions)

    // first dag run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    assert(recDO.getSparkDataFrame().count() > 0)

    // and cleanup special config again
    Environment._globalConfig = Environment._globalConfig.copy(allowAsRecursiveInput = Seq())
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

class TestStreamingTransformer extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    val dfTgt1 = dfs("src1").unionByName(dfs("src2"))
    Map("tgt1" -> dfTgt1)
  }
}

class TestDfsUnionOfThree extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._
    val dfTgt = dfs("src1").select($"lastname", $"firstname", $"rating").withColumn("origin", lit(1))
    .union(dfs("src2").select($"lastname", $"firstname", $"rating").withColumn("origin", lit(2)))
    .union(dfs("src3").select($"lastname", $"firstname", $"rating").withColumn("origin", lit(3)))
    Map("tgt1" -> dfTgt)
  }
}
