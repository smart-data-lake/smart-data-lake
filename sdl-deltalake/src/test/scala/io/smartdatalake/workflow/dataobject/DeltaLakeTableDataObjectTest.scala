/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{ColumnStatsType, SDLSaveMode, SaveModeMergeOptions, TableStatsType}
import io.smartdatalake.testutils.custom.TestCustomDfCreator
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.DeltaLakeTestUtils.deltaDb
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, ProcessingLogicException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import java.nio.file.Files

class DeltaLakeTableDataObjectTest extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  // set additional spark options for delta lake
  protected implicit val session : SparkSession = DeltaLakeTestUtils.session
  import session.implicits._

  val tempDir = Files.createTempDirectory("tempHadoopDO")
  val tempPath: String = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = context.copy(phase = ExecutionPhase.Exec)
  val contextInit: ActionPipelineContext = context.copy(phase = ExecutionPhase.Init)

  override def beforeAll(): Unit = {
    val wareousePath = new Path("spark-warehouse/delta.db")
    implicit val fs: FileSystem = HdfsUtil.getHadoopFsFromSpark(wareousePath)(session)
    HdfsUtil.deletePath(wareousePath, false)
  }

  before {
    instanceRegistry.clear()
  }

  test("CustomDf2DeltaTable") {

    // setup DataObjects
    val feed = "customDf2Delta"
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(db = Some(deltaDb), name = "custom_df_copy", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(contextExec)

    val expected = sourceDO.getSparkDataFrame()
    val actual = targetDO.getSparkDataFrame()
    val resultat = expected.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("CustomDf2DeltaTable",Seq())(actual)(expected)
    assert(resultat)

    // check statistics
    assert(targetDO.getStats().apply(TableStatsType.NumRows.toString) == 2)
    val colStats = targetDO.getColumnStats()
    assert(colStats.apply("num").get(ColumnStatsType.Max.toString).contains(1))
    assert(colStats.apply("text").get(ColumnStatsType.Max.toString).contains("Foo!"))
  }

  test("CustomDf2DeltaTable_partitioned") {

    // setup DataObjects
    val feed = "customDf2Delta_partitioned"
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(db = Some(deltaDb), name = "custom_df_copy_partitioned", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", partitions=Seq("num"), path=Some(targetTablePath), table=targetTable)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(contextExec)

    val expected = sourceDO.getSparkDataFrame()
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("CustomDf2DeltaTable_partitioned",Seq())(actual)(expected)
    assert(resultat)

    // move partition
    assert(targetDO.listPartitions.map(_.elements).toSet == Set(Map("num" -> "0"), Map("num" -> "1")))
    targetDO.movePartitions(Seq((PartitionValues(Map("num" -> "0")), PartitionValues(Map("num" -> "2")))))
    assert(targetDO.listPartitions.map(_.elements).toSet == Set(Map("num" -> "1"), Map("num" -> "2")))
  }

  test("SaveMode overwrite with different schema") {
    val targetTable = Table(db = Some(deltaDb), name = "test_overwrite", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Overwrite, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: overwrite all with different schema
    val df2 = Seq(("ext","doe","john",10,"test"),("ext","smith","peter",1,"test"))
      .toDF("type", "lastname", "firstname", "rating2", "test")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val resultat2: Boolean = df2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode overwrite",Seq())(actual2)(df2)
    assert(resultat2)
  }

  test("SaveMode overwrite with different schema on managed table") {
    val targetTable = Table(db = Some(deltaDb), name = "test_overwrite_managed", query = None)
    val targetDO = DeltaLakeTableDataObject(id="target", table=targetTable, saveMode = SDLSaveMode.Overwrite, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: overwrite all with different schema
    val df2 = Seq(("ext","doe","john",10,"test"),("ext","smith","peter",1,"test"))
      .toDF("type", "lastname", "firstname", "rating2", "test")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val resultat2: Boolean = df2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode overwrite",Seq())(actual2)(df2)
    assert(resultat2)
  }

  test("SaveMode append with different schema") {
    val targetTable = Table(db = Some(deltaDb), name = "test_append", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Append, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    // 2nd load: append all with different schema
    val df2 = Seq(("ext","doe","john",10,"test"),("ext","smith","peter",1,"test"))
      .toDF("type", "lastname", "firstname", "rating2", "test")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame().filter($"lastname" === "doe")
    val result2 = actual2.count() == 2 && (df1.columns ++ df2.columns).toSet == actual2.columns.toSet
    if (!result2) TestUtil.printFailedTestResult("SaveMode append",Seq())(actual2)(df2)
    assert(result2)
  }

  test("SaveMode append with different schema on managed table") {
    val targetTable = Table(db = Some(deltaDb), name = "test_append_managed", query = None)
    val targetDO = DeltaLakeTableDataObject(id="target", table=targetTable, saveMode = SDLSaveMode.Append, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    // 2nd load: append all with different schema
    val df2 = Seq(("ext","doe","john",10,"test"),("ext","smith","peter",1,"test"))
      .toDF("type", "lastname", "firstname", "rating2", "test")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame().filter($"lastname" === "doe")
    val result2 = actual2.count() == 2 && (df1.columns ++ df2.columns).toSet == actual2.columns.toSet
    if (!result2) TestUtil.printFailedTestResult("SaveMode append",Seq())(actual2)(df2)
    assert(result2)
  }

  test("SaveMode overwrite and delete partition") {
    val targetTable = Table(db = Some(deltaDb), name = "test_overwrite", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, partitions = Seq("type"), saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "static"))
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    assert(targetDO.listPartitions.toSet == Set(PartitionValues(Map("type"->"ext")), PartitionValues(Map("type"->"int"))))

    // 2nd load: overwrite partition type=ext
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    intercept[ProcessingLogicException](targetDO.writeSparkDataFrame(df2)) // not allowed to overwrite all partitions
    targetDO.writeSparkDataFrame(df2, partitionValues = Seq(PartitionValues(Map("type"->"ext"))))
    val expected2 = df2.union(df1.where($"type"=!="ext"))
    val actual2 = targetDO.getSparkDataFrame()
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode overwrite and delete partition",Seq())(actual2)(expected2)
    assert(resultat2)

    // delete partition
    targetDO.deletePartitions(Seq(PartitionValues(Map("type"->"int"))))
    assert(targetDO.listPartitions == Seq(PartitionValues(Map("type"->"ext"))))
  }

  test("SaveMode overwrite partitions dynamically") {
    val targetTable = Table(db = Some(deltaDb), name = "test_overwrite", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, partitions = Seq("type")
      , saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "dynamic"))
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    assert(targetDO.listPartitions.toSet == Set(PartitionValues(Map("type"->"ext")), PartitionValues(Map("type"->"int"))))

    // 2nd load: dynamically overwrite partition type=ext
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df2) // allowed overwriting partitions because of partitionOverwriteMode=dynamic
    val expected2 = df2.union(df1.where($"type"=!="ext"))
    val actual2 = targetDO.getSparkDataFrame()
    val resul2 = expected2.isEqual(actual2)
    if (!resul2) TestUtil.printFailedTestResult("SaveMode overwrite partitions dynamically",Seq())(actual2)(expected2)
    assert(resul2)
  }

  test("SaveMode overwrite and delete partition on managed table") {
    val targetTable = Table(db = Some(deltaDb), name = "test_overwrite_managed", query = None)
    val targetDO = DeltaLakeTableDataObject(id="target", table=targetTable, partitions = Seq("type"), saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "static"))
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    assert(targetDO.listPartitions.toSet == Set(PartitionValues(Map("type"->"ext")), PartitionValues(Map("type"->"int"))))

    // 2nd load: overwrite partition type=ext
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    intercept[ProcessingLogicException](targetDO.writeSparkDataFrame(df2)) // not allowed to overwrite all partitions
    targetDO.writeSparkDataFrame(df2, partitionValues = Seq(PartitionValues(Map("type"->"ext"))))
    val expected2 = df2.union(df1.where($"type"=!="ext"))
    val actual2 = targetDO.getSparkDataFrame()
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode overwrite and delete partition",Seq())(actual2)(expected2)
    assert(resultat2)

    // delete partition
    targetDO.deletePartitions(Seq(PartitionValues(Map("type"->"int"))))
    assert(targetDO.listPartitions == Seq(PartitionValues(Map("type"->"ext"))))
  }

  test("SaveMode append") {
    val targetTable = Table(db = Some(deltaDb), name = "test_append", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Append)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: append data
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = df2.union(df1)
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode append",Seq())(actual2)(expected2)
    assert(resultat2)
  }

  test("SaveMode append on managed table") {
    val targetTable = Table(db = Some(deltaDb), name = "test_append_managed", query = None)
    val targetDO = DeltaLakeTableDataObject(id="target", table=targetTable, saveMode = SDLSaveMode.Append)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: append data
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = df2.union(df1)
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode append",Seq())(actual2)(expected2)
    assert(resultat2)
  }

  test("SaveMode merge") {
    val targetTable = Table(db = Some(deltaDb), name = "test_merge", query = None, primaryKey = Some(Seq("type","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = Seq(("ext","doe","john",10),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode merge",Seq())(actual2)(expected2)
    assert(resultat2)
  }

  test("SaveMode merge with schema evolution") {
    val targetTable = Table(db = Some(deltaDb), name = "test_merge", query = None, primaryKey = Some(Seq("type","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge, options = Map("mergeSchema" -> "true"), allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key with different schema
    // - column 'rating' deleted -> existing records will keep column rating untouched (values are preserved and not set to null), new records will get new column rating set to null.
    // - column 'rating2' added -> existing records will get new column rating2 set to null
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating2")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = Seq(("ext","doe","john",Some(5),Some(10)),("ext","smith","peter",Some(3),None),("int","emma","brown",None,Some(7)))
      .toDF("type", "lastname", "firstname", "rating", "rating2")
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("SaveMode merge",Seq())(actual2)(expected2)
    assert(resultat2)
  }

  // Note that this is not possible with DeltaLake <= 3.2.0, as schema evolution with mergeStmt.insertExpr is not properly supported.
  // Unfortunately this is needed by HistorizeAction with merge.
  // We test for failure to be notified once it is working...
  test("SaveMode merge with updateCols and schema evolution - fails in deltalake <= 3.2.0") {
    val targetTable = Table(db = Some(deltaDb), name = "test_merge", query = None, primaryKey = Some(Seq("type","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge, options = Map("mergeSchema" -> "true"), allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key with different schema
    // - column 'rating' deleted -> existing records will keep column rating untouched (values are preserved and not set to null), new records will get new column rating set to null.
    // - column 'rating2' added -> existing records will get new column rating2 set to null
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating2")
    // this doesnt work for now, see also https://github.com/delta-io/delta/issues/2300
    intercept[AnalysisException](targetDO.writeSparkDataFrame(df2, saveModeOptions = Some(SaveModeMergeOptions(updateColumns = Seq("lastname", "firstname", "rating", "rating2")))))
  }

  test("returns correct metrics") {
    val srcDO = MockDataObject("src1").register
    val l1 = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())

    val targetTable = Table(db = Some(deltaDb), name = "test_metrics", query = None)
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id = "target", path = Some(targetTablePath), table = targetTable, saveMode = SDLSaveMode.Overwrite, allowSchemaEvolution = true)
    instanceRegistry.register(targetDO)
    targetDO.dropTable

    // prepare & start load
    val testAction = CopyAction(id = s"actionA", inputId = srcDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = testAction.exec(Seq(srcSubFeed))(contextExec.copy(currentAction = Some(testAction))).head
    assert(!tgtSubFeed.metrics.flatMap(_.get("records_written")).contains(0), "records_written should be >0 or removed")
    assert(!tgtSubFeed.metrics.flatMap(_.get("bytes_written")).contains(0), "bytes_written should be >0 or removed")
    assert(!tgtSubFeed.metrics.flatMap(_.get("no_data")).contains(true), "no_data should not be true")
    assert(tgtSubFeed.metrics.flatMap(_.get("count")).contains(3))
    assert(tgtSubFeed.metrics.flatMap(_.get("rows_inserted")).contains(3))
  }

  test("normal output mode without cdc activated") {
    // create data object
    val targetTable = Table(db = Some(deltaDb), name = "test_inc", primaryKey = Some(Seq("id")))
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject("deltaDO1", table = targetTable, path = Some(targetTablePath), saveMode = SDLSaveMode.Append)
    targetDO.dropTable

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)

    // test
    val newState1 = targetDO.getState
    targetDO.setState(newState1)

    // check
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4
  }

  test("incremental output mode with inserts") {

    // create data object
    val targetTable = Table(db = Some(deltaDb), name = "test_inc", primaryKey = Some(Seq("id")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject("deltaDO1", table = targetTable, path=Some(targetTablePath), saveMode = SDLSaveMode.Append)
    targetDO.dropTable
    targetDO.setState(None) // initialize incremental output with empty state

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)
    val newState1 = targetDO.getState

    // test 1
    targetDO.setState(newState1)
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4


    // append test data 2
    val df2 = Seq((5, "B", 5)).toDF("id", "p", "value")
    targetDO.writeSparkDataFrame(df2)
    val newState2 = targetDO.getState

    // test 2
    targetDO.setState(newState2)
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 1

    // append test data 3
    val df3 = Seq((6, "T", 5), (7, "R", 7), (8, "T", 2)).toDF("id", "p", "value")
    targetDO.writeSparkDataFrame(df3)
    val newState3 = targetDO.getState

    // test 3
    targetDO.setState(newState3)
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 3

    assert(newState1.get < newState2.get)
    assert(newState2.get < newState3.get)

    targetDO.setState(None) // to get the full dataframe
    targetDO.getSparkDataFrame()(contextInit).count() shouldEqual 8
  }

  test("incremental output mode without primary keys") {

    // create data object
    val targetTable = Table(db = Some(deltaDb), name = "test_inc")
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject("deltaDO1", table = targetTable, path = Some(targetTablePath), saveMode = SDLSaveMode.Append)
    targetDO.dropTable
    targetDO.setState(None) // initialize incremental output with empty state

    // write test data
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)
    val newState1 = targetDO.getState
    targetDO.setState(newState1)
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4

    val df2 = Seq((5, "B", 5)).toDF("id", "p", "value")
    targetDO.writeSparkDataFrame(df2)
    val newState2 = targetDO.getState

    // test
    val thrown = intercept[IllegalArgumentException] {
      targetDO.setState(newState2)
      targetDO.getSparkDataFrame()(contextExec).count()
    }

    // check
    assert(thrown.isInstanceOf[IllegalArgumentException])
    assert(thrown.getMessage.contains("PrimaryKey for table"))

  }

  test("incremental output mode with updates and inserts") {

    // create data object
    val targetTable = Table(db = Some(deltaDb), name = "test_inc", primaryKey = Some(Seq("id")))
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject("deltaDO1", table = targetTable, path = Some(targetTablePath))
    targetDO.dropTable
    targetDO.setState(None) // initialize incremental output with empty state

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)
    val newState1 = targetDO.getState
    targetDO.setState(newState1)
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4

    // do updates and inserts
    session.sql(s"INSERT INTO $deltaDb.test_inc VALUES (5, 'T', 7) ")
    val newState2 = targetDO.getState
    session.sql(s"INSERT INTO $deltaDb.test_inc VALUES (6, 'U', 3) ")
    session.sql(s"UPDATE $deltaDb.test_inc SET p = 'Z', value = 8 WHERE id = 1")
    session.sql(s"UPDATE $deltaDb.test_inc SET p = 'W', value = 1 WHERE id = 1")

    // test
    val resultDf = Seq((5, "T", 7), (6, "U", 3), (1, "W", 1)).toDF("id", "p", "value")

    targetDO.setState(newState2)
    val testDf = targetDO.getSparkDataFrame()(contextExec)

    testDf.count() shouldEqual 3 // 2x new insert + 1x the latest update

    testDf.collect() sameElements resultDf.collect()

  }



}
