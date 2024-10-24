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
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.custom.TestCustomDfCreator
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.action.{CopyAction, NoDataToProcessWarning}
import io.smartdatalake.workflow.connection.{HadoopFileConnection, IcebergTableConnection}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

class IcebergTableDataObjectTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  protected implicit val session : SparkSession = IcebergTestUtils.session
  import session.implicits._

  val tempDir = Files.createTempDirectory("tempHadoopDO")
  val tempPath: String = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = context.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("Write data") {

    // setup DataObjects
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "custom_df_copy", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)
    targetDO.prepare

    // prepare & start load
    val testAction = CopyAction(id = s"load", inputId = sourceDO.id, outputId = targetDO.id)
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

  test("Write data partitioned") {

    // setup DataObjects
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "custom_df_copy_partitioned", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", partitions=Seq("num"), path=Some(targetTablePath), table=targetTable)
    targetDO.dropTable
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val testAction = CopyAction(id = s"load", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(contextExec)

    val expected = sourceDO.getSparkDataFrame()
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("CustomDf2DeltaTable_partitioned",Seq())(actual)(expected)
    assert(resultat)
    assert(targetDO.listPartitions.map(_.elements).toSet == Set(Map("num" -> "0"), Map("num" -> "1")))
  }

  test("SaveMode overwrite with different schema") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_overwrite", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Overwrite, allowSchemaEvolution = true)
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
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_append", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Append, allowSchemaEvolution = true)
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
    targetDO.initSparkDataFrame(df2, Seq()) // for applying schema evolution
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame().filter($"lastname" === "doe")
    val result2 = actual2.count() == 2 && (df1.columns ++ df2.columns).toSet == actual2.columns.toSet
    if (!result2) TestUtil.printFailedTestResult("SaveMode append with different schema",Seq())(actual2)(df2)
    assert(result2)
  }

  test("SaveMode overwrite and delete partition") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_overwrite", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, partitions = Seq("type")
      , saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "static")
    )
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

    // 2nd load: overwrite partition type=ext
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    intercept[ProcessingLogicException](targetDO.writeSparkDataFrame(df2)) // not allowed to overwrite all partitions
    targetDO.writeSparkDataFrame(df2, partitionValues = Seq(PartitionValues(Map("type"->"ext"))))
    val expected2 = df2.union(df1.where($"type"=!="ext"))
    val actual2 = targetDO.getSparkDataFrame()
    val resul2 = expected2.isEqual(actual2)
    if (!resul2) TestUtil.printFailedTestResult("SaveMode overwrite and delete partition",Seq())(actual2)(expected2)
    assert(resul2)

    // delete partition
    targetDO.deletePartitions(Seq(PartitionValues(Map("type"->"int"))))
    assert(targetDO.listPartitions == Seq(PartitionValues(Map("type"->"ext"))))
  }

  test("SaveMode overwrite partitions dynamically") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_overwrite_dynamic", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, partitions = Seq("type")
      , saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "dynamic")
    )
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

  test("SaveMode append") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_append", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Append)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

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


  test("throw NoDataToProcessWarning if no new snapshot created (no data)") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_nodata", query = None)
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    // Iceberg doesnt create a new snapshot if no data is written with dynamic partition mode
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, partitions = Seq("type"))
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    // 2nd load: no data -> NoDataToProcessWarning
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    Environment._enableSparkPlanNoDataCheck = Some(false) // disable triggering SparkPlanNoDataWarning, as this test is about another case
    intercept[NoDataToProcessWarning](targetDO.writeSparkDataFrame(df2.where(lit(false))))
    Environment._enableSparkPlanNoDataCheck = Some(true)

    // 3nd load: write data
    targetDO.writeSparkDataFrame(df2)
  }


  test("SaveMode merge") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_merge", query = None, primaryKey = Some(Seq("type","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    // 2nd load: merge data by primary key
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = Seq(("ext","doe","john",10),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    val result2 = expected2.isEqual(actual2)
    if (!result2) TestUtil.printFailedTestResult("SaveMode merge",Seq())(actual2)(expected2)
    assert(result2)
  }


  test("SaveMode merge with updateCols") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_merge", query = None, primaryKey = Some(Seq("type","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    // 2nd load: merge data by primary key
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df2, saveModeOptions = Some(SaveModeMergeOptions(updateColumns = Seq("rating"))))
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = Seq(("ext","doe","john",10),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    val result2 = expected2.isEqual(actual2)
    if (!result2) TestUtil.printFailedTestResult("SaveMode merge",Seq())(actual2)(expected2)
    assert(result2)
  }

  // Note that this is not possible with DeltaLake 1.x, as schema evolution with mergeStmt is not properly supported.
  // We test for failure to be notified once it is working...
  // Once this works again, also enable 3rd load in IcebergHistorizeWithMergeActionTest and IcebergDeduplicateWithMergeActionTest test cases again
  test("SaveMode merge with schema evolution") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_merge", query = None, primaryKey = Some(Seq("tpe","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("tpe", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val result = df1.isEqual(actual)
    if (!result) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(result)

    // 2nd load: merge data by primary key with different schema
    // - column 'rating' deleted -> existing records will keep column rating untouched (values are preserved and not set to null), new records will get new column rating set to null.
    // - column 'rating2' added -> existing records will get new column rating2 set to null
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("tpe", "lastname", "firstname", "rating2")
    targetDO.initSparkDataFrame(df2, Seq())
    targetDO.writeSparkDataFrame(df2)
    val actual2 = targetDO.getSparkDataFrame()
    val expected2 = Seq(("ext","doe","john",Some(5),Some(10)),("ext","smith","peter",Some(3),None),("int","emma","brown",None,Some(7)))
      .toDF("tpe", "lastname", "firstname", "rating", "rating2")
    val result2 = expected2.isEqual(actual2)
    if (!result2) TestUtil.printFailedTestResult("SaveMode merge",Seq())(actual2)(expected2)
    assert(result2)
  }

  // Note that this is not possible with DeltaLake 1.x, as schema evolution with mergeStmt.insertExpr is not properly supported.
  // We test for failure to be notified once it is working...
  // Once this works again, also enable 3rd load in IcebergHistorizeWithMergeActionTest and IcebergDeduplicateWithMergeActionTest test cases again
  test("SaveMode merge with updateCols and schema evolution - fails in deltalake 1.x") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_merge", query = None, primaryKey = Some(Seq("tpe","lastname","firstname")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Merge, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3))
      .toDF("tpe", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key with different schema
    // - column 'rating' deleted -> existing records will keep column rating untouched (values are preserved and not set to null), new records will get new column rating set to null.
    // - column 'rating2' added -> existing records will get new column rating2 set to null
    val df2 = Seq(("ext","doe","john",10),("int","emma","brown",7))
      .toDF("tpe", "lastname", "firstname", "rating2")
    intercept[AnalysisException](targetDO.writeSparkDataFrame(df2, saveModeOptions = Some(SaveModeMergeOptions(updateColumns = Seq("lastname", "firstname", "rating", "rating2")))))
  }

  test("incremental output mode with inserts") {

    // create data object
    val targetTable = Table(catalog =  Some("iceberg1"), db = Some("default"), name = "test_inc", primaryKey = Some(Seq("id")))
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject("icebergDO1", table = targetTable, path = Some(targetTablePath), saveMode = SDLSaveMode.Append)
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
    assert(targetDO.getSparkDataFrame()(contextExec).count() == 4)


    // append test data 2
    val df2 = Seq((5, "B", 5)).toDF("id", "p", "value")
    targetDO.writeSparkDataFrame(df2)
    val newState2 = targetDO.getState

    // test 2
    targetDO.setState(newState2)
    assert(targetDO.getSparkDataFrame()(contextExec).count() == 1)

    // append test data 3
    val df3 = Seq((6, "T", 5), (7, "R", 7), (8, "T", 2)).toDF("id", "p", "value")
    targetDO.writeSparkDataFrame(df3)
    val newState3 = targetDO.getState

    // test 3
    targetDO.setState(newState3)
    assert(targetDO.getSparkDataFrame()(contextExec).count() == 3)

    targetDO.setState(None) // to get the full dataframe
    assert(targetDO.getSparkDataFrame()(contextExec).count() == 8)
  }

  test("incremental output mode without primary keys") {

    // create data object
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_inc")
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject("icebergDO1", table = targetTable, path = Some(targetTablePath), saveMode = SDLSaveMode.Append)
    targetDO.dropTable
    targetDO.setState(None) // initialize incremental output with empty state

    // write test data
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)
    val newState1 = targetDO.getState

    // test
    val thrown = intercept[IllegalArgumentException] {
      targetDO.setState(newState1)
      targetDO.getSparkDataFrame()(contextExec).count()
    }

    // check
    assert(thrown.isInstanceOf[IllegalArgumentException])
    assert(thrown.getMessage.contains(s"PrimaryKey for table"))

  }

  test("incremental output mode with updates and inserts") {

    // create data object
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_inc", primaryKey = Some(Seq("id")))
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject("icebergDO1", table = targetTable, path = Some(targetTablePath))
    targetDO.dropTable
    targetDO.setState(None) // initialize incremental output with empty state

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)
    val newState1 = targetDO.getState
    targetDO.setState(newState1)
    assert(targetDO.getSparkDataFrame()(contextExec).count() == 4)

    // do updates and inserts
    session.sql(s"INSERT INTO ${targetTable.fullName} VALUES (5, 'T', 7) ")
    val newState2 = targetDO.getState
    session.sql(s"INSERT INTO ${targetTable.fullName} VALUES (6, 'U', 3) ")
    session.sql(s"UPDATE ${targetTable.fullName} SET p = 'Z', value = 8 WHERE id = 1")
    session.sql(s"UPDATE ${targetTable.fullName} SET p = 'W', value = 1 WHERE id = 1")

    // test
    val resultDf = Seq((5, "T", 7), (6, "U", 3), (1, "W", 1)).toDF("id", "p", "value")

    targetDO.setState(newState2)
    val testDf = targetDO.getSparkDataFrame()(contextExec)

    assert(testDf.count() == 3) // 2x new insert + 1x the latest update

    testDf.collect() sameElements resultDf.collect()

  }


  // TODO: addFilesParallelism > 1 results in Iceberg NotSerializableException, see https://github.com/apache/iceberg/issues/11147
  test("Create from parquet files") {
    // Define Iceberg Table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_to_iceberg", query = None)
    val icebergConnection = IcebergTableConnection(id = "iceberg", db = "default", pathPrefix = tempPath, addFilesParallelism = Some(1))
    instanceRegistry.register(icebergConnection)
    val targetPath = icebergTable.name
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, connectionId = Some(icebergConnection.id))

    // Create parquet files
    val parquetConnection = HadoopFileConnection(id = "parquet", pathPrefix = tempPath)
    instanceRegistry.register(parquetConnection)
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, connectionId = Some(parquetConnection.id))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // Initialize Iceberg table
    icebergDO.prepare // does the table conversion

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }
  }

  test("Create from parquet files of legacy hive tables (c000 file ending)") {
    // Define Iceberg Table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_legacy_to_iceberg", query = None)
    val icebergConnection = IcebergTableConnection(id = "iceberg", db = "default", pathPrefix = tempPath, addFilesParallelism = Some(1))
    instanceRegistry.register(icebergConnection)
    val targetPath = icebergTable.name
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, connectionId = Some(icebergConnection.id))

    // Create parquet files
    val parquetConnection = HadoopFileConnection(id = "parquet", pathPrefix = tempPath)
    instanceRegistry.register(parquetConnection)
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, connectionId = Some(parquetConnection.id))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    parquetDO.filesystem.listStatus(parquetDO.hadoopPath)
      .filter(s => s.isFile && s.getPath.getName.endsWith(".snappy.parquet"))
      .map(_.getPath.toString)
      .foreach { p =>
        parquetDO.renameFile(p, p.stripSuffix(".snappy.parquet"))
      }

    // Initialize Iceberg table
    icebergDO.prepare // does the table conversion

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }
  }

  test("Create from parquet files partitioned of legacy hive tables (c000 file ending)") {
    // Define Iceberg Table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_partitioned_legacy_to_iceberg", query = None)
    val icebergConnection = IcebergTableConnection(id = "iceberg", db = "default", pathPrefix = tempPath, addFilesParallelism = Some(1))
    instanceRegistry.register(icebergConnection)
    val targetPath = icebergTable.name
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, partitions = Seq("tpe"), connectionId = Some(icebergConnection.id))

    // Create parquet files
    val parquetConnection = HadoopFileConnection(id = "parquet", pathPrefix = tempPath)
    instanceRegistry.register(parquetConnection)
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, partitions = Seq("tpe"), connectionId = Some(parquetConnection.id))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    RemoteIteratorWrapper(parquetDO.filesystem.listFiles(parquetDO.hadoopPath, true))
      .filter(s => s.isFile && s.getPath.getName.endsWith(".snappy.parquet"))
      .map(_.getPath.toString)
      .foreach { p =>
        parquetDO.renameFile(p, p.stripSuffix(".snappy.parquet"))
      }

    // Initialize Iceberg table
    icebergDO.prepare // does the table conversion

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }
  }

  test("Create from parquet files partitioned") {

    // Define Iceberg table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_to_iceberg_partitioned")
    val targetPath = tempPath + s"/${icebergTable.name}"
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, partitions = Seq("tpe"))

    // Create parquet files
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, partitions = Seq("tpe"))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // Initialize Iceberg table
    icebergDO.prepare

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }
  }

  test("Write data with hadoop catalog to non-default db") {

    // setup DataObjects
    val sourceDO = CustomDfDataObject(id = "source", creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(catalog = Some("iceberg_hadoop"), db = Some("test"), name = "custom_df_copy", query = None)
    val targetDO = IcebergTableDataObject(id = "target", path = None, table = targetTable)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // create hadoop catalog 'test' database
    val warehouseDir = new Path(session.conf.get(s"spark.sql.catalog.${targetTable.catalog.get}.warehouse"))
    val fs = HdfsUtil.getHadoopFsFromSpark(warehouseDir)
    fs.mkdirs(new Path(warehouseDir, targetTable.db.get))

    // prepare DataObject
    targetDO.prepare

    // prepare & start load
    val testAction = CopyAction(id = s"load", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(contextExec)

    val expected = sourceDO.getSparkDataFrame()
    val actual = targetDO.getSparkDataFrame()
    assert(expected.isEqual(actual))
  }

  test("Create from parquet files partitioned on hadoop catalog") {

    // Setup Iceberg table
    val icebergTable = Table(catalog = Some("iceberg_hadoop"), db = Some("default"), name = "parquet_to_iceberg")
    val icebergDO = IcebergTableDataObject(id = "iceberg", table = icebergTable, partitions = Seq("tpe"))

    // Create parquet files
    val parquetDO = ParquetFileDataObject(id = "parquet", path = icebergDO.hadoopPath.toString, partitions = Seq("tpe"))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // initialize Iceberg table
    icebergDO.prepare

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.isEqual(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }
  }


}
