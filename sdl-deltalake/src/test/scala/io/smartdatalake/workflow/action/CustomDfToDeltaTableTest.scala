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
package io.smartdatalake.workflow.action

import java.nio.file.Files
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.custom.TestCustomDfCreator
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.dataobject.{CustomDfDataObject, DeltaLakeModulePlugin, DeltaLakeTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, ProcessingLogicException, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class CustomDfToDeltaTableTest extends FunSuite with BeforeAndAfter {

  // we need a session with additional properties...
  protected implicit val session : SparkSession = new DeltaLakeModulePlugin().additionalSparkProperties()
    .foldLeft(TestUtil.sparkSessionBuilder(withHive = true)) {
      case (builder, config) => builder.config(config._1, config._2)
    }.getOrCreate()
  import session.implicits._

  val tempDir = Files.createTempDirectory("tempHadoopDO")
  val tempPath: String = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  before { instanceRegistry.clear() }

  test("CustomDf2DeltaTable") {

    // setup DataObjects
    val feed = "customDf2Delta"
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(db = Some("default"), name = "custom_df_copy", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(session, contextExec)

    val expected = sourceDO.getDataFrame()
    val actual = targetDO.getDataFrame()
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(expected)
    assert(resultat)
  }

  test("CustomDf2DeltaTable_partitioned") {

    // setup DataObjects
    val feed = "customDf2Delta_partitioned"
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfCreator].getName)))
    val targetTable = Table(db = Some("default"), name = "custom_df_copy_partitioned", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", partitions=Seq("num"), path=Some(targetTablePath), table=targetTable)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(session, contextExec)

    val expected = sourceDO.getDataFrame()
    val actual = targetDO.getDataFrame()
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(expected)
    assert(resultat)
  }

  test("SaveMode overwrite") {
    val targetTable = Table(db = Some("default"), name = "test_overwrite", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Overwrite)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeDataFrame(df1)
    val actual = targetDO.getDataFrame()
    val resultat: Boolean = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: overwrite all with different schema
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating2")
    targetDO.writeDataFrame(df2)
    val actual2 = targetDO.getDataFrame()
    val resultat2: Boolean = df2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual2)(df2)
    assert(resultat2)
  }

  test("SaveMode overwrite and delete partition") {
    val targetTable = Table(db = Some("default"), name = "test_overwrite", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, partitions = Seq("type"), saveMode = SDLSaveMode.Overwrite)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeDataFrame(df1)
    val actual = targetDO.getDataFrame()
    val resultat: Boolean = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    assert(targetDO.listPartitions.toSet == Set(PartitionValues(Map("type"->"ext")), PartitionValues(Map("type"->"int"))))

    // 2nd load: overwrite partition type=ext
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    intercept[ProcessingLogicException](targetDO.writeDataFrame(df2)) // not allowed to overwrite all partitions
    targetDO.writeDataFrame(df2, partitionValues = Seq(PartitionValues(Map("type"->"ext"))))
    val expected2 = df2.union(df1.where($"type"=!="ext"))
    val actual2 = targetDO.getDataFrame()
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual2)(expected2)
    assert(resultat2)

    // delete partition
    targetDO.deletePartitions(Seq(PartitionValues(Map("type"->"int"))))
    assert(targetDO.listPartitions == Seq(PartitionValues(Map("type"->"ext"))))
  }

  test("SaveMode append") {
    val targetTable = Table(db = Some("default"), name = "test_append", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=Some(targetTablePath), table=targetTable, saveMode = SDLSaveMode.Append)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeDataFrame(df1)
    val actual = targetDO.getDataFrame()
    val resultat = df1.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: append data
    val df2 = Seq(("ext","doe","john",10),("ext","smith","peter",1))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeDataFrame(df2)
    val actual2 = targetDO.getDataFrame()
    val expected2 = df2.union(df1)
    val resultat2: Boolean = expected2.isEqual(actual2)
    if (!resultat2) TestUtil.printFailedTestResult("Df2HiveTable",Seq())(actual2)(expected2)
    assert(resultat2)
  }
}
