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

import java.nio.file.Files
import java.time.LocalDateTime

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.PartitionDiffMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.action.customlogic.{CustomDfsTransformer, CustomDfsTransformerConfig}
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table, TickTockHiveTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, InitSubFeed, SparkSubFeed}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CustomSparkActionTest extends FunSuite with BeforeAndAfter {
  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear()
  }

  test("spark action with custom transformation class to load multiple sources into multiple targets") {
    // setup DataObjects
    val feed = "multiple_dfs"

    val srcTable1 = Table(Some("default"), "copy_input1")
    val srcDO1 = HiveTableDataObject("src1", Some(tempPath + s"/${srcTable1.fullName}"), table = srcTable1, numInitialHdfsPartitions = 1)
    srcDO1.dropTable
    instanceRegistry.register(srcDO1)

    val srcTable2 = Table(Some("default"), "copy_input2")
    val srcDO2 = HiveTableDataObject("src2", Some(tempPath + s"/${srcTable2.fullName}"), table = srcTable2, numInitialHdfsPartitions = 1)
    srcDO2.dropTable
    instanceRegistry.register(srcDO2)

    val tgtTable1 = Table(Some("default"), "copy_output1", None, Some(Seq("lastname", "firstname")))
    val tgtDO1 = HiveTableDataObject("tgt1", Some(tempPath + s"/${tgtTable1.fullName}"), table = tgtTable1, numInitialHdfsPartitions = 1)
    tgtDO1.dropTable
    instanceRegistry.register(tgtDO1)

    val tgtTable2 = Table(Some("default"), "copy_output2", None, Some(Seq("lastname", "firstname")))
    val tgtDO2 = HiveTableDataObject("tgt2", Some(tempPath + s"/${tgtTable2.fullName}"), table = tgtTable2, numInitialHdfsPartitions = 1)
    tgtDO2.dropTable
    instanceRegistry.register(tgtDO2)

    // prepare & start load
    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val customTransformerConfig = CustomDfsTransformerConfig(
      className = Some("io.smartdatalake.workflow.action.TestDfsTransformerIncrement"),
      options = Map("increment1" -> "1"), runtimeOptions = Map("increment2" -> "runId")
    )

    val action1 = CustomSparkAction("action1", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), transformer = customTransformerConfig)(context1.instanceRegistry)

    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeDataFrame(l1, Seq())
    srcDO2.writeDataFrame(l1, Seq())

    val tgtSubFeeds = action1.exec(Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq())))
    assert(tgtSubFeeds.size == 2)
    assert(tgtSubFeeds.map(_.dataObjectId) == Seq(tgtDO1.id, tgtDO2.id))

    val r1 = tgtDO1.getDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 6) // should be increased by 1 through TestDfTransformer

    // same for the second dataframe
    val r2 = tgtDO2.getDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 6)
  }

  test("spark action with recursive input") {
    // setup DataObjects
    val feed = "recursive_inputs"

    val srcTable1 = Table(Some("default"), "copy_input1")
    val srcDO1 = TickTockHiveTableDataObject("src1", Some(tempPath + s"/${srcTable1.fullName}"), table = srcTable1, numInitialHdfsPartitions = 1)
    srcDO1.dropTable
    instanceRegistry.register(srcDO1)

    val tgtTable1 = Table(Some("default"), "copy_output1", None, Some(Seq("lastname", "firstname")))
    val tgtDO1 = TickTockHiveTableDataObject("tgt1", Some(tempPath + s"/${tgtTable1.fullName}"), table = tgtTable1, numInitialHdfsPartitions = 1)
    tgtDO1.dropTable
    instanceRegistry.register(tgtDO1)

    // prepare & start load
    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val customTransformerConfig = CustomDfsTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestDfsTransformerRecursive"))

    // first action to create output table as it does not exist yet
    val action1 = CustomSparkAction("action1", List(srcDO1.id), List(tgtDO1.id), transformer = customTransformerConfig)(context1.instanceRegistry)
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeDataFrame(l1, Seq())

    val tgtSubFeedsNonRecursive = action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))
    assert(tgtSubFeedsNonRecursive.size == 1)
    assert(tgtSubFeedsNonRecursive.map(_.dataObjectId) == Seq(tgtDO1.id))

    val r1 = session.table(s"${tgtTable1.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 6) // should be increased by 1 through TestDfTransformer

    // second action to test recursive inputs
    val action2 = CustomSparkAction("action1", List(srcDO1.id), List(tgtDO1.id), transformer = customTransformerConfig, recursiveInputIds = List(tgtDO1.id))(context1.instanceRegistry)

    val tgtSubFeedsRecursive = action2.exec(Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "tgt1", Seq())))
    assert(tgtSubFeedsRecursive.size == 1) // still 1 as recursive inputs are handled separately

    val r2 = session.table(s"${tgtTable1.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 11) // Record should be updated a second time with data from tgt1

  }

  test("copy with partition diff execution mode 2 iterations") {

    // setup DataObjects
    val feed = "partitiondiff"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("type","lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare action
    val refTimestamp = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp), SmartDataLakeBuilderConfig())
    val customTransformerConfig = CustomDfsTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestDfsTransformerDummy"))
    val action = CustomSparkAction("a1", Seq(srcDO.id), Seq(tgtDO.id), transformer = customTransformerConfig, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", Seq()) // InitSubFeed needed to test initExecutionMode!

    // prepare & start first load
    val l1 = Seq(("A","doe","john",5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type"->"A")))
    srcDO.writeDataFrame(l1, l1PartitionValues) // prepare testdata
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed)).head

    // check first load
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)

    // prepare & start 2nd load
    val l2 = Seq(("B","pan","peter",11)).toDF("type", "lastname", "firstname", "rating")
    val l2PartitionValues = Seq(PartitionValues(Map("type"->"B")))
    srcDO.writeDataFrame(l2, l2PartitionValues) // prepare testdata
    assert(srcDO.getDataFrame().count == 2) // note: this needs spark.sql.sources.partitionOverwriteMode=dynamic, otherwise the whole table is overwritten
    val tgtSubFeed2 = action.exec(Seq(srcSubFeed)).head

    // check 2nd load
    assert(tgtSubFeed2.dataObjectId == tgtDO.id)
    assert(tgtSubFeed2.partitionValues.toSet == l2PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 2)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet ++ l2PartitionValues.toSet)
  }

  test("copy with partition diff execution mode and mainInput/Output") {

    // setup DataObjects
    val feed = "partitiondiff"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val srcTable2 = Table(Some("default"), "dummy")
    val srcDO2 = HiveTableDataObject( "src2", Some(tempPath+s"/${srcTable2.fullName}"), table = srcTable2, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    srcDO2.dropTable
    instanceRegistry.register(srcDO2)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("type","lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)
    val tgtTable2 = Table(Some("default"), "copy_output", None, Some(Seq("type","lastname","firstname")))
    val tgtDO2 = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgtTable2.fullName}"), table = tgtTable2, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    tgtDO2.dropTable
    instanceRegistry.register(tgtDO2)

    // prepare action
    val refTimestamp = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp), SmartDataLakeBuilderConfig())
    val customTransformerConfig = CustomDfsTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestDfsTransformerDummy"))
    val action = CustomSparkAction("a1", Seq(srcDO.id, srcDO2.id), Seq(tgtDO.id, tgtDO2.id), transformer = customTransformerConfig
      , mainInputId = Some("src1"), mainOutputId = Some("tgt1"), executionMode = Some(PartitionDiffMode()))
    val srcSubFeed1 = InitSubFeed("src1", Seq()) // InitSubFeed needed to test initExecutionMode!
    val srcSubFeed2 = InitSubFeed("src2", Seq())

    // prepare & start first load
    val l1 = Seq(("A","doe","john",5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type"->"A")))
    srcDO.writeDataFrame(l1, l1PartitionValues) // prepare testdata
    srcDO2.writeDataFrame(l1, l1PartitionValues)
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed1, srcSubFeed2)).head

    // check load
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)
  }

  test("copy load with 2 transformations from sql code") {

    // setup DataObjects
    val feed = "copy"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)

    val tgtTable1 = Table(Some("default"), "copy_output_1", None, Some(Seq("lastname","firstname")))
    val tgtDO1 = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable1.fullName}"), Seq("lastname"), analyzeTableAfterWrite=true, table = tgtTable1, numInitialHdfsPartitions = 1)
    tgtDO1.dropTable
    instanceRegistry.register(tgtDO1)

    val tgtTable2 = Table(Some("default"), "copy_output_2", None, Some(Seq("lastname","firstname")))
    val tgtDO2 = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgtTable2.fullName}"), Seq("lastname"), analyzeTableAfterWrite=true, table = tgtTable2, numInitialHdfsPartitions = 1)
    tgtDO2.dropTable
    instanceRegistry.register(tgtDO2)

    // prepare & start load
    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val customTransformerConfig = CustomDfsTransformerConfig(sqlCode = Map(DataObjectId("tgt1")->"select * from copy_input where rating = 5", DataObjectId("tgt2")->"select * from copy_input where rating = 3"))
    val action1 = CustomSparkAction("ca", List(srcDO.id), List(tgtDO1.id,tgtDO2.id), transformer = customTransformerConfig)
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head

    session.table(s"${tgtTable1.fullName}").show

    val r1 = session.table(s"${tgtTable1.fullName}")
      .select($"lastname")
      .as[String].collect().toSeq
    assert(r1.size == 1) // only one record has rating 5 (see where condition)
    assert(r1.head == "jonson")

    val r2 = session.table(s"${tgtTable2.fullName}")
      .select($"lastname")
      .as[String].collect().toSeq
    assert(r2.size == 1) // only one record has rating 5 (see where condition)
    assert(r2.head == "doe")

  }

}


class TestDfsTransformerIncrement extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._
    Map(
      "tgt1" -> dfs("src1").withColumn("rating", $"rating" + options("increment1").toInt)
    , "tgt2" -> dfs("src2").withColumn("rating", $"rating" + options("increment2").toInt)
    )
  }
}

class TestDfsTransformerRecursive extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._
    if(!dfs.contains("tgt1")) {
      // first run without recursive inputs
      Map(
        "tgt1" -> dfs("src1").withColumn("rating", $"rating"+1)
      )
    }
    else {
      // second run with recursive inputs
      Map(
        "tgt1" -> dfs("tgt1").withColumn("rating", $"rating"+5)
      )
    }

  }
}

class TestDfsTransformerDummy extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    // one to one...
    dfs.map{ case (id, df) => (id.replaceFirst("src","tgt"), df) }
  }
}

class TestDfsTransformerFilterDummy extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    // return only the first df sorted by ID
    dfs.toSeq.sortBy(_._1).take(1).map{ case (id, df) => (id.replaceFirst("src","tgt"), df) }.toMap
  }
}
