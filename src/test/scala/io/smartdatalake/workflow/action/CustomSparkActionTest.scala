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

import java.io.File
import java.time.LocalDateTime

import com.holdenkarau.spark.testing.Utils
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.action.customlogic.{CustomDfsTransformer, CustomDfsTransformerConfig}
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class CustomSparkActionTest extends FunSuite with BeforeAndAfter {
  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  val tempDir: File = Utils.createTempDir()
  val tempPath: String = tempDir.toPath.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear()
  }

  test("spark action with custom transformation class to load multiple sources into multiple targets") {
    // setup DataObjects
    val feed = "multiple_dfs"

    val srcTable1 = Table(Some("default"), "copy_input1")
    HiveUtil.dropTable(session, srcTable1.db.get, srcTable1.name)
    val srcPath1 = tempPath + s"/${srcTable1.fullName}"
    val srcDO1 = HiveTableDataObject("src1", srcPath1, table = srcTable1, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO1)

    val srcTable2 = Table(Some("default"), "copy_input2")
    HiveUtil.dropTable(session, srcTable2.db.get, srcTable2.name)
    val srcPath2 = tempPath + s"/${srcTable2.fullName}"
    val srcDO2 = HiveTableDataObject("src2", srcPath2, table = srcTable2, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO2)

    val tgtTable1 = Table(Some("default"), "copy_output1", None, Some(Seq("lastname", "firstname")))
    HiveUtil.dropTable(session, tgtTable1.db.get, tgtTable1.name)
    val tgtPath1 = tempPath + s"/${tgtTable1.fullName}"
    val tgtDO1 = HiveTableDataObject("tgt1", tgtPath1, table = tgtTable1, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO1)


    val tgtTable2 = Table(Some("default"), "copy_output2", None, Some(Seq("lastname", "firstname")))
    HiveUtil.dropTable(session, tgtTable2.db.get, tgtTable2.name)
    val tgtPath2 = tempPath + s"/${tgtTable2.fullName}"
    val tgtDO2 = HiveTableDataObject("tgt2", tgtPath2, table = tgtTable2, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO2)

    // prepare & start load
    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(refTimestamp1))
    val customTransformerConfig = CustomDfsTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestDfsTransformer"))

    val action1 = CustomSparkAction("action1", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), transformer = customTransformerConfig)(context1.instanceRegistry)

    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable1, srcPath1, l1)
    TestUtil.prepareHiveTable(srcTable2, srcPath2, l1)

    val tgtSubFeeds = action1.exec(Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq())))
    assert(tgtSubFeeds.size == 2)
    assert(tgtSubFeeds.map(_.dataObjectId) == Seq(tgtDO1.id, tgtDO2.id))

    val r1 = session.table(s"${tgtTable1.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 6) // should be increased by 1 through TestDfTransformer

    // same for the second dataframe
    val r2 = session.table(s"${tgtTable2.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 6)
  }

}


class TestDfsTransformer extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._
    Map(
      "tgt1" -> dfs("src1").withColumn("rating", $"rating"+1)
    , "tgt2" -> dfs("src2").withColumn("rating", $"rating"+1)
    )
  }
}
