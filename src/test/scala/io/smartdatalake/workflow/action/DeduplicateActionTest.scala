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
import java.sql.Timestamp
import java.time.LocalDateTime

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table, TickTockHiveTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class DeduplicateActionTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear()
  }

  test("deduplicate 1st 2nd load") {

    // setup DataObjects
    val feed = "deduplicate"
    val srcTable = Table(Some("default"), "deduplicate_input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", Some(srcPath),  table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "deduplicate_output", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtTable.db.get, tgtTable.name )
    val tgtPath = tempPath+s"/${tgtTable.fullName}"
    val tgtDO = TickTockHiveTableDataObject( "tgt1", Some(tgtPath), table = tgtTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id)
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, l1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(session, context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = session.table(s"${tgtTable.fullName}")
      .select($"rating", $"dl_ts_captured")
      .as[(Int,Timestamp)].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head._2.toLocalDateTime == refTimestamp1)

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp2), SmartDataLakeBuilderConfig())
    val action2 = DeduplicateAction("dda2", srcDO.id, tgtDO.id)
    val l2 = Seq(("doe","john",10)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, l2)
    action2.exec(Seq(SparkSubFeed(None, "src1", Seq())))(session, context2)

    val r2 = session.table(s"${tgtTable.fullName}")
      .select($"rating", $"dl_ts_captured").orderBy($"dl_ts_captured")
      .as[(Int,Timestamp)].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head._1 == 10) // rating should be the second one
    assert(r2.head._2.toLocalDateTime == refTimestamp2)
  }

  test("early validation that output primary key exists") {
    // setup DataObjects
    val srcTable = Table(Some("default"), "deduplicate_input")
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "deduplicate_output")
    val tgtPath = tempPath+s"/${tgtTable.fullName}"
    val tgtDO = TickTockHiveTableDataObject( "tgt1", Some(tgtPath), table = tgtTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    intercept[IllegalArgumentException]{DeduplicateAction("dda", srcDO.id, tgtDO.id)}
  }

  test("deduplicate with filter clause") {

    // setup DataObjects
    val feed = "deduplicate"
    val srcTable = Table(Some("default"), "deduplicate_input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "deduplicate_output", None, Some(Seq("lastname","firstname")))
    HiveUtil.dropTable(session, tgtTable.db.get, tgtTable.name )
    val tgtPath = tempPath+s"/${tgtTable.fullName}"
    val tgtDO = TickTockHiveTableDataObject( "tgt1", Some(tgtPath), table = tgtTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, filterClause = Some("lastname='jonson'"))
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, l1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(session, context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = session.table(s"${tgtTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
  }
}
