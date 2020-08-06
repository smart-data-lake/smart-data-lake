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
package io.smartdatalake.workflow.dataobject
import java.nio.file.Files
import java.time.LocalDateTime

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.action.CustomSparkAction
import io.smartdatalake.workflow.action.customlogic.{CustomDfsTransformer, CustomDfsTransformerConfig}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}


class TickTockHiveTableDataObjectTest extends FunSuite with BeforeAndAfter {
  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear()
  }

  test("Empty dataframe is created if schemaMin is provided") {

    val feed = "autocreate"
    val schemaMin: StructType = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable=false) :: StructField("rating", IntegerType, nullable=false) :: Nil)

    // create source dataobject
    val srcTable = Table(Some("default"), "input")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name)
    val srcPath = tempPath + s"/${srcTable.fullName}"
    val srcDO = TickTockHiveTableDataObject("input", Some(srcPath), table = srcTable, partitions = Seq(), numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)

    // create target
    val tgtTable = Table(Some("default"),"output")
    HiveUtil.dropTable(session, srcTable.db.get, srcTable.name)
    val tgtPath = tempPath + s"/${tgtTable.fullName}"
    val tgtDO = TickTockHiveTableDataObject("output", Some(tgtPath), table = tgtTable, partitions = Seq(), numInitialHdfsPartitions = 1, schemaMin=Some(schemaMin))
    instanceRegistry.register(tgtDO)

    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext(feed, "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val customTransformerConfig = CustomDfsTransformerConfig(className = Some("io.smartdatalake.workflow.dataobject.TestDfsTransformerEmptyDf"))

    val action = CustomSparkAction("action", List(srcDO.id), List(tgtDO.id), transformer = customTransformerConfig, recursiveInputIds = List(tgtDO.id))(context1.instanceRegistry)

    // write test files
    val l1 = Seq((1, "john", 5)).toDF("id", "name", "rating")
    TestUtil.prepareHiveTable(srcTable, srcPath, l1)

    val tgtSubFeeds = action.exec(Seq(SparkSubFeed(None, "input", Seq()), SparkSubFeed(None, "output", Seq())))

    val r = session.table(s"${tgtTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r.size == 1)
    assert(r.head == 6) // should be increased by 1 through TestDfTransformer
  }
}

class TestDfsTransformerEmptyDf extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._

    Map(
      "output" -> dfs("input").withColumn("rating", $"rating"+1)
    )

  }
}
