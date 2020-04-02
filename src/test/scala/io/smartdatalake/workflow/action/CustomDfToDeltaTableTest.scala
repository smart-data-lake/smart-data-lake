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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.dataobject.{CustomDfDataObject, DeltaLakeTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class CustomDfToDeltaTableTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  val tempDir = Files.createTempDirectory("tempHadoopDO")
  val tempPath: String = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before { instanceRegistry.clear() }

  test("CustomDf2DeltaTable") {

    // setup DataObjects
    val feed = "customDf2Delta"
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some("io.smartdatalake.config.TestCustomDfCreator")))
    val targetTable = Table(db = Some("default"), name = "custom_df_copy", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", path=targetTablePath, table=targetTable, numInitialHdfsPartitions=1)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val startTime = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(startTime))
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))

    val expected = sourceDO.getDataFrame()
    val actual = targetDO.getDataFrame()
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("Df2HiveTable",Seq())(actual)(expected)
    assert(resultat)
  }

  test("CustomDf2DeltaTable_partitioned") {

    // setup DataObjects
    val feed = "customDf2Delta_partitioned"
    val sourceDO = CustomDfDataObject(id="source",creator = CustomDfCreatorConfig(className = Some("io.smartdatalake.config.TestCustomDfCreator")))
    val targetTable = Table(db = Some("default"), name = "custom_df_copy_partitioned", query = None, primaryKey = Some(Seq("line")))
    val targetTablePath = tempPath+s"/${targetTable.fullName}"
    val targetDO = DeltaLakeTableDataObject(id="target", partitions=Seq("num"), path=targetTablePath, table=targetTable, numInitialHdfsPartitions=1)
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val startTime = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext(feed, "test", instanceRegistry, Some(startTime))
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))

    val expected = sourceDO.getDataFrame()
    val actual = targetDO.getDataFrame()
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("Df2HiveTable",Seq())(actual)(expected)
    assert(resultat)
  }

}
