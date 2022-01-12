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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.SaveModeMergeOptions
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.connection.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, JdbcTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

class CopyWithMergeActionTest extends FunSuite with BeforeAndAfter {

  protected implicit val session : SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:file:target/JdbcTableDataObjectTest/hsqldb", "org.hsqldb.jdbcDriver")

  before {
    instanceRegistry.clear()
  }

  test("copy 1st 2nd load, SaveModeMergeOptions, schema evolution") {

    // setup DataObjects
    val feed = "copy"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"),  table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    instanceRegistry.register(jdbcConnection)
    val tgtTable = Table(Some("public"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = JdbcTableDataObject( "tgt1", table = tgtTable, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"lastname varchar(255), firstname varchar(255)"), allowSchemaEvolution = true)
    tgtDO.dropTable
    tgtDO.connection.dropTable(tgtTable.fullName+"_sdltmp")
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val action1 = CopyAction("dda", srcDO.id, tgtDO.id, saveModeOptions = Some(SaveModeMergeOptions()))
    val l1 = Seq(("doe","john",5),("pan","peter",5),("hans","muster",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))
    action1.exec(Seq(srcSubFeed))(contextExec)

    {
      val expected = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5))
        .toDF("lastname", "firstname", "rating")
      val actual = tgtDO.getDataFrame()
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load - schema evolution: column rating -> rating2!
    val l2 = Seq(("doe","john",10),("pan","peter",5),("pan","peter2",5)).toDF("lastname", "firstname", "rating2")
    srcDO.writeDataFrame(l2, Seq())
    action1.init(Seq(srcSubFeed))
    action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))(contextExec)

    {
      val expected = Seq(("doe", "john", Some(5), Some(10)), ("pan", "peter", Some(5), Some(5)), ("pan", "peter2", None, Some(5)), ("hans", "muster", Some(5), None))
        .toDF("lastname", "firstname", "rating", "rating2")
      val actual = tgtDO.getDataFrame()
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }
}
