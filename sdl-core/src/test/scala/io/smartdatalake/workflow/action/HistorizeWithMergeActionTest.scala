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
import io.smartdatalake.definitions
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ExecutionPhase
 import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
 import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, JdbcTableDataObject, Table}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.{Files, Path => NioPath}
import java.sql.Timestamp
import java.time.LocalDateTime

 class HistorizeWithMergeActionTest extends FunSuite with BeforeAndAfter {

   protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
   import session.implicits._

   implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

   private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:file:target/JdbcTableDataObjectTest/hsqldb", "org.hsqldb.jdbcDriver")

   private var tempDir: NioPath = _
   private var tempPath: String = _

   before {
     instanceRegistry.clear()
     tempDir = Files.createTempDirectory("test")
     tempPath = tempDir.toAbsolutePath.toString
   }

   after {
     FileUtils.deleteDirectory(tempDir.toFile)
   }

   test("historize load mergeModeEnable") {

     val context = TestUtil.getDefaultActionPipelineContext

     // setup DataObjects
     val feed = "historize"
     val srcTable = Table(Some("default"), "historize_input")
     val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
     srcDO.dropTable(context)
     instanceRegistry.register(srcDO)
     instanceRegistry.register(jdbcConnection)
     val tgtTable = Table(Some("public"), "historize_output", None, Some(Seq("lastname","firstname")))
     val tgtDO = JdbcTableDataObject( "tgt1", table = tgtTable, connectionId = "jdbcCon1")//, jdbcOptions = Map("createTableColumnTypes"->"lastname varchar, firstname varchar"))
     tgtDO.dropTable(context)
     instanceRegistry.register(tgtDO)

     // prepare & start 1st load
     val refTimestamp1 = LocalDateTime.now()
     val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
     val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true)
     val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
     srcDO.writeSparkDataFrame(l1, Seq())(context1)
     val srcSubFeed = SparkSubFeed(None, "src1", Seq())
     action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
     action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
     action1.exec(Seq(srcSubFeed))(context1)

     {
       val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp)))
         .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getSparkDataFrame()(context1)
         .drop(Historization.historizeHashColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
       assert(resultat)
     }

     // prepare & start 2nd load
     val refTimestamp2 = LocalDateTime.now()
     val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
     val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true)
     val l2 = Seq(("doe","john",10)).toDF("lastname", "firstname", "rating")
     srcDO.writeSparkDataFrame(l2, Seq())(context2)
     val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
     action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
     action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
     action2.exec(Seq(srcSubFeed2))(context2)

     {
       val expected = Seq(
         ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
         ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp))
       ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getSparkDataFrame()(context2)
         .drop(Historization.historizeHashColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
       assert(resultat)
     }

     // prepare & start 3rd load with schema evolution
     val refTimestamp3 = LocalDateTime.now()
     val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
     val tgtDOwithSchemaEvolution = tgtDO.copy(id = "tgt3", allowSchemaEvolution = true) // table remains the same...
     instanceRegistry.register(tgtDOwithSchemaEvolution)
     val action3 = HistorizeAction("ha3", srcDO.id, tgtDOwithSchemaEvolution.id, mergeModeEnable = true)
     val l3 = Seq(("doe","john",10,"test")).toDF("lastname", "firstname", "rating", "test")
     srcDO.writeSparkDataFrame(l3, Seq())(context3)
     val srcSubFeed3 = SparkSubFeed(None, "src1", Seq())
     action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
     action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
     action3.exec(Seq(srcSubFeed3))(context3)

     {
       val expected = Seq(
         ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
         ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minusNanos(1000000L))),
         ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp))
       ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getSparkDataFrame()(context3)
         .drop(Historization.historizeHashColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
       assert(resultat)
     }
   }

   test("historize load mergeModeEnable CDC") {

     val context = TestUtil.getDefaultActionPipelineContext

     // setup DataObjects
     val feed = "historize"
     val srcTable = Table(Some("default"), "historize_input")
     val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
     srcDO.dropTable(context)
     instanceRegistry.register(srcDO)
     instanceRegistry.register(jdbcConnection)
     val tgtTable = Table(Some("public"), "historize_output", None, Some(Seq("lastname","firstname")))
     val tgtDO = JdbcTableDataObject( "tgt1", table = tgtTable, connectionId = "jdbcCon1")//, jdbcOptions = Map("createTableColumnTypes"->"lastname varchar, firstname varchar"))
     tgtDO.dropTable(context)
     instanceRegistry.register(tgtDO)

     // prepare & start 1st load
     val refTimestamp1 = LocalDateTime.now()
     val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
     val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
     val l1 = Seq(("doe","john",5,"new"), ("pan","peter",5,"new")).toDF("lastname", "firstname", "rating", "operation")
     srcDO.writeSparkDataFrame(l1, Seq())(context1)
     val srcSubFeed = SparkSubFeed(None, "src1", Seq())
     action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
     action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
     action1.exec(Seq(srcSubFeed))(context1)

     {
       val expected = Seq(
          ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp)),
          ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp))
       ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getSparkDataFrame()(context1)
         .drop(Historization.historizeDummyColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
       assert(resultat)
     }

     // prepare & start 2nd load
     val refTimestamp2 = LocalDateTime.now()
     val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
     val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
     val l2 = Seq(("doe","john",10,"updated"), ("pan","peter",5,"deleted")).toDF("lastname", "firstname", "rating", "operation")
     srcDO.writeSparkDataFrame(l2, Seq())(context1)
     val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
     action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
     action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
     action2.exec(Seq(srcSubFeed2))(context2)

     {
       val expected = Seq(
         ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
         ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp)),
         ("pan","peter", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
       ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getSparkDataFrame()(context1)
         .drop(Historization.historizeDummyColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
       assert(resultat)
     }

     // prepare & start 3rd load with schema evolution
     val refTimestamp3 = LocalDateTime.now()
     val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
     val tgtDOwithSchemaEvolution = tgtDO.copy(id = "tgt3", allowSchemaEvolution = true) // table remains the same...
     instanceRegistry.register(tgtDOwithSchemaEvolution)
     val action3 = HistorizeAction("ha3", srcDO.id, tgtDOwithSchemaEvolution.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
     val l3 = Seq(("doe","john",10,"test","updated")).toDF("lastname", "firstname", "rating", "test", "operation")
     srcDO.writeSparkDataFrame(l3, Seq())(context3)
     val srcSubFeed3 = SparkSubFeed(None, "src1", Seq())
     action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
     action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
     action3.exec(Seq(srcSubFeed3))(context3)

     {
       val expected = Seq(
         ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
         ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minusNanos(1000000L))),
         ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp)),
         ("pan","peter", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L)))
       ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getSparkDataFrame()(context3)
         .drop(Historization.historizeDummyColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
       assert(resultat)
     }
   }
 }
