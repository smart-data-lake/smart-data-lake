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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.connection.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.{DeltaLakeModulePlugin, DeltaLakeTableDataObject, HiveTableDataObject, JdbcTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime

 class DeltaLakeHistorizeWithMergeActionTest extends FunSuite with BeforeAndAfter {

   protected implicit val session: SparkSession =  new DeltaLakeModulePlugin().additionalSparkProperties()
     .foldLeft(TestUtil.sparkSessionBuilder(withHive = true)) {
       case (builder, config) => builder.config(config._1, config._2)
     }.getOrCreate()
   import session.implicits._

   private val tempDir = Files.createTempDirectory("test")
   private val tempPath = tempDir.toAbsolutePath.toString

   implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

   before {
     instanceRegistry.clear()
   }

   test("historize 1st 2nd load mergeModeEnable") {

     // setup DataObjects
     val feed = "historize"
     val srcTable = Table(Some("default"), "historize_input")
     val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
     srcDO.dropTable
     instanceRegistry.register(srcDO)
     val tgtTable = Table(Some("default"), "historize_output", None, Some(Seq("lastname","firstname")))
     val tgtDO = DeltaLakeTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable)
     tgtDO.dropTable
     instanceRegistry.register(tgtDO)

     // prepare & start 1st load
     val refTimestamp1 = LocalDateTime.now()
     val context1 = ActionPipelineContext(feed, "test", SDLExecutionId.executionId1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig(), phase = ExecutionPhase.Exec)
     val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true)
     val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
     srcDO.writeDataFrame(l1, Seq())(session, context1)
     val srcSubFeed = SparkSubFeed(None, "src1", Seq())
     action1.init(Seq(srcSubFeed))(session, context1).head
     action1.exec(Seq(srcSubFeed))(session,context1).head

     {
       val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp)))
         .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getDataFrame()(session, context1)
         .drop(Historization.historizeHashColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
       assert(resultat)
     }

     // prepare & start 2nd load
     val refTimestamp2 = LocalDateTime.now()
     val context2 = ActionPipelineContext(feed, "test", SDLExecutionId.executionId1, instanceRegistry, Some(refTimestamp2), SmartDataLakeBuilderConfig(), phase = ExecutionPhase.Exec)
     val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true)
     val l2 = Seq(("doe","john",10)).toDF("lastname", "firstname", "rating")
     srcDO.writeDataFrame(l2, Seq())(session, context1)
     val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
     action2.exec(Seq(srcSubFeed2))(session, context2)

     {
       val expected = Seq(
         ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
         ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(definitions.HiveConventions.getHistorizationSurrogateTimestamp))
       ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
       val actual = tgtDO.getDataFrame()(session, context1)
         .drop(Historization.historizeHashColName)
       val resultat = expected.isEqual(actual)
       if (!resultat) TestUtil.printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
       assert(resultat)
     }
   }
 }
