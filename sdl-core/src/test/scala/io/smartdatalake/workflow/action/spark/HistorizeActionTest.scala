/*
 * sdl-core - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.historization.HistorizationTestUtils.defaultTimeAxisUnit
import io.smartdatalake.workflow.ExecutionPhase
import io.smartdatalake.workflow.action.HistorizeAction
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path => NioPath}
import java.sql.Timestamp
import java.time.LocalDateTime

class HistorizeActionTest extends AnyFunSuite with BeforeAndAfter
  with io.smartdatalake.testutils.spark.dataset.TestToolDataset
  with io.smartdatalake.util.spark.dataset.Equality {

  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:HistorizeActionTest", "org.hsqldb.jdbcDriver")

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  private var tempDir: NioPath = _
  private var tempPath: String = _

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
    tempDir = Files.createTempDirectory("test")
    tempPath = tempDir.toAbsolutePath.toString
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("historize load") {
    val context = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1", primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id)
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
    action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)
    assert(tgtSubFeed.asInstanceOf[SparkSubFeed].isDummy) // should return a dummy DataFrame as breakDataFrameOutputLineage is set to true

    {
      val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Environment.historizationUpperHorizonTimestamp))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context1)
        .drop(Historization.historizeHashColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 1st load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id)
    val l2 = Seq(("doe", "john", 10)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2, Seq())(context1)
    val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
    action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
    action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
    action2.init(Seq(srcSubFeed))(context2.copy(phase = ExecutionPhase.Init))
    action2.exec(Seq(srcSubFeed2))(context2)

    {
      val expected = Seq(
        ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minus(defaultTimeAxisUnit.get))),
        ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp)
      ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context1)
        .drop(Historization.historizeHashColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 3rd load with schema evolution
    val refTimestamp3 = LocalDateTime.now()
    val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
    val action3 = HistorizeAction("ha3", srcDO.id, tgtDO.id)
    val l3 = Seq(("doe", "john", 10, "test")).toDF("lastname", "firstname", "rating", "test")
    srcDO.writeSparkDataFrame(l3, Seq())(context3)
    val srcSubFeed3 = SparkSubFeed(None, "src1", Seq())
    action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
    action3.preInit(Seq(srcSubFeed), Seq())(context3.copy(phase = ExecutionPhase.Init))
    action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
    action3.exec(Seq(srcSubFeed3))(context3)

    {
      val expected = Seq(
        ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minus(defaultTimeAxisUnit.get))),
        ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minus(defaultTimeAxisUnit.get))),
        ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), definitions.Environment.historizationUpperHorizonTimestamp)
      ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context3)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  test("early validation that output primary key exists") {
    // setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1").register

    // check primary key missing
    val exception = intercept[IllegalArgumentException] {
      HistorizeAction("hist1", srcDO.id, tgtDO.id)
    }
    withClue(exception.getMessage) {
      assert(exception.getMessage.contains("Primary key must be defined for output DataObject"))
    }
  }
}
