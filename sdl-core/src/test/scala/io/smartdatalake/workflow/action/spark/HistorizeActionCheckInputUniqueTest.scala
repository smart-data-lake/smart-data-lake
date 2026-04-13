/*
 * Smart Data Lake - Build your data lake the smart way.
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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.workflow.action.HistorizeAction
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class HistorizeActionCheckInputUniqueTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)
  val contextPrepare: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Prepare)

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
  }

  test("HistorizeAction with checkInputUnique=true should succeed with unique input keys") {

    // Setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1", primaryKey = Some(Seq("id"))).register

    // Create input data with unique keys
    val inputDf = Seq(
      (1, "alice",   100),
      (2, "bob",     200),
      (3, "charlie", 300)
    ).toDF("id", "name", "amount")

    // Prepare action with checkInputUnique enabled
    val action = HistorizeAction("ha1", srcDO.id, tgtDO.id, checkInputUnique = true)

    srcDO.writeSparkDataFrame(inputDf, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())

    // Execute action - should succeed
    action.prepare(contextPrepare)
    action.preInit(Seq(srcSubFeed), Seq())(contextInit)
    action.init(Seq(srcSubFeed))(contextInit)
    action.exec(Seq(srcSubFeed))(contextExec).head

    // Verify historized data was written
    val result = tgtDO.getSparkDataFrame()
    assert(result.count() == 3)
    assert(result.columns.contains(Environment.capturedColumnName))
    assert(result.columns.contains(Environment.delimitedColumnName))
  }

  test("HistorizeAction with checkInputUnique=true should fail with duplicate input keys") {
    // Setup DataObjects
    val srcDO = MockSparkDataObject("src2").register
    val tgtDO = MockSparkDataObject("tgt2", primaryKey = Some(Seq("id"))).register

    // Prepare action with checkInputUnique enabled
    val action = HistorizeAction("ha2", srcDO.id, tgtDO.id, checkInputUnique = true)

    // Create input data with duplicate keys
    val inputDf = Seq(
      (1, "alice",           100),
      (2, "bob",             200),
      (1, "alice_duplicate", 150) // Duplicate id=1
    ).toDF("id", "name", "amount")

    srcDO.writeSparkDataFrame(inputDf, Seq())
    val srcSubFeed = SparkSubFeed(None, "src2", Seq())

    // Execute action - should fail with DuplicateInputDataException
    action.prepare(contextPrepare)
    action.preInit(Seq(srcSubFeed), Seq())(contextInit)
    action.init(Seq(srcSubFeed))(contextInit)
    val ex = intercept[TaskFailedException] {
      action.exec(Seq(srcSubFeed))(contextExec)
    }.getCause

    // Verify error message mentions uniqueness validation failure
    assert(ex.getMessage.contains("uniqueness validation failed"))
    assert(ex.getMessage.contains("duplicate"))
    assert(ex.getMessage.contains("id"))
    // Verify error message contains sample of duplicate records
    assert(ex.getMessage.contains("Sample of duplicate records:"))
  }

  test("HistorizeAction with checkInputUnique=false should succeed with duplicate input keys (default behavior)") {
    // Setup DataObjects
    val srcDO = MockSparkDataObject("src3").register
    val tgtDO = MockSparkDataObject("tgt3", primaryKey = Some(Seq("id"))).register

    // Prepare action with checkInputUnique disabled (default)
    val action = HistorizeAction("ha3", srcDO.id, tgtDO.id)

    // Create input data with duplicate keys
    val inputDf = Seq(
      (1, "alice",           100),
      (2, "bob",             200),
      (1, "alice_duplicate", 150) // Duplicate id=1 - will be deduplicated
    ).toDF("id", "name", "amount")

    srcDO.writeSparkDataFrame(inputDf, Seq())
    val srcSubFeed = SparkSubFeed(None, "src3", Seq())

    // Execute action - should succeed (duplicates are silently dropped)
    action.prepare(contextPrepare)
    action.preInit(Seq(srcSubFeed), Seq())(contextInit)
    action.init(Seq(srcSubFeed))(contextInit)
    action.exec(Seq(srcSubFeed))(contextExec).head

    // Verify historized data was written with only 2 unique records
    val result = tgtDO.getSparkDataFrame()
    assert(result.count() == 2) // Duplicates were dropped
  }

  test("HistorizeAction with checkInputUnique=true and composite primary key") {
    // Setup DataObjects
    val srcDO = MockSparkDataObject("src5").register
    val tgtDO = MockSparkDataObject("tgt5", primaryKey = Some(Seq("country", "city"))).register

    // Prepare action with checkInputUnique enabled
    val action = HistorizeAction("ha5", srcDO.id, tgtDO.id, checkInputUnique = true)

    // Create input data with unique composite keys
    val inputDf = Seq(
      ("USA", "New York",    1000),
      ("USA", "Los Angeles", 800),
      ("UK",  "London",      900)
    ).toDF("country", "city", "population")

    srcDO.writeSparkDataFrame(inputDf, Seq())
    val srcSubFeed = SparkSubFeed(None, "src5", Seq())

    // Execute action - should succeed
    action.prepare(contextPrepare)
    action.preInit(Seq(srcSubFeed), Seq())(contextInit)
    action.init(Seq(srcSubFeed))(contextInit)
    action.exec(Seq(srcSubFeed))(contextExec).head

    // Verify historized data was written
    val result = tgtDO.getSparkDataFrame()
    assert(result.count() == 3)
  }

  test("HistorizeAction with checkInputUnique=true and composite primary key with duplicates") {
    // Setup DataObjects
    val srcDO = MockSparkDataObject("src6").register
    val tgtDO = MockSparkDataObject("tgt6", primaryKey = Some(Seq("country", "city"))).register

    // Prepare action with checkInputUnique enabled
    val action = HistorizeAction("ha6", srcDO.id, tgtDO.id, checkInputUnique = true)

    // Create input data with duplicate composite keys
    val inputDf = Seq(
      ("USA", "New York",    1000),
      ("USA", "Los Angeles", 800),
      ("USA", "New York",    1100) // Duplicate (USA, New York)
    ).toDF("country", "city", "population")

    srcDO.writeSparkDataFrame(inputDf, Seq())
    val srcSubFeed = SparkSubFeed(None, "src6", Seq())

    // Execute action - should fail
    action.prepare(contextPrepare)
    action.preInit(Seq(srcSubFeed), Seq())(contextInit)
    action.init(Seq(srcSubFeed))(contextInit)
    val ex = intercept[TaskFailedException] {
      action.exec(Seq(srcSubFeed))(contextExec)
    }.getCause

    // Verify error message mentions uniqueness validation failure and both key columns
    assert(ex.getMessage.contains("uniqueness validation failed"))
    assert(ex.getMessage.contains("duplicate"))
    assert(ex.getMessage.contains("country") && ex.getMessage.contains("city"))
    // Verify error message contains sample of duplicate records
    assert(ex.getMessage.contains("Sample of duplicate records:"))
  }
}
