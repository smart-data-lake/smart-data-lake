/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.app

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.testutils.{GenericExecFailTransformer, SmartDataLakeBuilderBehaviour}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, ScalaClassGenericDfTransformer}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.expectation.{Expectation, SQLExpectation}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for [[SmartDataLakeBuilder]] with the plain-Scala engine,
 * instantiating the engine-agnostic tests of [[SmartDataLakeBuilderBehaviour]].
 *
 * This tests use configuration test/resources/application.conf
 */
class SmartDataLakeBuilderTest extends AnyFunSuite with SmartDataLakeLogger with SmartDataLakeBuilderBehaviour {

  override def defaultEngineConnection: Connection with EngineConnection = ScalaTestUtil.defaultScalaConnection

  override def createMockDataObject(id: String, partitions: Seq[String], primaryKey: Option[Seq[String]], expectations: Seq[Expectation])(implicit instanceRegistry: InstanceRegistry): MockScalaDataObject = {
    MockScalaDataObject(id, partitions = partitions, primaryKey = primaryKey, expectations = expectations).register
  }

  override def failTransformer: GenericDfTransformer =
    ScalaClassGenericDfTransformer(className = classOf[GenericExecFailTransformer].getName, runtimeOptions = Map("phase" -> "executionPhase"))

  override def testCountExpectation: Expectation =
    SQLExpectation(name = "testCount", aggExpression = "count(*)", expectation = Some("= 0"))

  test("Test command line argument parsing") {
    val config = sdlb.parse(Seq("-c", "test.conf", "-f", "test", "-n", "name", "--partition-values", "dt=20000101,20000102")).get
    assert(config == SmartDataLakeBuilderConfig(configuration = Seq("test.conf"), feedSel = "test", applicationName = Some("name"), partitionValues = Some(Seq(PartitionValues(Map("dt" -> "20000101")), PartitionValues(Map("dt" -> "20000102"))))))
  }

  test("Test command line unbounded argument parsing") {
    val config = sdlb.parse(Seq("-c", "test.conf", "-f", "test", "-n", "name", "--partition-values", "dt=20000101", "--partition-values", "dt=20000102", "-o", "test.abc=def", "-o", "test.ghi=jkl")).get
    assert(config == SmartDataLakeBuilderConfig(configuration = Seq("test.conf"), feedSel = "test", applicationName = Some("name"),
      partitionValues = Some(Seq(PartitionValues(Map("dt" -> "20000101")), PartitionValues(Map("dt" -> "20000102")))),
      configurationValueOverwrite = Map(("test.abc", "def"), ("test.ghi", "jkl"))
    ))
  }

  test("Test command line config value overwrite") {
    implicit val hadoopConfiguration: Configuration = new Configuration()
    val appConfig = sdlb.parse(Seq("-c", "cp:/application.conf", "-f", "test", "-o", "global.abc=def", "-o", "global.synchronousStreamingTriggerIntervalSec=5")).get
    val hoconConfig = appConfig.getHoconConfig()
    assert(hoconConfig.getString("global.abc") == "def")
    assert(hoconConfig.getInt("global.synchronousStreamingTriggerIntervalSec") == 5)
  }

  test("sdlb run with 2 actions and positive top-level partition values filter, recovery after action 2 failed the first time") {
    testRecoveryAfterActionFailed()
  }

  test("sdlb run recovered although state file contains only succeeded and cancelled actions") {
    testRecoveryOfCancelledRun()
  }

  test("sdlb run not recovered because failed state file was accepted by moving it to succeeded directory") {
    testAcceptFailedRunInSucceededDir()
  }

  test("sdlb run with skipped action and recovery after action 2 failed the first time") {
    testRecoveryWithSkippedAction()
  }

  test("complex sdlb run with skipped action and recovery after action 2 failed the first time") {
    testComplexRecoveryWithSkippedActions()
  }

  test("sdlb run skipped action chain triggered from exec phase") {
    testSkippedActionChainTriggeredFromExecPhase()
  }

  test("sdlb run 2nd action skipped, check metrics") {
    testSkippedActionMetrics()
  }

  test("sdlb run incremental chain") {
    testIncrementalChain()
  }

  test("sdlb run with executionMode=PartitionDiffMode, increase runId on second run, state listener") {
    testPartitionDiffModeSecondRunStateListener()
  }

  test("sdlb run with 2 actions and PartitionDiffMode, recovery after action 2 failed the first time") {
    testPartitionDiffModeRecoveryWithExpectation()
  }
}
