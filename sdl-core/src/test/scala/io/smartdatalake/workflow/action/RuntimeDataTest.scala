/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ExecutionPhase, GenericMetrics}
import org.scalatest.FunSuite

import java.time.LocalDateTime

class RuntimeDataTest extends FunSuite {

  test("store and get synchronous events") {
    val runtimeData = SynchronousRuntimeData(10)
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now().plusSeconds(1), ExecutionPhase.Exec, RuntimeEventState.SUCCEEDED, None, Seq()))
    assert(runtimeData.getEvents().size==2)
    assert(runtimeData.getEvents(Some(SDLExecutionId(1))).size==2)
    assert(runtimeData.getLatestEventState.contains(RuntimeEventState.SUCCEEDED))
    runtimeData.addEvent(SDLExecutionId(2), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.PREPARED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(2), RuntimeEvent(LocalDateTime.now().plusSeconds(1), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(2), RuntimeEvent(LocalDateTime.now().plusSeconds(2), ExecutionPhase.Exec, RuntimeEventState.FAILED, None, Seq()))
    assert(runtimeData.getEvents().size==3)
    assert(runtimeData.getEvents(Some(SDLExecutionId(1))).size==2)
    assert(runtimeData.getEvents(Some(SDLExecutionId(2))).size==3)
    assert(runtimeData.getLatestEventState.contains(RuntimeEventState.FAILED))
  }

  test("store and get asynchronous events") {
    val runtimeData = AsynchronousRuntimeData(10)
    // synchronous execution first
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now().plusSeconds(1), ExecutionPhase.Exec, RuntimeEventState.SUCCEEDED, None, Seq()))
    assert(runtimeData.getEvents().isEmpty) // only asynchronous events can be current
    assert(runtimeData.getLatestEventState.isEmpty)
    assert(runtimeData.getEvents(Some(SDLExecutionId(1))).size==2)
    // asynchronous execution
    runtimeData.addEvent(SparkStreamingExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.PREPARED, None, Seq()))
    runtimeData.addEvent(SparkStreamingExecutionId(1), RuntimeEvent(LocalDateTime.now().plusSeconds(1), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SparkStreamingExecutionId(1), RuntimeEvent(LocalDateTime.now().plusSeconds(2), ExecutionPhase.Exec, RuntimeEventState.FAILED, None, Seq()))
    assert(runtimeData.getEvents().size==3)
    assert(runtimeData.getEvents(Some(SDLExecutionId(1))).size==2)
    assert(runtimeData.getEvents(Some(SparkStreamingExecutionId(1))).size==3)
    assert(runtimeData.getLatestEventState.contains(RuntimeEventState.FAILED))
  }
  
  test("store and get synchronous metrics") {
    val runtimeData = SynchronousRuntimeData(10)
    val dataObjectId = DataObjectId("test")
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId, GenericMetrics("test-metric1", 1, Map("metric1" -> 1)))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId, GenericMetrics("test-metric2", 2, Map("metric2" -> 2)))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId+"dummy", GenericMetrics("test-metric99", 2, Map()))
    assert(runtimeData.getMetrics(dataObjectId, Some(SDLExecutionId(1))).exists(_.getMainInfos.isDefinedAt("metric2")))
    assert(runtimeData.getMetrics(dataObjectId).exists(_.getMainInfos.isDefinedAt("metric2")))
    intercept[AssertionError](runtimeData.addMetric(Some(SDLExecutionId(2)), dataObjectId, GenericMetrics("test2-metric1", 1, Map())))
    runtimeData.addEvent(SDLExecutionId(2), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SDLExecutionId(2)), dataObjectId, GenericMetrics("test2-metric1", 1, Map("metric1" -> 1)))
    runtimeData.addMetric(Some(SDLExecutionId(2)), dataObjectId, GenericMetrics("test2-metric2", 2, Map("metric2" -> 2)))
    assert(runtimeData.getMetrics(dataObjectId, Some(SDLExecutionId(2))).exists(_.getMainInfos.isDefinedAt("metric2")))
    assert(runtimeData.getMetrics(dataObjectId).exists(_.getMainInfos.isDefinedAt("metric2")))
  }

  test("store and get asynchronous metrics") {
    val runtimeData = AsynchronousRuntimeData(10)
    val dataObjectId = DataObjectId("test")
    // synchronous execution first
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId, GenericMetrics("spark-metric1", 1, Map()))
    runtimeData.addMetric(None, dataObjectId, GenericMetrics("spark-metric1", 1, Map()))
    // asynchronous execution 1
    runtimeData.addEvent(SparkStreamingExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SparkStreamingExecutionId(1)), dataObjectId, GenericMetrics("test-metric1", 1, Map("metric1" -> 1)))
    runtimeData.addMetric(Some(SparkStreamingExecutionId(1)), dataObjectId, GenericMetrics("test-metric2", 2, Map("metric2" -> 2)))
    runtimeData.addMetric(Some(SparkStreamingExecutionId(1)), dataObjectId+"dummy", GenericMetrics("test-metric99", 2, Map()))
    assert(runtimeData.getMetrics(dataObjectId, Some(SparkStreamingExecutionId(1))).exists(_.getMainInfos.isDefinedAt("metric2")))
    assert(runtimeData.getMetrics(dataObjectId).exists(_.getMainInfos.isDefinedAt("metric2")))
    // metric for wrong asynchronous execution
    intercept[AssertionError](runtimeData.addMetric(Some(SparkStreamingExecutionId(2)), dataObjectId, GenericMetrics("test2-metric1", 1, Map())))
    // another synchronous execution (should not happen in real-life, but nevertheless a test what happens)
    runtimeData.addEvent(SDLExecutionId(2), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SDLExecutionId(2)), dataObjectId, GenericMetrics("spark-metric2", 1, Map()))
    // second asynchronous execution
    runtimeData.addEvent(SparkStreamingExecutionId(2), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SparkStreamingExecutionId(2)), dataObjectId, GenericMetrics("test2-metric1", 1, Map("metric1" -> 1)))
    runtimeData.addMetric(Some(SparkStreamingExecutionId(2)), dataObjectId, GenericMetrics("test2-metric2", 2, Map("metric2" -> 2)))
    assert(runtimeData.getMetrics(dataObjectId, Some(SparkStreamingExecutionId(2))).exists(_.getMainInfos.isDefinedAt("metric2")))
    assert(runtimeData.getMetrics(dataObjectId).exists(_.getMainInfos.isDefinedAt("metric2")))
  }

  test("get final metrics and exception on late arriving metrics") {
    val runtimeData = SynchronousRuntimeData(10)
    val dataObjectId = DataObjectId("test")
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId, GenericMetrics("test-metric1", 1, Map("metric1" -> 1)))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId, GenericMetrics("test-metric2", 2, Map("metric2" -> 2)))
    runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId+"dummy", GenericMetrics("test-metric99", 2, Map()))
    assert(runtimeData.getFinalMetrics(dataObjectId).exists(_.getMainInfos.isDefinedAt("metric2")))
    intercept[LateArrivingMetricException](runtimeData.addMetric(Some(SDLExecutionId(1)), dataObjectId, GenericMetrics("test1-metric3", 3, Map())))
  }

  test("get summarized runtime info") {
    val runtimeData = SynchronousRuntimeData(10)
    val inputDataObjectId = DataObjectId("input")
    val outputDataObjectId = DataObjectId("test")
    val now = LocalDateTime.now()
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(now, ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq(SparkSubFeed(None, outputDataObjectId, Seq()))))
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(now.plusSeconds(10), ExecutionPhase.Exec, RuntimeEventState.SUCCEEDED, None, Seq(SparkSubFeed(None, outputDataObjectId, Seq()))))
    runtimeData.addMetric(Some(SDLExecutionId(1)), outputDataObjectId, GenericMetrics("test-metric1", 1, Map()))
    runtimeData.addMetric(Some(SDLExecutionId(1)), outputDataObjectId, GenericMetrics("test-metric2", 2, Map("test"->999)))
    runtimeData.addMetric(Some(SDLExecutionId(1)), outputDataObjectId+"dummy", GenericMetrics("test-metric99", 2, Map()))
    val info = runtimeData.getRuntimeInfo(Seq(inputDataObjectId), Seq(outputDataObjectId), Seq())
    assert(info.exists(_.duration.get.getSeconds == 10))
    val doResults = info.flatMap(_.results.find(_.subFeed.dataObjectId == outputDataObjectId))
    assert(doResults.exists(_.mainMetrics("test") == 999))
  }

  test("housekeeping") {
    val runtimeData = SynchronousRuntimeData(5)
    runtimeData.addEvent(SDLExecutionId(1), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(2), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(3), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(4), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    runtimeData.addEvent(SDLExecutionId(5), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    assert(runtimeData.getEvents(Some(SDLExecutionId(1))).size == 1)
    runtimeData.addEvent(SDLExecutionId(6), RuntimeEvent(LocalDateTime.now(), ExecutionPhase.Exec, RuntimeEventState.STARTED, None, Seq()))
    assert(runtimeData.getEvents(Some(SDLExecutionId(1))).isEmpty)
  }

}
