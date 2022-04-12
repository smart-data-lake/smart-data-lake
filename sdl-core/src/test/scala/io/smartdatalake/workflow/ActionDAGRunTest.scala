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

package io.smartdatalake.workflow

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.SdlConfigObject._
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.{ResultRuntimeInfo, RuntimeEventState, RuntimeInfo, SDLExecutionId}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import java.time.{Duration, LocalDateTime}

class ActionDAGRunTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  test("convert ActionDAGRunState to json and back") {
    val df = Seq(("a",1)).toDF("txt", "value")
    val infoA = RuntimeInfo(SDLExecutionId.executionId1, RuntimeEventState.SUCCEEDED, startTstmp = Some(LocalDateTime.now()), duration = Some(Duration.ofMinutes(5)), msg = Some("test"),
      results = Seq(ResultRuntimeInfo(SparkSubFeed(Some(SparkDataFrame(df)), "do1", partitionValues = Seq(PartitionValues(Map("test"->1)))),Map("test"->1, "test2"->"abc"))), dataObjectsState = Seq(DataObjectState(DataObjectId("do1"), "test")))
    val state = ActionDAGRunState(SmartDataLakeBuilderConfig(), 1, 1, LocalDateTime.now, LocalDateTime.now, Map(ActionId("a") -> infoA), isFinal = false)
    val json = state.toJson
    println(json)
    // remove DataFrame from SparkSubFeed, it should not be serialized
    val expectedState = state.copy(actionsState = state.actionsState
      .mapValues(actionState => actionState
        .copy(results = actionState.results.map( result => result
          .copy(subFeed = result.subFeed match {
            case subFeed: SparkSubFeed => subFeed.copy(dataFrame = None)
            case subFeed => subFeed
          })))))
    // check
    val deserializedState = ActionDAGRunState.fromJson(json)
    assert(deserializedState == expectedState)
  }
}
