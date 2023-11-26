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

import io.smartdatalake.app.{AppUtil, BuildVersionInfo, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.SdlConfigObject._
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.action.{RuntimeEventState, RuntimeInfo, SDLExecutionId}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import java.nio.file.Files
import java.time.{Duration, LocalDateTime}
import scala.collection.JavaConverters._

class ActionDAGRunTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  test("convert ActionDAGRunState to json and back") {
    val df = Seq(("a",1)).toDF("txt", "value")
    val startTime = LocalDateTime.now
    val duration = Duration.ofMinutes(5)
    val endTime = startTime.plus(duration)
    val infoA = RuntimeInfo(SDLExecutionId.executionId1, RuntimeEventState.SUCCEEDED, startTstmp = Some(startTime), duration = Some(duration), endTstmp = Some(endTime), msg = Some("test"),
      results = Seq(SparkSubFeed(Some(SparkDataFrame(df)), "do1", partitionValues = Seq(PartitionValues(Map("test"->1))), metrics = Some(Map("test"->1, "test2"->"abc")))),
      dataObjectsState = Seq(DataObjectState(DataObjectId("do1"), "test")))
    val buildVersionInfo = BuildVersionInfo.readBuildVersionInfo
    val appVersion = AppUtil.getManifestVersion
    val state = ActionDAGRunState(SmartDataLakeBuilderConfig(feedSel = "abc"), 1, 1, LocalDateTime.now, LocalDateTime.now, Map(ActionId("a") -> infoA), isFinal = false, Some(1), buildVersionInfo = buildVersionInfo, appVersion = appVersion )
    val json = state.toJson
    // remove DataFrame from SparkSubFeed, it should not be serialized
    val expectedState = state.copy(actionsState = state.actionsState
      .mapValues(actionState => actionState
        .copy(results = actionState.results.map {
          case subFeed: SparkSubFeed => subFeed.copy(dataFrame = None)
          case subFeed => subFeed
        })))
    // check
    val deserializedState = ActionDAGRunState.fromJson(json)
    assert(deserializedState == expectedState)
  }

  test("read old state version") {
    val stateContent = CustomCodeUtil.readResourceFile("stateFileV2.json")
    val migratedState = ActionDAGRunState.fromJson(stateContent)
    assert(migratedState.runStateFormatVersion.get == ActionDAGRunState.runStateFormatVersion)
  }

  test("append to state index file") {
    // enable writing index json
    Environment._hadoopFileStateStoreIndexAppend = Some(true)

    // prepare state
    val df = Seq(("a", 1)).toDF("txt", "value")
    val startTime = LocalDateTime.now
    val duration = Duration.ofMinutes(5)
    val endTime = startTime.plus(duration)
    val infoA = RuntimeInfo(SDLExecutionId.executionId1, RuntimeEventState.SUCCEEDED, startTstmp = Some(startTime), duration = Some(duration), endTstmp = Some(endTime), msg = Some("test"),
      results = Seq(SparkSubFeed(Some(SparkDataFrame(df)), "do1", partitionValues = Seq(PartitionValues(Map("test" -> 1))), metrics = Some(Map("test" -> 1, "test2" -> "abc")))),
      dataObjectsState = Seq(DataObjectState(DataObjectId("do1"), "test")))
    val buildVersionInfo = BuildVersionInfo.readBuildVersionInfo
    val appVersion = AppUtil.getManifestVersion
    val state = ActionDAGRunState(SmartDataLakeBuilderConfig(feedSel = "abc"), 1, 1, LocalDateTime.now, LocalDateTime.now, Map(ActionId("a") -> infoA), isFinal = true, Some(1), buildVersionInfo = buildVersionInfo, appVersion = appVersion)

    // prepare state store
    val tempDir = Files.createTempDirectory("test")
    val stateStore = HadoopFileActionDAGRunStateStore(tempDir.toString, "test", session.sparkContext.hadoopConfiguration)

    // write first state
    {
      stateStore.saveState(state)
      val index = HdfsUtil.readHadoopFile(tempDir.resolve("index").toString)(session.sparkContext.hadoopConfiguration)
      assert(index.linesIterator.count(_.trim.nonEmpty) == 1)
    }

    // write second state
    {
      stateStore.saveState(state)
      val index = HdfsUtil.readHadoopFile(tempDir.resolve("index").toString)(session.sparkContext.hadoopConfiguration)
      assert(index.linesIterator.count(_.trim.nonEmpty) == 2)
    }
  }
}
