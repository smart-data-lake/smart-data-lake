/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.workflow.{ActionDAGRun, ActionPipelineContext, ExecutionPhase}
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.types.TimestampType

import java.nio.file.Files
class DeduplicateTransformerTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit var contextInit: ActionPipelineContext = _
  var contextPrep: ActionPipelineContext = _
  var contextExec: ActionPipelineContext = _

  before {
    Environment._instanceRegistry = new InstanceRegistry()
    contextInit = TestUtil.getDefaultActionPipelineContext(Environment.instanceRegistry)
    contextPrep = contextInit.copy(phase = ExecutionPhase.Prepare)
    contextExec = contextInit.copy(phase = ExecutionPhase.Exec) // note that mutable Map dataFrameReuseStatistics is shared between contextInit & contextExec like this!
    Environment.instanceRegistry.clear()
  }

  test("deduplication test with primary key") {

    // prepare
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = "coalesce(updated_at, created_at)", primaryKeyColumns = Some(Seq("id")))

    val df = SparkDataFrame(Seq(
      (1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("id", "created_at", "updated_at").select($"id", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    val resultDf = SparkDataFrame(Seq(
      (1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("id", "created_at", "updated_at").select($"id", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    // execute
    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformedDf.collect == resultDf.collect)
  }

  test("deduplication test with primary key and different rankingExpression") {

    // prepare
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = "created_at", primaryKeyColumns = Some(Seq("id")))

    val df = SparkDataFrame(Seq(
      (1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("id", "created_at", "updated_at").select($"id", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    val resultDf = SparkDataFrame(Seq(
      (1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("id", "created_at", "updated_at").select($"id", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    // execute
    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformedDf.collect == resultDf.collect)
  }

  test("deduplication test with multiple primary key columns") {

    // prepare
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = "coalesce(updated_at, created_at)", primaryKeyColumns = Some(Seq("pk1", "pk2")))

    val df = SparkDataFrame(Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at").select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    val resultDf = SparkDataFrame(Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at").select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    // execute
    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformedDf.collect == resultDf.collect)
  }

  test("deduplication test without primary key") {

    // prepare
    val feed = "deduplicate_pipeline"

    // setup DataObjects
    val srcTable = Table(Some("default"), "deduplicate_input", primaryKey = Some(Seq("pk1", "pk2")))
    val srcDO = HiveTableDataObject("src1", Some(tempPath + s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)(Environment.instanceRegistry)
    srcDO.dropTable
    Environment.instanceRegistry.register(srcDO)

    val df = Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at")
      .select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType))

    srcDO.writeSparkDataFrame(df)

    val tgtTable = Table(Some("default"), "deduplicate_output")
    val tgtDO = HiveTableDataObject("tgt1", Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, numInitialHdfsPartitions = 1)(Environment.instanceRegistry)
    tgtDO.dropTable
    Environment.instanceRegistry.register(tgtDO)

    // setup action
    val action = CopyAction("copy_with_deduplication", srcDO.id, tgtDO.id, transformers = Seq(DeduplicateTransformer(rankingExpression = "coalesce(updated_at, created_at)")))(Environment.instanceRegistry)
    Environment.instanceRegistry.register(action)

    // setup DAG
    val dag = ActionDAGRun(Seq(action))

    // execute
    val thrown = intercept[TaskFailedException] {
      dag.prepare(contextPrep)
      dag.init(contextInit)
      dag.exec(contextExec)
    }

    // check
    assert(thrown.isInstanceOf[TaskFailedException])
    assert(thrown.getMessage == "Task copy_with_deduplication failed. Root cause is 'IllegalArgumentException: requirement failed: There are no primary key columns defined ether by parameter nor by detection with actionId.'")
  }

  test("deduplication test with primary key columns detection") {

    // prepare
    val feed = "deduplicate_pipeline"

    // setup DataObjects
    val srcTable = Table(Some("default"), "deduplicate_input", primaryKey = Some(Seq("pk1", "pk2")))
    val srcDO = HiveTableDataObject("src1", Some(tempPath + s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)(Environment.instanceRegistry)
    srcDO.dropTable
    Environment.instanceRegistry.register(srcDO)

    val df = Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at")
      .select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType))

    srcDO.writeSparkDataFrame(df)

    val tgtTable = Table(Some("default"), "deduplicate_output", None, Some(Seq("pk1", "pk2")))
    val tgtDO = HiveTableDataObject("tgt1", Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, numInitialHdfsPartitions = 1)(Environment.instanceRegistry)
    tgtDO.dropTable
    Environment.instanceRegistry.register(tgtDO)


    // setup action
    val action = CopyAction("copy_with_deduplication", srcDO.id, tgtDO.id, transformers = Seq(DeduplicateTransformer(rankingExpression = "coalesce(updated_at, created_at)")))(Environment.instanceRegistry)
    Environment.instanceRegistry.register(action)

    // setup DAG
    val dag = ActionDAGRun(Seq(action))

    // execute
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val resultDf = SparkDataFrame(Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at")
      .select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    val transformedDf = session.table(s"${tgtTable.fullName}")

    assert(transformedDf.collect sameElements resultDf.inner.collect)
  }

}
