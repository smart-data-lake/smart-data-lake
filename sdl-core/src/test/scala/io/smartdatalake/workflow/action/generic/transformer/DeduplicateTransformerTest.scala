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
package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.{ActionDAGRun, ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class DeduplicateTransformerTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextPrep: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Prepare)
  implicit val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec) // note that mutable Map dataFrameReuseStatistics is shared between contextInit & contextExec like this!

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
  }

  test("deduplication test with primary key") {

    // prepare
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)"), primaryKeyColumns = Some(Seq("id")))

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
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = Some("created_at"), primaryKeyColumns = Some(Seq("id")))

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
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)"), primaryKeyColumns = Some(Seq("pk1", "pk2")))

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

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1", primaryKey = Some(Seq("pk1", "pk2"))).register

    val df = Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at")
      .select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType))
    srcDO.writeSparkDataFrame(df)
    val tgtDO = MockSparkDataObject("tgt1").register

    // setup action
    val action = CopyAction("copy_with_deduplication", srcDO.id, tgtDO.id,
      transformers = Seq(DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)")))
    )
    instanceRegistry.register(action)

    // setup DAG
    val dag = ActionDAGRun(Seq(action))

    // execute
    val thrown = intercept[TaskFailedException] {
      dag.prepare(contextPrep)
      dag.init(contextInit)
      dag.exec(contextExec)
    }

    // check
    assert(thrown.cause.isInstanceOf[ConfigurationException])
  }

  test("deduplication test with primary key columns detection") {

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1", primaryKey = Some(Seq("pk1", "pk2"))).register

    val df = Seq(
      (1, 1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, 2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, 2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("pk1", "pk2", "created_at", "updated_at")
      .select($"pk1", $"pk2", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType))
    srcDO.writeSparkDataFrame(df)
    val tgtDO = MockSparkDataObject("tgt1", primaryKey = Some(Seq("pk1", "pk2"))).register

    // setup action
    val action = CopyAction("copy_with_deduplication", srcDO.id, tgtDO.id,
      transformers = Seq(DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)")))
    )
    instanceRegistry.register(action)

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

    val transformedDf = tgtDO.getSparkDataFrame()

    assert(transformedDf.collect sameElements resultDf.inner.collect)
  }

}
