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
package io.smartdatalake.testutils

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.generic.transformer.DeduplicateTransformer
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame, TableDataObject}
import io.smartdatalake.workflow.{ActionDAGRun, ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}
import org.scalatest.Assertions

import java.sql.Timestamp
import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[DeduplicateTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * Note: not portable to ScalaSubFeed today: every test here uses `rankingExpression`, which is implemented via
 * `window`/`row_number`, neither of which is implemented for ScalaSubFeed.
 */
trait DeduplicateTransformerBehaviour extends Assertions {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testDeduplicationWithPrimaryKey(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)"), primaryKeyColumns = Some(Seq("id")))

    val df = Seq(
      (1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2019-05-26 13:37:09")),
      (2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("id", "created_at", "updated_at")

    val resultDf = Seq(
      (1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("id", "created_at", "updated_at")

    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testDeduplicationWithPrimaryKeyAndDifferentRankingExpression(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = Some("created_at"), primaryKeyColumns = Some(Seq("id")))

    val df = Seq(
      (1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2019-05-26 13:37:09")),
      (2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("id", "created_at", "updated_at")

    val resultDf = Seq(
      (1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("id", "created_at", "updated_at")

    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testDeduplicationWithMultiplePrimaryKeyColumns(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)"), primaryKeyColumns = Some(Seq("pk1", "pk2")))

    val df = Seq(
      (1, 1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, 2, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2019-05-26 13:37:09")),
      (2, 2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("pk1", "pk2", "created_at", "updated_at")

    val resultDf = Seq(
      (1, 1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, 2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("pk1", "pk2", "created_at", "updated_at")

    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testDeduplicationWithoutPrimaryKey(
      createSrcDataObject: String => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: String => TableDataObject
  ): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val srcDO = createSrcDataObject("src1")
    instanceRegistry.register(srcDO)
    val df = Seq(
      (1, 1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, 2, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2019-05-26 13:37:09")),
      (2, 2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("pk1", "pk2", "created_at", "updated_at")
    srcDO.writeDataFrame(df, Seq())
    val tgtDO = createTgtDataObject("tgt1")
    instanceRegistry.register(tgtDO)

    val action = CopyAction("copy_with_deduplication", srcDO.id, tgtDO.id,
      transformers = Seq(DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)")))
    )
    instanceRegistry.register(action)

    val dag = ActionDAGRun(Seq(action))
    val contextPrep = context.copy(phase = ExecutionPhase.Prepare)
    val contextInit = context.copy(phase = ExecutionPhase.Init)
    val contextExec = context.copy(phase = ExecutionPhase.Exec)

    val thrown = intercept[TaskFailedException] {
      dag.prepare(contextPrep)
      dag.init(contextInit)
      dag.exec(contextExec)
    }

    assert(thrown.cause.isInstanceOf[ConfigurationException])
  }

  def testDeduplicationWithPrimaryKeyColumnsDetection(
      createSrcDataObject: String => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Seq[String]) => TableDataObject with CanCreateDataFrame
  ): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val srcDO = createSrcDataObject("src1")
    instanceRegistry.register(srcDO)
    val df = Seq(
      (1, 1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, 2, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2019-05-26 13:37:09")),
      (2, 2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("pk1", "pk2", "created_at", "updated_at")
    srcDO.writeDataFrame(df, Seq())
    val tgtDO = createTgtDataObject("tgt1", Seq("pk1", "pk2"))
    instanceRegistry.register(tgtDO)

    val action = CopyAction("copy_with_deduplication", srcDO.id, tgtDO.id,
      transformers = Seq(DeduplicateTransformer(rankingExpression = Some("coalesce(updated_at, created_at)")))
    )
    instanceRegistry.register(action)

    val dag = ActionDAGRun(Seq(action))
    val contextPrep = context.copy(phase = ExecutionPhase.Prepare)
    val contextInit = context.copy(phase = ExecutionPhase.Init)
    val contextExec = context.copy(phase = ExecutionPhase.Exec)

    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    val resultDf = Seq(
      (1, 1, Timestamp.valueOf("2019-04-25 12:23:29"), Timestamp.valueOf("2020-06-21 22:51:48")),
      (2, 2, Timestamp.valueOf("2019-05-26 13:37:10"), Timestamp.valueOf("2023-06-16 01:55:49"))
    ).toDF("pk1", "pk2", "created_at", "updated_at")

    val transformedDf = tgtDO.getDataFrame()(contextExec)

    assert(resultDf.isEqual(transformedDf))
  }
}
