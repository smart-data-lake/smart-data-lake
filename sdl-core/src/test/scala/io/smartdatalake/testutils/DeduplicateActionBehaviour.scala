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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.plainScala.ScalaTestUtil.getCommonSubFeed
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.DeduplicateAction
import io.smartdatalake.workflow.action.generic.transformer.{FilterTransformer, SQLDfTransformer}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.{LocalDateTime, Month}
import scala.reflect.runtime.universe.Type

trait DeduplicateActionBehaviour extends GenericTestTool {
  this: SmartDataLakeLogger =>

  implicit private val implicitLogger: Logger = logger

  import io.smartdatalake.testutils.plainScala.ScalaTestUtil.registerDataObject

  def defaultEngineConnection: Connection with EngineConnection

  private def sourceTsCol = "last_updated"

  private def ts(str: String) = Timestamp.valueOf(LocalDateTime.parse(str.replace(" ", "T")))

  def testDeduplicateTwoRuns(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id)
    val l1 = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)
    assert(tgtSubFeed.asInstanceOf[DataFrameSubFeed].dataFrame.isEmpty) // should transport only the schema, as cacheOutput is false by default

    {
      val expected = Seq(
        ("doe",  "john",   5, Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter",  5, Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(("doe", "john", 10), ("pan", "peter", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())(context1)
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context2)

    {
      // note that we expect pan/peter/5 with updated refTimestamp even though all attributes stay the same
      val expected = Seq(
        ("doe",  "john",   10, Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter",  5,  Timestamp.valueOf(refTimestamp2)),
        ("hans", "muster", 5,  Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  def testDeduplicateWithFilter(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val subFeedType = getCommonSubFeed(srcDO, tgtDO)
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper._
    import helper.implicits._

    // prepare & start 1st load
    // note: pass the actual subFeedType as subFeedTypeForValidation, as the default (SparkSubFeed) might not be on the classpath
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(LocalDateTime.now), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, transformers = Seq(FilterTransformer(filterClause = "lastname='jonson'", subFeedTypeForValidation = subFeedType.typeSymbol.fullName)))
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")

    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO
      .getDataFrame()
      .select(col("rating"))
      .collect
    assert(r1.size == 1)
  }

  def testDeduplicateWithTransformerChangingSchema(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction(
      "dda",
      srcDO.id,
      tgtDO.id,
      transformers = Seq(SQLDfTransformer(code = Some("select lastname, firstname, rating as Rating from %{inputViewName}")))
    )
    val l1 = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    {
      val expected = Seq(
        ("doe",  "john",   5, Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter",  5, Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "Rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(("doe", "john", 10), ("pan", "peter", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())(context1)
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context2)

    {
      // note that we expect pan/peter/5 with updated refTimestamp even though all attributes stay the same
      val expected = Seq(
        ("doe",  "john",   10, Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter",  5,  Timestamp.valueOf(refTimestamp2)),
        ("hans", "muster", 5,  Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "Rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  def testDeduplicateWithSchemaEvolution(subFeedType: Type): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val colId = "id"
    val colValueOld = "old_value_column_string"
    val colValueNew = "new_value_column_decimal"
    def ts(str: String) = Timestamp.valueOf(LocalDateTime.parse(str.replace(" ", "T")))

    // initial deduplication while adding new column
    val df1 = Seq((1, "X", ts("2020-07-01 10:00")))
      .toDF(colId, colValueOld, Environment.capturedColumnName)

    val df2 = Seq((1, "A", 100))
      .toDF(colId, colValueOld, colValueNew)

    val dateTime1 = Timestamp.valueOf(LocalDateTime.of(2020, Month.AUGUST, 15, 10, 0, 0))
    val dfResult1 = DeduplicateAction
      .deduplicateDataFrame(Option(df1), Seq(colId), dateTime1, ignoreOldDeletedColumns = false, ignoreOldDeletedNestedColumns = true)(df2)

    // deduplicate again, using the new column
    val df3 = Seq((1, "B", 200))
      .toDF(colId, colValueOld, colValueNew)

    val dateTime2 = Timestamp.valueOf(LocalDateTime.of(2020, Month.AUGUST, 16, 10, 0, 0))
    val dfResult2 = DeduplicateAction
      .deduplicateDataFrame(Option(dfResult1), Seq(colId), dateTime2, ignoreOldDeletedColumns = false,
        ignoreOldDeletedNestedColumns = true)(df3)

    // the expected result is the final passed value with a captured column
    val dfExpected: GenericDataFrame = Seq((1, "B", 200, ts("2020-08-16 10:00")))
      .toDF(colId, colValueOld, colValueNew, Environment.capturedColumnName)

    assert(dfExpected.isEqual(dfResult2))
  }

  def testDeduplicateWithMergeMode(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id)
    val l1 = Seq(
      ("doe",  "john",   5),
      ("pan",  "peter",  5),
      ("hans", "muster", 5)
    ).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1).head

    {
      val expected = Seq(
        ("doe",  "john",   5, Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter",  5, Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(
      ("doe", "john",  10),
      ("pan", "peter", 5)
    ).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())(context2)
    action1.init(Seq(srcSubFeed))(context2.copy(phase = ExecutionPhase.Init)).head
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context2)

    {
      // note that we expect pan/peter/5 with updated refTimestamp even though all attributes stay the same
      val expected = Seq(
        ("doe",  "john",   10, Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter",  5,  Timestamp.valueOf(refTimestamp2)),
        ("hans", "muster", 5,  Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context2)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 3rd load with schema evolution
    val refTimestamp3 = LocalDateTime.now()
    val context3 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
    val l3 = Seq(
      ("doe", "john", 11)
    ).toDF("lastname", "firstname", "rating2")
    srcDO.writeDataFrame(l3, Seq())(context3)
    action1.init(Seq(srcSubFeed))(context3.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context3)

    {
      val expected = Seq(
        ("doe",  "john",   10, Some(11), Timestamp.valueOf(refTimestamp3)),
        ("pan",  "peter",  5,  None,     Timestamp.valueOf(refTimestamp2)),
        ("hans", "muster", 5,  None,     Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "rating2", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context3)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate load", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  def testDeduplicateWithMergeModeUpdateCapturedColumnOnlyWhenChanged(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, updateCapturedColumnOnlyWhenChanged = true)
    val l1 = Seq(
      ("doe",  "john",   Some(5)),
      ("pan",  "peter",  Some(5)),
      ("pan",  "peter2", None),
      ("pan",  "peter3", None),
      ("hans", "muster", Some(5))
    ).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1).head

    {
      val expected = Seq(
        ("doe",  "john",   Some(5), Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter",  Some(5), Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter2", None,    Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter3", None,    Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", Some(5), Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 =
      Seq(
        ("doe", "john",   Some(10)),
        ("pan", "peter",  Some(5)),
        ("pan", "peter2", Some(3)),
        ("pan", "peter3", None)
      ).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())(context2)
    action1.init(Seq(srcSubFeed))(context2.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context2)

    {
      // note that we expect pan/peter/5, pan/peter2/3 and pan/peter3/null with old refTimestamp because all attributes stay the same
      val expected = Seq(
        ("doe",  "john",   Some(10), Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter",  Some(5),  Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter2", Some(3),  Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter3", None,     Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", Some(5),  Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context2)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 3rd load with schema evolution
    val refTimestamp3 = LocalDateTime.now()
    val context3 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
    val l3 = Seq(("doe", "john", 11)).toDF("lastname", "firstname", "rating2")
    srcDO.writeDataFrame(l3, Seq())(context3)
    action1.init(Seq(srcSubFeed))(context3.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context3)

    {
      val expected = Seq(
        ("doe",  "john",   Some(10), Some(11), Timestamp.valueOf(refTimestamp3)),
        ("pan",  "peter",  Some(5),  None,     Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter2", Some(3),  None,     Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter3", None,     None,     Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", Some(5),  None,     Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", "rating2", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context3)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate load", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  def testDeduplicateWithMergeModeSourceTimestampColumn(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    val srcTs1 = ts("2024-01-01 10:00:00")
    val srcTs2 = ts("2024-01-02 10:00:00")
    val srcTsLate = ts("2023-12-01 10:00:00")

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, sourceTimestampColumn = Some(sourceTsCol))
    val l1 = Seq(
      ("doe", "john",   Some(5), Some(srcTs1)),
      ("pan", "peter",  Some(5), Some(srcTs1)),
      ("nn",  "nobody", Some(5), None) // no source timestamp -> fallback to the runs reference timestamp
    ).toDF("lastname", "firstname", "rating", sourceTsCol)
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1)

    {
      val expected = Seq(
        ("doe", "john",   Some(5), srcTs1),
        ("pan", "peter",  Some(5), srcTs1),
        ("nn",  "nobody", Some(5), Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating", Environment.capturedColumnName)
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st load with sourceTimestampColumn", Seq())(actual)(expected)
      assert(resultat)
      // the source timestamp column itself is not written to the output
      assert(!actual.columns.map(_.toLowerCase).contains(sourceTsCol))
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(
      ("doe",  "john",   Some(10), Some(srcTs2)),    // changed in the source system
      ("pan",  "peter",  Some(99), Some(srcTsLate)), // arriving late, must not be applied
      ("hans", "muster", Some(5),  Some(srcTs2))     // new record
    ).toDF("lastname", "firstname", "rating", sourceTsCol)
    srcDO.writeDataFrame(l2, Seq())(context2)
    action1.init(Seq(srcSubFeed))(context2.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context2)

    {
      val expected = Seq(
        ("doe",  "john",   Some(10), srcTs2),
        ("pan",  "peter",  Some(5),  srcTs1), // late record ignored, previous version kept
        ("nn",   "nobody", Some(5),  Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", Some(5),  srcTs2)
      )
        .toDF("lastname", "firstname", "rating", Environment.capturedColumnName)
      val actual = tgtDO.getDataFrame()(context2)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 2nd load with sourceTimestampColumn", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  def testDeduplicateWithMergeModeSourceTimestampColumnUpdateOnlyWhenChanged(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    val srcTs1 = ts("2024-01-01 10:00:00")
    val srcTs2 = ts("2024-01-02 10:00:00")

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, sourceTimestampColumn = Some(sourceTsCol),
      updateCapturedColumnOnlyWhenChanged = true)
    val l1 = Seq(
      ("doe", "john",  5, srcTs1),
      ("pan", "peter", 5, srcTs1)
    ).toDF("lastname", "firstname", "rating", sourceTsCol)
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1)

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(
      ("doe", "john",  10, srcTs1), // the source system did not increase its timestamp, the record is not updated
      ("pan", "peter", 5,  srcTs2)  // the source system marks the record as changed
    ).toDF("lastname", "firstname", "rating", sourceTsCol)
    srcDO.writeDataFrame(l2, Seq())(context2)
    action1.init(Seq(srcSubFeed))(context2.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(ScalaSubFeed(None, "src1", Seq())))(context2)

    {
      val expected = Seq(
        ("doe", "john",  5, srcTs1),
        ("pan", "peter", 5, srcTs2)
      )
        .toDF("lastname", "firstname", "rating", Environment.capturedColumnName)
      val actual = tgtDO.getDataFrame()(context2)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 2nd load with sourceTimestampColumn", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  def testDeduplicateWithMergeModeSchemaEvolution(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction(
      "dda",
      srcDO.id,
      tgtDO.id,
      transformers = Seq(SQLDfTransformer(code = Some("select lastname, firstname, rating as rating2 from %{inputViewName}")))
    )
    val l1 = Seq(
      ("doe",  "john",   5),
      ("pan",  "peter",  5),
      ("hans", "muster", 5)
    ).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(context1)
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    {
      val expected = Seq(
        ("doe",  "john",   5, Timestamp.valueOf(refTimestamp1)),
        ("pan",  "peter",  5, Timestamp.valueOf(refTimestamp1)),
        ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating2", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context1)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(
      ("doe", "john",  10),
      ("pan", "peter", 5)
    ).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())(context2)
    action1.init(Seq(srcSubFeed))(context2.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context2)

    {
      // note that we expect pan/peter/5 with updated refTimestamp even though all attributes stay the same
      val expected = Seq(
        ("doe",  "john",   10, Timestamp.valueOf(refTimestamp2)),
        ("pan",  "peter",  5,  Timestamp.valueOf(refTimestamp2)),
        ("hans", "muster", 5,  Timestamp.valueOf(refTimestamp1))
      )
        .toDF("lastname", "firstname", "rating2", "dl_ts_captured")
      val actual = tgtDO.getDataFrame()(context2)
      val resultat = expected.isEqual(actual)
      if (!resultat) printFailedTestResultGdf("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }
}
