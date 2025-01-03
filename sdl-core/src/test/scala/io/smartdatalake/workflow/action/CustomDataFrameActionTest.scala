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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.{InstanceRegistry, SdlConfigObject}
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.TestUtil.createParquetDataObject
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskSkippedDontStopWarning
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.{CustomMode, CustomModeLogic, ExecutionModeResult, PartitionDiffMode}
import io.smartdatalake.workflow.action.expectation.{CompletenessExpectation, TransferRateExpectation}
import io.smartdatalake.workflow.action.generic.transformer.SQLDfsTransformer
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfsTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.expectation.{CountExpectation, ExpectationScope, SQLExpectation}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, InitSubFeed}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.{Files, Path => NioPath}

class CustomDataFrameActionTest extends FunSuite with BeforeAndAfter {
  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  private var tempDir: NioPath = _
  private var tempPath: String = _

  before {
    instanceRegistry.clear()
    tempDir = Files.createTempDirectory("test")
    tempPath = tempDir.toAbsolutePath.toString
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("spark action with custom transformation class to load multiple sources into multiple targets") {
    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val tgtDO1 = MockDataObject("tgt1", primaryKey = Some(Seq("lastname", "firstname"))).register
    val tgtDO2 = MockDataObject("tgt2", primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDfsTransformer(
      className = classOf[TestDfsTransformerIncrement].getName,
      options = Map("increment1" -> "1"), runtimeOptions = Map("increment2" -> "runId")
    )

    val action1 = CustomDataFrameAction("action1", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), transformers = Seq(customTransformerConfig))
    instanceRegistry.register(action1)

    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l1, Seq())
    srcDO2.writeSparkDataFrame(l1, Seq())

    val tgtSubFeeds = action1.exec(Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq())))(contextExec)
    assert(tgtSubFeeds.size == 2)
    assert(tgtSubFeeds.map(_.dataObjectId) == Seq(tgtDO1.id, tgtDO2.id))

    val r1 = tgtDO1.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 6) // should be increased by 1 through TestDfTransformer

    // same for the second dataframe
    val r2 = tgtDO2.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 6)
  }

  test("spark action with recursive input") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val tgtDO1 = MockDataObject("tgt1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDfsTransformer(className = classOf[TestDfsTransformerRecursive].getName)

    // first action to create output table as it does not exist yet
    val action1 = CustomDataFrameAction("action1", List(srcDO1.id), List(tgtDO1.id), transformers = Seq(customTransformerConfig))
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l1, Seq())

    val tgtSubFeedsNonRecursive = action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))(contextExec)
    assert(tgtSubFeedsNonRecursive.map(_.dataObjectId) == Seq(tgtDO1.id))

    val r1 = tgtDO1.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1 == Seq(6)) // should be increased by 1 through TestDfTransformer

    // second action to test recursive inputs
    val action2 = CustomDataFrameAction("action1", List(srcDO1.id), List(tgtDO1.id), transformers = Seq(customTransformerConfig), recursiveInputIds = List(tgtDO1.id))

    val tgtSubFeedsRecursive = action2.exec(Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "tgt1", Seq())))(contextExec)
    assert(tgtSubFeedsRecursive.size == 1) // still 1 as recursive inputs are handled separately

    val r2 = tgtDO1.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2 == Seq(11)) // Record should be updated a second time with data from tgt1

  }

  test("copy with partition diff execution mode 2 iterations") {

    // setup DataObjects
    val srcDO = MockDataObject("src1", partitions = Seq("type")).register
    val tgtDO = MockDataObject("tgt1", partitions = Seq("type"), primaryKey = Some(Seq("type", "lastname", "firstname"))).register

    // prepare action
    val customTransformerConfig = ScalaClassSparkDfsTransformer(className = classOf[TestDfsTransformerDummy].getName)
    val action = CustomDataFrameAction("a1", Seq(srcDO.id), Seq(tgtDO.id), transformers = Seq(customTransformerConfig), executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", Seq())

    // prepare & start first load
    val l1 = Seq(("A", "doe", "john", 5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type" -> "A")))
    srcDO.writeSparkDataFrame(l1, l1PartitionValues) // prepare testdata
    action.init(Seq(srcSubFeed))
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed))(contextExec).head

    // check first load
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getSparkDataFrame().count() == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)

    // prepare & start 2nd load
    action.reset
    val l2 = Seq(("B", "pan", "peter", 11)).toDF("type", "lastname", "firstname", "rating")
    val l2PartitionValues = Seq(PartitionValues(Map("type" -> "B")))
    srcDO.writeSparkDataFrame(l2, l2PartitionValues) // prepare testdata
    assert(srcDO.getSparkDataFrame().count() == 2) // note: this needs spark.sql.sources.partitionOverwriteMode=dynamic, otherwise the whole table is overwritten
    action.init(Seq(srcSubFeed))
    val tgtSubFeed2 = action.exec(Seq(srcSubFeed))(contextExec).head

    // check 2nd load
    assert(tgtSubFeed2.dataObjectId == tgtDO.id)
    assert(tgtSubFeed2.partitionValues.toSet == l2PartitionValues.toSet)
    assert(tgtDO.getSparkDataFrame().count() == 2)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet ++ l2PartitionValues.toSet)
  }

  test("copy with partition diff execution mode and mainInput/Output") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1", partitions = Seq("type")).register
    val srcDO2 = MockDataObject("src2", partitions = Seq("type")).register
    val srcDO3 = MockDataObject("src3").register
    val tgtDO1 = MockDataObject("tgt1", partitions = Seq("type")).register
    val tgtDO2 = MockDataObject("tgt2", partitions = Seq("type")).register
    val tgtDO3 = MockDataObject("tgt3").register

    // prepare action
    val customTransformerConfig = ScalaClassSparkDfsTransformer(className = classOf[TestDfsTransformerDummy].getName)
    val action = CustomDataFrameAction("a1", Seq(srcDO1.id, srcDO2.id, srcDO3.id), Seq(tgtDO1.id, tgtDO2.id, tgtDO3.id), transformers = Seq(customTransformerConfig)
      , mainInputId = Some("src1"), mainOutputId = Some("tgt1"), executionMode = Some(PartitionDiffMode()))
    val srcSubFeed1 = InitSubFeed("src1", Seq())
    val srcSubFeed2 = InitSubFeed("src2", Seq())
    val srcSubFeed3 = InitSubFeed("src3", Seq())

    // prepare & start first load
    val l1 = Seq(("A", "doe", "john", 5)).toDF("type", "lastname", "firstname", "rating")
    val l2 = Seq(("A", "doe", "john", 5), ("B", "doe", "john", 5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type" -> "A")))
    val l2PartitionValues = Seq(PartitionValues(Map("type" -> "A")), PartitionValues(Map("type" -> "B")))
    srcDO1.writeSparkDataFrame(l1, l1PartitionValues)
    srcDO2.writeSparkDataFrame(l2, l2PartitionValues)
    srcDO3.writeSparkDataFrame(l2, Seq()) // src3 is not partitioned
    action.init(Seq(srcSubFeed1, srcSubFeed2, srcSubFeed3))
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed1, srcSubFeed2, srcSubFeed3))(contextExec).head

    // check load
    assert(tgtSubFeed1.dataObjectId == tgtDO1.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO1.getSparkDataFrame().count() == 1) // partition type=A is missing
    assert(tgtDO1.listPartitions.toSet == l1PartitionValues.toSet)
    assert(tgtDO2.getSparkDataFrame().count() == 1) // only partitions according to srcDO1 read
    assert(tgtDO2.listPartitions.toSet == l1PartitionValues.toSet)
    assert(tgtDO3.getSparkDataFrame().count() == 2) // all records read because not partitioned
  }

  test("copy load with multiple transformations and multiple outputs from sql code") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val intDO1 = MockDataObject("int1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register
    val tgtDO1 = MockDataObject("tgt1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register
    val tgtDO2 = MockDataObject("tgt2", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start load
    // note that src2 is passed on to customTransformerConfig2, even if it's not re-defined in customTransformerConfig1
    // note that intermediate dataframe int1 is used as output, and tgt1 is a transformer output but not used as action output
    val customTransformerConfig1 = SQLDfsTransformer(code = Map(intDO1.id.id -> "select * from src1 where rating = 5"))
    val customTransformerConfig2 = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from int1", tgtDO2.id.id -> "select * from src2 where rating = 3"))
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(intDO1.id, tgtDO2.id), transformers = Seq(customTransformerConfig1, customTransformerConfig2))
    instanceRegistry.register(action1)
    val l = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l, Seq())
    srcDO2.writeSparkDataFrame(l, Seq())
    val srcSubFeeds = Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq()))
    val tgtSubFeed = action1.exec(srcSubFeeds)(contextExec).head

    val r1 = intDO1.getSparkDataFrame()
      .select($"lastname")
      .as[String].collect().toSeq
    assert(r1.size == 1) // only one record has rating 5 (see where condition)
    assert(r1.head == "jonson")

    val r2 = tgtDO2.getSparkDataFrame()
      .select($"lastname")
      .as[String].collect().toSeq
    assert(r2.size == 1) // only one record has rating 5 (see where condition)
    assert(r2.head == "doe")

  }

  test("copy load with transformer, 2 inputs and skip condition") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val tgtDO1 = MockDataObject("tgt1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare
    val customTransformerConfig = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1 union all select * from src2"))
    val l1 = Seq(("jonson", "rob", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l1, Seq())
    val l2 = Seq(("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO2.writeSparkDataFrame(l2, Seq())

    // condition: skip only if both input subfeeds are skipped
    val executionCondition = Some(Condition("!inputSubFeeds.src1.isSkipped or !inputSubFeeds.src2.isSkipped"))

    // skip if both subfeeds skipped
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id), transformers = Seq(customTransformerConfig), executionCondition = executionCondition)
    instanceRegistry.register(action1)
    val srcSubFeed1 = SparkSubFeed(None, "src1", Seq(), isSkipped = true)
    val srcSubFeed2 = SparkSubFeed(None, "src2", Seq(), isSkipped = true)
    val tgtSubFeed1 = SparkSubFeed(None, "tgt1", Seq(), isSkipped = true)
    action1.preInit(Seq(srcSubFeed1, srcSubFeed2), Seq())
    intercept[TaskSkippedDontStopWarning[_]](action1.preExec(Seq(srcSubFeed1, srcSubFeed2))(contextExec))
    action1.postExec(Seq(srcSubFeed1, srcSubFeed2), Seq(tgtSubFeed1))(contextExec)

    // dont skip if one subfeed skipped
    val action2 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id),
      transformers = Seq(customTransformerConfig), executionCondition = executionCondition)
    instanceRegistry.register(action2)
    val srcSubFeed3 = SparkSubFeed(None, "src1", Seq(), isSkipped = true)
    val srcSubFeed4 = SparkSubFeed(None, "src2", Seq(), isSkipped = false)
    action2.preInit(Seq(srcSubFeed3, srcSubFeed4), Seq()) // no exception
    action2.preExec(Seq(srcSubFeed3, srcSubFeed4)) // no exception
  }

  // data from skipped input is nevertheless read, after decision to execute Action is made (e.g. subFeed,isSkipped will be set to false).
  test("copy load with transformer, a regular and a skipped input, skipped input is reset after decision to execute Action was made") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val tgtDO1 = MockDataObject("tgt1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare
    val customTransformerConfig = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1 union all select * from src2"))
    val l1 = Seq(("jonson", "rob", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l1, Seq())
    val l2 = Seq(("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO2.writeSparkDataFrame(l2, Seq())

    // condition: always execute
    val executionCondition = Some(Condition("true"))

    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id), transformers = Seq(customTransformerConfig), executionCondition = executionCondition)
    instanceRegistry.register(action1)
    val srcSubFeed1 = SparkSubFeed(None, "src1", Seq(), isSkipped = false)
    val srcSubFeed2 = SparkSubFeed(None, "src2", Seq(), isSkipped = true)
    action1.preInit(Seq(srcSubFeed1, srcSubFeed2), Seq())
    action1.preExec(Seq(srcSubFeed1, srcSubFeed2))(contextExec)
    action1.exec(Seq(srcSubFeed1, srcSubFeed2))(contextExec)

    // check record from l1 and l2 is present (union all in SQL transformer)
    assert(tgtDO1.getSparkDataFrame().count() == 2)
  }

  test("date to month aggregation with partition value transformation") {

    // setup DataObjects
    val srcDO = MockDataObject("src1", partitions = Seq("dt")).register
    val tgtDO1 = MockDataObject("tgt1", partitions = Seq("mt")).register

    // prepare & simulate load (init only)
    val customTransformerConfig = ScalaClassSparkDfsTransformer(className = classOf[TestDfsTransformerPartitionValues].getName)
    val action1 = CustomDataFrameAction("ca", List(srcDO.id), List(tgtDO1.id), transformers = Seq(customTransformerConfig))
    val l1 = Seq(("20100101", "jonson", "rob", 5), ("20100103", "doe", "bob", 3)).toDF("dt", "lastname", "firstname", "rating")
    val srcPartitionValues = Seq(PartitionValues(Map("dt" -> "20100101")), PartitionValues(Map("dt" -> "20100103")))
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", srcPartitionValues)
    val tgtSubFeed = action1.init(Seq(srcSubFeed))(contextExec).head.asInstanceOf[SparkSubFeed]

    val expectedPartitionValues = Seq(PartitionValues(Map("mt" -> "201001")))
    assert(tgtSubFeed.partitionValues == expectedPartitionValues)
    assert(tgtSubFeed.dataFrame.get.schema.columns.contains("mt"))
  }

  test("custom execution mode result options") {
    val srcDO = MockDataObject("src1").register
    val tgtDO1 = MockDataObject("tgt1").register

    // prepare & simulate load (init only)
    val customTransformerConfig = ScalaClassSparkDfsTransformer(className = classOf[TestDfsTransformerOptions].getName)
    val customExecutionMode = CustomMode(className = classOf[TestResultOptionsCustomMode].getName)
    val action1 = CustomDataFrameAction("ca", List(srcDO.id), List(tgtDO1.id), transformers = Seq(customTransformerConfig), executionMode = Some(customExecutionMode))
    val l1 = Seq(("20100101", "jonson", "rob", 5), ("20100103", "doe", "bob", 3)).toDF("dt", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(contextExec)
  }

  test("copy load detect no-data warning from SparkPlan on main output") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val tgtDO1 = createParquetDataObject("tgt1")
    val tgtDO2 = createParquetDataObject("tgt2")

    // prepare & start load
    val customTransformerConfig1 = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1"))
    val customTransformerConfig2 = SQLDfsTransformer(code = Map(tgtDO2.id.id -> "select * from src2"))
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), mainInputId = Some(srcDO1.id), mainOutputId = Some(tgtDO1.id), transformers = Seq(customTransformerConfig1, customTransformerConfig2))
    instanceRegistry.register(action1)
    val l = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l.where(lit(false)), Seq()) // empty DataFrame on main input -> main output
    srcDO2.writeSparkDataFrame(l, Seq())
    val srcSubFeeds = Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq()))
    intercept[NoDataToProcessWarning](action1.exec(srcSubFeeds)(contextExec).head)

    // check that tgt1 is empty
    // getSparkDataFrame will throw IllegalArgumentException because it no files have been written to this DataObject...
    intercept[IllegalArgumentException](tgtDO1.getSparkDataFrame())
    // check that tgt2 is written nevertheless!
    assert(tgtDO2.getSparkDataFrame().count() == 2)
  }

  test("copy load ignore no-data warning from SparkPlan if not main output ") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val tgtDO1 = createParquetDataObject("tgt1")
    val tgtDO2 = createParquetDataObject("tgt2")

    // prepare & start load
    val customTransformerConfig1 = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1"))
    val customTransformerConfig2 = SQLDfsTransformer(code = Map(tgtDO2.id.id -> "select * from src2"))
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), mainInputId = Some(srcDO1.id), mainOutputId = Some(tgtDO1.id), transformers = Seq(customTransformerConfig1, customTransformerConfig2))
    instanceRegistry.register(action1)
    val l = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l, Seq())
    srcDO2.writeSparkDataFrame(l.where(lit(false)), Seq()) // empty DataFrame on non-main-input -> non-main-output
    val srcSubFeeds = Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq()))
    action1.exec(srcSubFeeds)(contextExec).head

    // check that tgt1 is not empty
    assert(tgtDO1.getSparkDataFrame().count == 2)
    // check that tgt2 is empty
    // getSparkDataFrame will throw IllegalArgumentException because it no files have been written to this DataObject...
    intercept[IllegalArgumentException](tgtDO2.getSparkDataFrame())
  }

  test("copy load with constraints and expectations non-main input no_data") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1", expectations = Seq(
      CountExpectation(name = "count", expectation = Some(">= 1"))
    )).register
    val srcDO2 = MockDataObject("src2", expectations = Seq(
      CountExpectation(name = "count", expectation = Some("= 0")),
      CountExpectation(name = "countAll", expectation = Some("= 0"), scope = ExpectationScope.All)
    )).register
    val tgtDO1 = MockDataObject("tgt1", expectations = Seq(
      CountExpectation(expectation = Some(">= 1")),
      SQLExpectation("tgt1AvgRatingGt1", Some("avg rating should be bigger than 1"), "avg(rating)", Some("> 1")),
    )).register
    val tgtDO2 = MockDataObject("tgt2", expectations = Seq(
      CountExpectation(expectation = Some("= 0")),
      SQLExpectation("tgt2AvgRatingGt1", Some("avg rating should be bigger than 1"), "avg(rating)", Some("> 1")),
    )).register

    // prepare & start load
    val customTransformerConfig1 = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1"))
    val customTransformerConfig2 = SQLDfsTransformer(code = Map(tgtDO2.id.id -> "select * from src2"))
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), mainInputId = Some(srcDO1.id), mainOutputId = Some(tgtDO1.id),
      transformers = Seq(customTransformerConfig1, customTransformerConfig2),
      expectations = Seq(TransferRateExpectation(), CompletenessExpectation(expectation = None))
    )
    instanceRegistry.register(action1)
    val dfInput = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")

    // run with src2 empty (non-main input)
    srcDO1.writeSparkDataFrame(dfInput, Seq())
    srcDO2.writeSparkDataFrame(dfInput.where(lit(false)), Seq())
    val srcSubFeeds = Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq()))
    val tgtSubFeeds = action1.exec(srcSubFeeds)(contextExec)
    val tgtSubFeed1 = tgtSubFeeds.find(_.dataObjectId == tgtDO1.id).get
    val tgtSubFeed2 = tgtSubFeeds.find(_.dataObjectId == tgtDO2.id).get

    // check expectation value in metrics
    val metrics1 = tgtSubFeed1.metrics.get
    assert(metrics1 == Map("count" -> 2, "countAll" -> 2, "records_written" -> 2, "tgt1AvgRatingGt1" -> 4.0, "count#src1" -> 2, "count#mainInput" -> 2, "countAll#src1" -> 2, "countAll#mainInput" -> 2, "pctTransfer" -> 1.0, "pctComplete" -> 1.0))
    val metrics2 = tgtSubFeed2.metrics.get
    assert(metrics2 == Map("count" -> 0, "tgt2AvgRatingGt1" -> None, "count#src2" -> 0, "countAll#src2" -> 0, "no_data" -> true))
  }

  test("copy load with constraints and expectations main input no_data") {

    // setup DataObjects
    val srcDO1 = MockDataObject("src1", expectations = Seq(
      CountExpectation(name = "count", expectation = Some("= 0"))
    )).register
    val srcDO2 = MockDataObject("src2", expectations = Seq(
      CountExpectation(name = "count", expectation = Some("= 2")),
      CountExpectation(name = "countAll", expectation = Some("= 2"), scope = ExpectationScope.All)
    )).register
    val tgtDO1 = MockDataObject("tgt1", expectations = Seq(
      CountExpectation(expectation = Some("= 0")),
      SQLExpectation("tgt1AvgRatingGt1", Some("avg rating should be bigger than 1"), "avg(rating)", Some("> 1")),
    )).register
    val tgtDO2 = MockDataObject("tgt2", expectations = Seq(
      CountExpectation(expectation = Some("= 2")),
      SQLExpectation("tgt2AvgRatingGt1", Some("avg rating should be bigger than 1"), "avg(rating)", Some("> 1")),
    )).register

    // prepare & start load
    val customTransformerConfig1 = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1"))
    val customTransformerConfig2 = SQLDfsTransformer(code = Map(tgtDO2.id.id -> "select * from src2"))
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), mainInputId = Some(srcDO1.id), mainOutputId = Some(tgtDO1.id),
      transformers = Seq(customTransformerConfig1, customTransformerConfig2),
      expectations = Seq(TransferRateExpectation(), CompletenessExpectation(expectation = None))
    )
    instanceRegistry.register(action1)
    val dfInput = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")

    // run with src1 empty (main input)
    srcDO1.writeSparkDataFrame(dfInput.where(lit(false)), Seq())
    srcDO2.writeSparkDataFrame(dfInput, Seq())
    val srcSubFeeds = Seq(SparkSubFeed(None, "src1", Seq()), SparkSubFeed(None, "src2", Seq()))
    val tgtSubFeeds = action1.exec(srcSubFeeds)(contextExec)
    val tgtSubFeed1 = tgtSubFeeds.find(_.dataObjectId == tgtDO1.id).get
    val tgtSubFeed2 = tgtSubFeeds.find(_.dataObjectId == tgtDO2.id).get

    // check expectation value in metrics
    val metrics1 = tgtSubFeed1.metrics.get
    assert(metrics1 == Map("count" -> 0, "countAll" -> 0, "tgt1AvgRatingGt1" -> None, "count#src1" -> 0, "count#mainInput" -> 0, "countAll#src1" -> 0, "countAll#mainInput" -> 0, "pctTransfer" -> "null", "pctComplete" -> "null", "no_data" -> true))
    val metrics2 = tgtSubFeed2.metrics.get
    assert(metrics2 == Map("count" -> 2, "tgt2AvgRatingGt1" -> 4.0, "count#src2" -> 2, "countAll#src2" -> 2, "records_written" -> 2))

  }
}


class TestDfsTransformerIncrement extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    import session.implicits._
    Map(
      "tgt1" -> dfs("src1").withColumn("rating", $"rating" + options("increment1").toInt)
      , "tgt2" -> dfs("src2").withColumn("rating", $"rating" + options("increment2").toInt)
    )
  }
}

class TestDfsTransformerRecursive extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    import session.implicits._
    if (!dfs.contains("tgt1")) {
      // first run without recursive inputs
      Map(
        "tgt1" -> dfs("src1").withColumn("rating", $"rating" + 1)
      )
    }
    else {
      // second run with recursive inputs
      Map(
        "tgt1" -> dfs("tgt1").withColumn("rating", $"rating" + 5)
      )
    }

  }
}

class TestDfsTransformerDummy extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    // one to one...
    dfs.map { case (id, df) => (id.replaceFirst("src", "tgt"), df) }
  }
}

// aggregate date partitions to month partitions
class TestDfsTransformerPartitionValues extends CustomDfsTransformer {

  import org.apache.spark.sql.functions._

  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    import session.implicits._
    val dfTgt = dfs("src1").withColumn("mt", substring($"dt", 1, 6))
    Map("tgt1" -> dfTgt)
  }

  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] = {
    Some(partitionValues.map(pv => (pv, PartitionValues(Map("mt" -> pv("dt").toString.take(6))))).toMap)
  }
}


class TestDfsTransformerFilterDummy extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    // return only the first df sorted by ID
    dfs.toSeq.sortBy(_._1).take(1).map { case (id, df) => (id.replaceFirst("src", "tgt"), df) }.toMap
  }
}

class TestResultOptionsCustomMode extends CustomModeLogic {
  override def apply(options: Map[String, String], actionId: SdlConfigObject.ActionId, input: DataObject, output: DataObject, givenPartitionValues: Seq[Map[String, String]], context: ActionPipelineContext): Option[ExecutionModeResult] = {
    Some(ExecutionModeResult(options = Map("testOption" -> "test")))
  }
}

class TestDfsTransformerOptions extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    assert(options.get("testOption").contains("test"))
    // one to one...
    dfs.map { case (id, df) => (id.replaceFirst("src", "tgt"), df) }
  }
}
