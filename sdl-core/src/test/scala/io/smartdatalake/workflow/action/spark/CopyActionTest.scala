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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeGenericOptions}
import io.smartdatalake.testutils.spark.dataset.Collection
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.LogUtil.getRootCause
import io.smartdatalake.workflow.action.executionMode.{FileIncrementalMoveMode, PartitionDiffMode}
import io.smartdatalake.workflow.action.expectation.{CompletenessExpectation, TransferRateExpectation}
import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfTransformer
import io.smartdatalake.workflow.action.generic.transformer.{ColumnsTransformer, FilterTransformer, SQLDfTransformer, ScalaClassGenericDfTransformer}
import io.smartdatalake.workflow.action.spark.transformer.{ScalaCodeSparkDfTransformer, SparkRepartitionTransformer}
import io.smartdatalake.workflow.action.{CopyAction, NoDataToProcessWarning}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation._
import io.smartdatalake.workflow.dataobject.generic.Constraint
import io.smartdatalake.workflow._
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => NioPath}

class CopyActionTest extends AnyFunSuite with BeforeAndAfter {

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  val functions: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(SparkSubFeed.subFeedType)
  import functions.implicits._

  private var tempDir: NioPath = _
  private var tempPath: String = _

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
    tempDir = Files.createTempDirectory("test")
    tempPath = tempDir.toAbsolutePath.toString
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("copy load with custom transformation class and incremental move mode (delete)") {

    val srcDO = ParquetFileDataObject("src1", tempPath + s"/src1", filenameColumn = Some("_filename"))
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtDO = MockSparkDataObject("tgt1").register

    // prepare & start load
    val customTransformerConfig = ScalaClassGenericDfTransformer(className = classOf[TestDfTransformer].getName)
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig), executionMode = Some(FileIncrementalMoveMode()))
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    assert(srcDO.getFileRefs(Seq()).nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "doe")), PartitionValues(Map("lastname" -> "jonson"))))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check output
    val r1 = tgtDO.getDataFrame()
      .select("rating").collect[Int].toSet
    assert(r1 == Set(4, 6)) // should be increased by 1 through TestDfTransformer

    // check input deleted by incremental move mode
    assert(srcDO.getFileRefs(Seq()).isEmpty)
  }

  test("copy load with custom transformation from code string, incremental move mode (archive) and schema file test") {

    // define custom transformation
    val codeStr = """
      import org.apache.spark.sql.{DataFrame, SparkSession}
      def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
        import session.implicits._
        df.withColumn("rating", $"rating" + 1)
      }
      // return as function
      transform _
    """
    val customTransformerConfig = ScalaCodeSparkDfTransformer(code = Some(codeStr))

    val srcDO = ParquetFileDataObject("src1", tempPath + s"/src1")
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtDO = MockSparkDataObject("tgt1").register

    // prepare data
    val executionMode = FileIncrementalMoveMode(archivePath = Some("archive"))
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig), executionMode = Some(executionMode))
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())

    // start load
    val srcFiles = srcDO.getFileRefs(Seq()).map(_.fullPath)
    assert(srcFiles.nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))

    // check result
    val r1 = tgtDO.getDataFrame().select("rating").collect[Int]
    assert(r1.size == 1)
    assert(r1.head == 6) // should be increased by 1 through TestDfTransformer
    // check input archived by incremental move mode
    assert(srcDO.getFileRefs(Seq()).isEmpty)
    val srcDOArchived = ParquetFileDataObject("src1", tempPath + s"/src1/archive")
    assert(srcDOArchived.getFileRefs(Seq()).nonEmpty)

    // start second load without new files - schema should be present because of schema file
    intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec).head)
  }

  test("copy load incremental move mode (archive) V1 DataSource") {

    val srcDO = XmlFileDataObject("src1", tempPath + s"/src1", xmlOptions = Some(Map("rowTag" -> "entry")))
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtDO = MockSparkDataObject("tgt1")
    instanceRegistry.register(tgtDO)

    // prepare data
    val executionMode = FileIncrementalMoveMode(archivePath = Some("archive"))
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, executionMode = Some(executionMode))
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())

    // start load
    val srcFiles = srcDO.getFileRefs(Seq()).map(_.fullPath)
    assert(srcFiles.nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))

    // check input archived by incremental move mode
    assert(srcDO.getFileRefs(Seq()).isEmpty)
    val srcDOArchived = XmlFileDataObject("src1", tempPath + s"/src1/archive")
    assert(srcDOArchived.getFileRefs(Seq()).nonEmpty)

    // start second load without new files - schema should be present because of schema file
    intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec).head)
  }

  test("copy load with transformation from sql code and constraint and expectation - Generic DataFrame observations") {
    // if approximate=false, metrics are calculated as generic calculated metrics (because Spark does not support count_distinct as observations aggregate expression)
    testCopyLoadWithTransformationAndConstraintsAndExpectation(approximateUniqueConstraint = false)
  }

  test("copy load with transformation from sql code and constraint and expectation - Spark DataFrame observations") {
    // if approximateUniqueConstraint=true, metrics are calculated as Spark DataFrame observations
    testCopyLoadWithTransformationAndConstraintsAndExpectation(approximateUniqueConstraint = true)
  }

  def testCopyLoadWithTransformationAndConstraintsAndExpectation(approximateUniqueConstraint: Boolean): Unit = {

    // setup DataObjects
    val srcDO = MockSparkDataObject(
      "src1",
      expectations = Seq(
        CountExpectation(expectation = Some(">= 1")),
        CountExpectation(name = "countAll", expectation = Some("= 2"), scope = ExpectationScope.All)
      )
    ).register
    val tgtDO = MockSparkDataObject(
      "tgt1",
      partitions = Seq("lastname"),
      primaryKey = Some(Seq("lastname", "firstname")),
      constraints = Seq(Constraint("firstnameNotNull", Some("firstname should be non empty"), "firstname is not null")),
      expectations = Seq(
        CountExpectation(expectation = Some(">= 1")),
        SQLExpectation("avgRatingGt1", Some("avg rating should be bigger than 1"), "avg(rating)", Some("> 1")),
        SQLFractionExpectation(
          "pctBob",
          countConditionExpression = "firstname = 'bob'",
          expectation = Some("= 0")
        ), // because we only select Rob and not Bob...
        CountExpectation(name = "countPerPartition", expectation = Some(">= 1"), scope = ExpectationScope.JobPartition),
        CountExpectation(name = "countAll", expectation = Some(">= 1"), scope = ExpectationScope.All),
        SQLQueryExpectation(
          name = "countOfPartitionsWith1Record",
          code = "select count(*) from (select lastname from %{inputViewName} group by lastname having count(*) = 1)",
          scope = ExpectationScope.All
        ),
        SQLExpectation("resultNull", Some("dont fail if result is null"), "null", Some("> 1")),
        UniqueKeyExpectation("primaryKey", approximate = approximateUniqueConstraint)
      )
    ).register

    // prepare & start load with positive constraint and expectation evaluation
    val customTransformerConfig1 = SQLDfTransformer(name = "sql1", code = Some("select * from %{inputViewName} where rating = 5"))
    val customTransformerConfig2 = SQLDfTransformer(
      name = "sql2",
      code = Some("select * from %{inputViewName} where rating = 5")
    ) // test multiple transformers - it doesnt matter if they do the same.
    val action1 = CopyAction(
      "ca",
      srcDO.id,
      tgtDO.id,
      transformers = Seq(customTransformerConfig1, customTransformerConfig2),
      expectations = Seq(TransferRateExpectation(), CompletenessExpectation(expectation = None))
    )
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed1 = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)

    // check result
    val r1 = tgtDO.getDataFrame().select("lastname").collect[String]
    assert(r1 == Seq("jonson")) // only one record has rating 5 (see where condition)

    // check expectation value in metrics
    val metrics1 = tgtSubFeed1.metrics.get
    assert(
      metrics1 == Map(
        "count"                        -> 1,
        "avgRatingGt1"                 -> 5.0,
        "pctBob"                       -> 0.0,
        "countPerPartition#jonson"     -> 1,
        "count#src1"                   -> 1,
        "count#mainInput"              -> 1,
        "countAll#src1"                -> 2,
        "countAll#mainInput"           -> 2,
        "pctTransfer"                  -> 1.0,
        "countAll"                     -> 1,
        "countAll#src1"                -> 2,
        "countAll#mainInput"           -> 2,
        "pctComplete"                  -> 0.5,
        "countOfPartitionsWith1Record" -> 1,
        "resultNull"                   -> None,
        "primaryKey"                   -> 1.0,
        "records_written"              -> 1
      )
    )

    // overwrite src with 2 record to process
    val l2 = Seq(("dau", "peter", 5), ("dau", "pan", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l2, Seq())
    action1.reset
    val tgtSubFeed2 = action1.exec(Seq(srcSubFeed))(contextExec).head

    // check expectation value in metrics - countAll should be 2 now, but count should stay 1
    val metrics2 = tgtSubFeed2.metrics.get
    assert(
      metrics2 == Map(
        "count"                        -> 2,
        "avgRatingGt1"                 -> 5.0,
        "pctBob"                       -> 0.0,
        "countPerPartition#dau"        -> 2,
        "count#src1"                   -> 2,
        "count#mainInput"              -> 2,
        "pctTransfer"                  -> 1.0,
        "countAll"                     -> 3,
        "countAll#src1"                -> 2,
        "countAll#mainInput"           -> 2,
        "pctComplete"                  -> 1.5,
        "countOfPartitionsWith1Record" -> 1,
        "resultNull"                   -> None,
        "primaryKey"                   -> 1.0,
        "records_written"              -> 2
      )
    )

    // fail tgt constraint evaluation
    val tgtDOConstraintFail = tgtDO
      .copy(
        id = "tgt1ConstraintFail",
        constraints = Seq(Constraint("firstnameNull", Some("firstname should be empty"), "firstname is null")),
        expectations = Seq()
      )
      .register
    val actionTgtConstraintFail = CopyAction("ca", srcDO.id, tgtDOConstraintFail.id)
    val ex1 = intercept[TaskFailedException](actionTgtConstraintFail.exec(Seq(srcSubFeed))(contextExec))
    assert(getRootCause(ex1).isInstanceOf[RuntimeException])

    // fail src constraint evaluation (validate on read)
    val srcDOConstraintFail = srcDO
      .copy(
        id = "src1ConstraintFail",
        constraints = Seq(Constraint("firstnameNull", Some("firstname should be empty"), "firstname is null"))
      )
      .register
    srcDOConstraintFail.writeDataFrame(l1, Seq())
    val actionSrcConstraintFail = CopyAction("ca", srcDOConstraintFail.id, tgtDO.id)
    val ex2 = intercept[TaskFailedException](actionSrcConstraintFail.exec(Seq(SparkSubFeed(None, srcDOConstraintFail.id, Seq())))(contextExec))
    assert(getRootCause(ex2).isInstanceOf[RuntimeException])

    // fail tgt expectation evaluation
    val tgtDOExpectationFail = tgtDO
      .copy(
        id = "tgt1ExpectationFail",
        expectations = Seq(SQLExpectation("avgRatingEq1", Some("avg rating should be 1"), "avg(rating)", Some("= 1"))),
        constraints = Seq()
      )
      .register
    instanceRegistry.register(tgtDOExpectationFail)
    val actionExpectationFail = CopyAction("ca", srcDO.id, tgtDOExpectationFail.id)
    val ex3 = intercept[TaskFailedException](actionExpectationFail.exec(Seq(srcSubFeed))(contextExec))
    assert(getRootCause(ex3).isInstanceOf[ExpectationValidationException])

    // fail src expectation evaluation
    val srcDOExpectationFail = srcDO
      .copy(
        id = "src1ExpectationFail",
        expectations = Seq(SQLExpectation("avgRatingEq1", Some("avg rating should be 1"), "avg(rating)", Some("= 1")))
      )
      .register
    srcDOExpectationFail.writeDataFrame(l1, Seq())
    val actionSrcExpectationFail = CopyAction("ca", srcDOExpectationFail.id, tgtDO.id)
    val ex4 = intercept[TaskFailedException](actionSrcExpectationFail.exec(Seq(SparkSubFeed(None, srcDOExpectationFail.id, Seq())))(contextExec))
    assert(getRootCause(ex4).isInstanceOf[ExpectationValidationException])
  }

  // TODO: test UniqueKeyExpectation fail with scope=Job / All!

  // Almost the same as copy load but without any transformation
  test("copy load without transformer (similar to old ingest action)") {

    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1", primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start load
    val action1 = CopyAction("a1", srcDO.id, tgtDO.id)
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO.getDataFrame().select("rating").collect[Int]
    assert(r1.size == 1)
    assert(r1.head == 5) // no transformer, rating should stay the same
  }

  test("copy with partition diff execution mode") {

    val srcDO = MockSparkDataObject("src1", partitions = Seq("type")).register
    val tgtDO = MockSparkDataObject("tgt1", partitions = Seq("type"), primaryKey = Some(Seq("type", "lastname", "firstname"))).register

    // prepare action
    val action = CopyAction("a1", srcDO.id, tgtDO.id, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", Seq()) // InitSubFeed needed to test executionMode!

    // prepare & start first load
    val l1 = Seq(("A", "doe", "john", 5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type" -> "A")))
    srcDO.writeDataFrame(l1, l1PartitionValues) // prepare testdata
    action.preInit(Seq(srcSubFeed), Seq())
    action.preExec(Seq(srcSubFeed))
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed))(contextExec).head
    action.postExec(Seq(srcSubFeed), Seq(tgtSubFeed1))

    // check first load
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)

    // prepare & start 2nd load
    action.reset
    val l2 = Seq(("B", "pan", "peter", 11)).toDF("type", "lastname", "firstname", "rating")
    val l2PartitionValues = Seq(PartitionValues(Map("type" -> "B")))
    srcDO.writeDataFrame(l2, l2PartitionValues) // prepare testdata
    assert(
      srcDO.getDataFrame().count == 2
    ) // note: this needs spark.sql.sources.partitionOverwriteMode=dynamic, otherwise the whole table is overwritten
    action.init(Seq(srcSubFeed))
    val tgtSubFeed2 = action.exec(Seq(srcSubFeed))(contextExec).head

    // check 2nd load
    assert(tgtSubFeed2.dataObjectId == tgtDO.id)
    assert(tgtSubFeed2.partitionValues.toSet == l2PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 2)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet ++ l2PartitionValues.toSet)
  }

  test("copy load with spark incremental mode and schema evolution") {

    val srcDO = MockSparkDataObject("src1", partitions = Seq("type")).register
    val tgtDO = MockSparkDataObject("tgt1", partitions = Seq("type"), primaryKey = Some(Seq("type", "lastname", "firstname"))).register

    // prepare action
    val action = CopyAction("a1", srcDO.id, tgtDO.id, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", Seq()) // InitSubFeed needed to test executionMode!

    // prepare & start first load
    val l1 = Seq(("A", "doe", "john", 5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type" -> "A")))
    srcDO.writeDataFrame(l1, l1PartitionValues) // prepare testdata
    action.preInit(Seq(srcSubFeed), Seq())
    action.preExec(Seq(srcSubFeed))
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed))(contextExec).head
    action.postExec(Seq(srcSubFeed), Seq(tgtSubFeed1))

    // check first load
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)

    // prepare & start 2nd load
    action.reset
    val l2 = Seq(("B", "pan", "peter", 11)).toDF("type", "lastname", "firstname", "rating")
    val l2PartitionValues = Seq(PartitionValues(Map("type" -> "B")))
    srcDO.writeDataFrame(l2, l2PartitionValues) // prepare testdata
    assert(
      srcDO.getDataFrame().count == 2
    ) // note: this needs spark.sql.sources.partitionOverwriteMode=dynamic, otherwise the whole table is overwritten
    action.init(Seq(srcSubFeed))
    val tgtSubFeed2 = action.exec(Seq(srcSubFeed))(contextExec).head

    // check 2nd load
    assert(tgtSubFeed2.dataObjectId == tgtDO.id)
    assert(tgtSubFeed2.partitionValues.toSet == l2PartitionValues.toSet)
    assert(tgtDO.getDataFrame().count == 2)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet ++ l2PartitionValues.toSet)
  }

  test("copy load with filter, additional columns and transformer options") {

    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start load
    val action1 = CopyAction(
      "ca",
      srcDO.id,
      tgtDO.id,
      transformers = Seq(
        ScalaClassGenericDfTransformer(
          className = classOf[TestOptionsDfTransformer].getName,
          options = Map("test" -> "test"),
          runtimeOptions = Map("appName" -> "application")
        ),
        FilterTransformer(filterClause = "lastname='jonson'"),
        ColumnsTransformer(additionalColumns = Map("run_id" -> "runId"))
      )
    )
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO.getDataFrame().select("rating", "test", "run_id").collect
      .map(row => (row.getAs[Int](0), row.getAs[String](1), row.getAs[Int](2)))
    assert(r1 == Seq((6, "test-appTest", 1)))
  }

  test("date to month aggregation with partition value transformation and PartitionDiffMode") {

    val srcDO = MockSparkDataObject("src1", partitions = Seq("dt")).register
    val tgtDO = MockSparkDataObject("tgt1", partitions = Seq("mt"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare, simulate
    val contextExec = contextInit.copy(phase = ExecutionPhase.Exec)
    val customTransformerConfig = ScalaClassGenericDfTransformer(className = classOf[TestAggDfTransformer].getName)
    val action1 = CopyAction(
      "ca",
      srcDO.id,
      tgtDO.id,
      transformers = Seq(customTransformerConfig),
      executionMode = Some(PartitionDiffMode(applyPartitionValuesTransform = true))
    )
    val l1 = Seq(("20100101", "jonson", "rob", 5), ("20100103", "doe", "bob", 3)).toDF("dt", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val srcSubFeedWithPartitions = srcSubFeed.copy(partitionValues = Seq(PartitionValues(Map("dt" -> "20100101")), PartitionValues(Map("dt" -> "20100103"))))
    action1.preInit(Seq(srcSubFeedWithPartitions), Seq())
    val tgtSubFeed = action1.init(Seq(srcSubFeedWithPartitions)).head.asInstanceOf[SparkSubFeed]

    // check simulate
    assert(tgtSubFeed.dataObjectId == tgtDO.id)
    val expectedPartitionValues = Seq(PartitionValues(Map("mt" -> "201001")))
    assert(tgtSubFeed.partitionValues == expectedPartitionValues)
    assert(tgtSubFeed.dataFrame.get.schema.columns.contains("mt"))

    // run
    action1.preExec(Seq(srcSubFeed))(contextExec)
    val resultSubFeeds = action1.exec(Seq(srcSubFeed))(contextExec)
    assert(tgtDO.getDataFrame().count == 2)
    action1.postExec(Seq(srcSubFeed), resultSubFeeds)(contextExec)

    // next run with no data
    action1.reset
    action1.preInit(Seq(srcSubFeed), Seq())
    action1.init(Seq(srcSubFeed))
    action1.preExec(Seq(srcSubFeed))(contextExec)
    val resultSubFeeds2 = intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec))
    assert(resultSubFeeds2.results.get.head.isSkipped)
  }

  test("copy load force saveMode") {

    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1", partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start 1st load - force SaveMode.Overwrite instead of Append
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, saveModeOptions = Some(SaveModeGenericOptions(SDLSaveMode.Overwrite)))
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "doe")), PartitionValues(Map("lastname" -> "jonson"))))
    action1.exec(Seq(srcSubFeed))(contextExec).head

    val r1 = tgtDO.getDataFrame().select("rating").collect[Int]
    assert(r1.toSet == Set(5, 3))

    // start 2nd load - data should be overwritten
    action1.exec(Seq(srcSubFeed))(contextExec).head

    val r2 = tgtDO.getDataFrame().select("rating").collect[Int]
    assert(r2.toSet == Set(5, 3))
  }

  test("fail on reading missing partition") {

    val srcDO = MockSparkDataObject("src1", partitions = Seq("lastname", "firstname")).register
    val tgtDO = MockSparkDataObject("tgt1", partitions = Seq("lastname", "firstname"), primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())

    // dont fail if partition exists
    val srcSubFeedOk = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "doe", "firstname" -> "bob"))))
    action1.exec(Seq(srcSubFeedOk))(contextExec)

    // fail if partition doesnt exist
    val srcSubFeedNok = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "joe", "firstname" -> "bob"))))
    intercept[AssertionError](action1.exec(Seq(srcSubFeedNok))(contextExec))

    // dont fail if partition information is an init of partition columns, and partition does exist
    val srcSubFeedInitOk = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "doe"))))
    action1.exec(Seq(srcSubFeedInitOk))(contextExec)

    // fail if partition information is an init of partition columns, but partition does not exist
    val srcSubFeedInitNok = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "joe"))))
    intercept[AssertionError](action1.exec(Seq(srcSubFeedInitNok))(contextExec))

    // dont fail if partition values is not an init of partition columns (lastname is not defined)
    val srcSubFeedNoInit = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("firstname" -> "bob"))))
    action1.exec(Seq(srcSubFeedNoInit))(contextExec)

  }

  test("copy load with generic DataFrameSubFeed as input") {

    // setup DataObjects
    // PKViolatorsDataObject has getSubFeedSupportedTypes=Seq(DataFrameSubFeed)
    // The Action should choose an appropriate SubFeedType for init/exec based on the output DataObject.

    val pkDO = MockSparkDataObject("pkTest", primaryKey = Some(Seq("id"))).register
    pkDO.writeDataFrame(SparkDataFrame(Collection.dfNonUniqueWithNull))

    val srcDO = PKViolatorsDataObject("src1")
    instanceRegistry.register(srcDO)
    val tgtDO = MockSparkDataObject("tgt1").register

    // prepare & start load with positive constraint and expectation evaluation
    val customTransformerConfig1 = SQLDfTransformer(name = "sql1", code = Some("select * from %{inputViewName}"))
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig1))
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(contextInit).head
  }

  test("copy load detect no-data rowCount=0 from SparkPlan") {

    val srcDO = ParquetFileDataObject("src1", tempPath + s"/src1")
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtDO = ParquetFileDataObject("tgt1", tempPath + s"/tgt1")
    instanceRegistry.register(tgtDO)

    // prepare empty Parquet file & start load
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(SparkRepartitionTransformer(numberOfTasksPerPartition = 10)))
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
      .where(functions.lit(false)) // write empty DataFrame
    Environment._enableSparkPlanNoDataCheck = Some(false)
    srcDO.writeDataFrame(l1, Seq())
    Environment._enableSparkPlanNoDataCheck = Some(true)
    assert(srcDO.getFileRefs(Seq()).nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(contextInit)
    intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec))

    // check that no files have been written to tgt1
    assert(tgtDO.getFileRefs(Seq()).isEmpty)
  }
}

class TestDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.withColumn("rating", col("rating") + lit(1))
  }

}

class TestOptionsDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.withColumn("rating", col("rating") + lit(1))
      .withColumn("test", lit(options("test") + "-" + options("appName")))
  }
}

class TestAggDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.withColumn("mt", substring(col("dt"), 1, 6))
  }

  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] =
    Some(partitionValues.map(pv => (pv, PartitionValues(Map("mt" -> pv("dt").toString.take(6))))).toMap)
}
