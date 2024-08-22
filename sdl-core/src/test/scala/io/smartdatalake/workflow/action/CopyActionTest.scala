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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeGenericOptions}
import io.smartdatalake.testutils.TestUtil.dfNonUniqueWithNull
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.dag.TaskFailedException.getRootCause
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.{FileIncrementalMoveMode, PartitionDiffMode}
import io.smartdatalake.workflow.action.expectation.{CompletenessExpectation, TransferRateExpectation}
import io.smartdatalake.workflow.action.generic.transformer.{AdditionalColumnsTransformer, FilterTransformer, SQLDfTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import io.smartdatalake.workflow.action.spark.transformer.{ScalaClassSparkDfTransformer, ScalaCodeSparkDfTransformer, SparkRepartitionTransformer}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation._
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, InitSubFeed}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{lit, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.{Files, Path => NioPath}

class CopyActionTest extends FunSuite with BeforeAndAfter {

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

  test("copy load with custom transformation class and incremental move mode (delete)") {

    // setup DataObjects
    val feed = "copy"
    val srcDO = ParquetFileDataObject( "src1", tempPath+s"/src1", filenameColumn = Some("_filename"))
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), Seq("lastname"), analyzeTableAfterWrite=true, table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDfTransformer(className = classOf[TestDfTransformer].getName)
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig), executionMode = Some(FileIncrementalMoveMode()))
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    assert(srcDO.getFileRefs(Seq()).nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "doe")),PartitionValues(Map("lastname" -> "jonson"))))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    action1.postExec(Seq(srcSubFeed),Seq(tgtSubFeed))
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check output
    val r1 = session.table(s"${tgtTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSet
    assert(r1 == Set(4,6)) // should be increased by 1 through TestDfTransformer

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

    // setup DataObjects
    val feed = "copy"
    val srcDO = ParquetFileDataObject( "src1", tempPath+s"/src1")
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), analyzeTableAfterWrite=true, table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare data
    val executionMode = FileIncrementalMoveMode(archivePath = Some("archive"))
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig), executionMode = Some(executionMode))
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())

    // start load
    val srcFiles = srcDO.getFileRefs(Seq()).map(_.fullPath)
    assert(srcFiles.nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    action1.postExec(Seq(srcSubFeed),Seq(tgtSubFeed))

    // check result
    val r1 = session.table(s"${tgtTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 6) // should be increased by 1 through TestDfTransformer
    // check input archived by incremental move mode
    assert(srcDO.getFileRefs(Seq()).isEmpty)
    val srcDOArchived = ParquetFileDataObject( "src1", tempPath+s"/src1/archive")
    assert(srcDOArchived.getFileRefs(Seq()).nonEmpty)

    // start second load without new files - schema should be present because of schema file
    intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec).head)
  }

  test("copy load incremental move mode (archive) V1 DataSource") {

    // setup DataObjects
    val feed = "copy"
    val srcDO = XmlFileDataObject("src1", tempPath + s"/src1")
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtDO = MockDataObject("tgt1")
    instanceRegistry.register(tgtDO)

    // prepare data
    val executionMode = FileIncrementalMoveMode(archivePath = Some("archive"))
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, executionMode = Some(executionMode))
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())

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
    val srcDO = MockDataObject("src1",
      expectations = Seq(
        CountExpectation(name = "count", expectation = Some(">= 1")),
        CountExpectation(name = "countAll", expectation = Some("= 2"), scope = ExpectationScope.All),
      )
    ).register
    val tgtTable = Table(Some("default"), "copy_output", None, primaryKey = Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), Seq("lastname"), analyzeTableAfterWrite=true, table = tgtTable, numInitialHdfsPartitions = 1,
      constraints = Seq(Constraint("firstnameNotNull", Some("firstname should be non empty"), "firstname is not null")),
      expectations = Seq(
        CountExpectation(expectation = Some(">= 1")),
        SQLExpectation("avgRatingGt1", Some("avg rating should be bigger than 1"), "avg(rating)", Some("> 1")),
        SQLFractionExpectation("pctBob", countConditionExpression = "firstname = 'bob'", expectation = Some("= 0")), // because we only select Rob and not Bob...
        CountExpectation(name = "countPerPartition", expectation = Some(">= 1"), scope = ExpectationScope.JobPartition),
        CountExpectation(name = "countAll", expectation = Some(">= 1"), scope = ExpectationScope.All),
        SQLQueryExpectation(name = "countOfPartitionsWith1Record", code = "select count(*) from (select lastname from %{inputViewName} group by lastname having count(*) = 1)", scope = ExpectationScope.All),
        SQLExpectation("resultNull", Some("dont fail if result is null"), "null", Some("> 1")),
        UniqueKeyExpectation("primaryKey", approximate=approximateUniqueConstraint)
      )
    )
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start load with positive constraint and expectation evaluation
    val customTransformerConfig1 = SQLDfTransformer(name = "sql1", code = "select * from %{inputViewName} where rating = 5")
    val customTransformerConfig2 = SQLDfTransformer(name = "sql2", code = "select * from %{inputViewName} where rating = 5") // test multiple transformers - it doesnt matter if they do the same.
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id,
      transformers = Seq(customTransformerConfig1, customTransformerConfig2),
      expectations = Seq(TransferRateExpectation(), CompletenessExpectation(expectation = None))
    )
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed1 = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)

    // check result
    val r1 = session.table(s"${tgtTable.fullName}")
      .select($"lastname")
      .as[String].collect().toSeq
    assert(r1 == Seq("jonson")) // only one record has rating 5 (see where condition)

    // check expectation value in metrics
    val metrics1 = tgtSubFeed1.metrics.get
    assert(metrics1 == Map("count" -> 1, "avgRatingGt1" -> 5.0, "pctBob" -> 0.0, "countPerPartition#jonson" -> 1, "count#src1" -> 1, "count#mainInput" -> 1, "countAll#src1" -> 2, "countAll#mainInput" -> 2, "pctTransfer" -> 1.0, "countAll" -> 1, "countAll#src1" -> 2, "countAll#mainInput" -> 2, "pctComplete" -> 0.5, "countOfPartitionsWith1Record" -> 1, "resultNull" -> None, "primaryKey" -> 1.0))

    // overwrite src with 2 record to process
    val l2 = Seq(("dau", "peter", 5), ("dau", "pan", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2, Seq())
    action1.reset
    val tgtSubFeed2 = action1.exec(Seq(srcSubFeed))(contextExec).head

    // check expectation value in metrics - countAll should be 2 now, but count should stay 1
    val metrics2 = tgtSubFeed2.metrics.get
    assert(metrics2 == Map("count" -> 2, "avgRatingGt1" -> 5.0, "pctBob" -> 0.0, "countPerPartition#dau" -> 2, "count#src1" -> 2, "count#mainInput" -> 2, "pctTransfer" -> 1.0, "countAll" -> 3, "countAll#src1" -> 2, "countAll#mainInput" -> 2, "pctComplete" -> 1.5, "countOfPartitionsWith1Record" -> 1, "resultNull" -> None, "primaryKey" -> 1.0))

    // fail tgt constraint evaluation
    val tgtDOConstraintFail = HiveTableDataObject( "tgt1constraintFail", Some(tempPath+s"/${tgtTable.fullName}"), Seq("lastname"), table = tgtTable,
      constraints = Seq(Constraint("firstnameNull", Some("firstname should be empty"), "firstname is null")),
    )
    instanceRegistry.register(tgtDOConstraintFail)
    val actionTgtConstraintFail = CopyAction("ca", srcDO.id, tgtDOConstraintFail.id)
    val ex1 = intercept[TaskFailedException](actionTgtConstraintFail.exec(Seq(srcSubFeed))(contextExec))
    assert(getRootCause(ex1).isInstanceOf[RuntimeException])

    // fail src constraint evaluation (validate on read)
    val srcDOConstraintFail = MockDataObject("src1constraintFail",
      constraints = Seq(Constraint("firstnameNull", Some("firstname should be empty"), "firstname is null"))).register
    srcDOConstraintFail.writeSparkDataFrame(l1, Seq())
    val actionSrcConstraintFail = CopyAction("ca", srcDOConstraintFail.id, tgtDO.id)
    val ex2 = intercept[TaskFailedException](actionSrcConstraintFail.exec(Seq(SparkSubFeed(None, srcDOConstraintFail.id, Seq())))(contextExec))
    assert(getRootCause(ex2).isInstanceOf[RuntimeException])

    // fail tgt expectation evaluation
    val tgtDOExpectationFail = HiveTableDataObject( "tgt1expectationFail", Some(tempPath+s"/${tgtTable.fullName}"), Seq("lastname"), table = tgtTable,
      expectations = Seq(SQLExpectation("avgRatingEq1", Some("avg rating should be 1"), "avg(rating)", Some("= 1")))
    )
    instanceRegistry.register(tgtDOExpectationFail)
    val actionExpectationFail = CopyAction("ca", srcDO.id, tgtDOExpectationFail.id)
    val ex3 = intercept[TaskFailedException](actionExpectationFail.exec(Seq(srcSubFeed))(contextExec))
    assert(getRootCause(ex3).isInstanceOf[ExpectationValidationException])

    // fail src expectation evaluation
    val srcDOExpectationFail = MockDataObject("src1expectationFail",
      expectations = Seq(SQLExpectation("avgRatingEq1", Some("avg rating should be 1"), "avg(rating)", Some("= 1")))).register
    srcDOExpectationFail.writeSparkDataFrame(l1, Seq())
    val actionSrcExpectationFail = CopyAction("ca", srcDOExpectationFail.id, tgtDO.id)
    val ex4 = intercept[TaskFailedException](actionSrcExpectationFail.exec(Seq(SparkSubFeed(None, srcDOExpectationFail.id, Seq())))(contextExec))
    assert(getRootCause(ex4).isInstanceOf[ExpectationValidationException])
  }

  // TODO: test UniqueKeyExpectation fail with scope=Job / All!

  // Almost the same as copy load but without any transformation
  test("copy load without transformer (similar to old ingest action)") {

    // setup DataObjects
    val feed = "notransform"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val action1 = CopyAction("a1", srcDO.id, tgtDO.id, transformer = None)
    val l1 = Seq(("doe","john",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = session.table(s"${tgtTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5) // no transformer, rating should stay the same
  }

  test("copy with partition diff execution mode") {

    // setup DataObjects
    val feed = "partitiondiff"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("type","lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)
    tgtDO.dropTable

    // prepare action
    val action = CopyAction("a1", srcDO.id, tgtDO.id, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", Seq()) // InitSubFeed needed to test executionMode!

    // prepare & start first load
    val l1 = Seq(("A","doe","john",5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type"->"A")))
    srcDO.writeSparkDataFrame(l1, l1PartitionValues) // prepare testdata
    action.preInit(Seq(srcSubFeed), Seq())
    val initOutputSubFeeds = action.init(Seq(srcSubFeed))
    action.preExec(Seq(srcSubFeed))
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed))(contextExec).head
    action.postExec(Seq(srcSubFeed), Seq(tgtSubFeed1))

    // check first load
    assert(initOutputSubFeeds.head.asInstanceOf[SparkSubFeed].dataFrame.get.schema.columns.last == "type", "partition columns must be moved last already in init phase")
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getSparkDataFrame().count() == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)

    // prepare & start 2nd load
    action.reset
    val l2 = Seq(("B","pan","peter",11)).toDF("type", "lastname", "firstname", "rating")
    val l2PartitionValues = Seq(PartitionValues(Map("type"->"B")))
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

  test("copy load with spark incremental mode and schema evolution") {

    // setup DataObjects
    val feed = "partitiondiff"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("type","lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, partitions = Seq("type"), numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgtDO)
    tgtDO.dropTable

    // prepare action
    val action = CopyAction("a1", srcDO.id, tgtDO.id, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", Seq()) // InitSubFeed needed to test executionMode!

    // prepare & start first load
    val l1 = Seq(("A","doe","john",5)).toDF("type", "lastname", "firstname", "rating")
    val l1PartitionValues = Seq(PartitionValues(Map("type"->"A")))
    srcDO.writeSparkDataFrame(l1, l1PartitionValues) // prepare testdata
    action.preInit(Seq(srcSubFeed), Seq())
    val initOutputSubFeeds = action.init(Seq(srcSubFeed))
    action.preExec(Seq(srcSubFeed))
    val tgtSubFeed1 = action.exec(Seq(srcSubFeed))(contextExec).head
    action.postExec(Seq(srcSubFeed), Seq(tgtSubFeed1))

    // check first load
    assert(initOutputSubFeeds.head.asInstanceOf[SparkSubFeed].dataFrame.get.schema.columns.last == "type", "partition columns must be moved last already in init phase")
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)
    assert(tgtSubFeed1.partitionValues.toSet == l1PartitionValues.toSet)
    assert(tgtDO.getSparkDataFrame().count() == 1)
    assert(tgtDO.listPartitions.toSet == l1PartitionValues.toSet)

    // prepare & start 2nd load
    action.reset
    val l2 = Seq(("B","pan","peter",11)).toDF("type", "lastname", "firstname", "rating")
    val l2PartitionValues = Seq(PartitionValues(Map("type"->"B")))
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

  test("copy load with filter, additional columns and transformer options") {

    // setup DataObjects
    val feed = "copy"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), Seq("lastname"), analyzeTableAfterWrite=true, table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id,
      transformers = Seq(
        ScalaClassSparkDfTransformer(className = classOf[TestOptionsDfTransformer].getName, options = Map("test" -> "test"), runtimeOptions = Map("appName" -> "application")),
        FilterTransformer(filterClause = "lastname='jonson'"),
        AdditionalColumnsTransformer(additionalColumns = Map("run_id" -> "runId"))
      )
    )
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO.getSparkDataFrame()
      .select($"rating", $"test", $"run_id")
      .as[(Int,String,Int)].collect().toSeq
    assert(r1 == Seq((6, "test-appTest", 1)))
  }

  test("date to month aggregation with partition value transformation and PartitionDiffMode") {

    // setup DataObjects
    val feed = "copy"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("dt"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), Seq("mt"), analyzeTableAfterWrite=true, table = tgtTable, numInitialHdfsPartitions = 1)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare, simulate
    val contextExec = contextInit.copy(phase = ExecutionPhase.Exec)
    val customTransformerConfig = ScalaClassSparkDfTransformer(className = classOf[TestAggDfTransformer].getName)
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig), executionMode = Some(PartitionDiffMode(applyPartitionValuesTransform=true)))
    val l1 = Seq(("20100101","jonson","rob",5),("20100103","doe","bob",3)).toDF("dt", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val srcSubFeedWithPartitions = srcSubFeed.copy(partitionValues = Seq(PartitionValues(Map("dt"->"20100101")), PartitionValues(Map("dt"->"20100103"))))
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
    assert(tgtDO.getSparkDataFrame().count() == 2)
    action1.postExec(Seq(srcSubFeed),resultSubFeeds)(contextExec)

    // next run with no data
    action1.reset
    action1.preInit(Seq(srcSubFeed), Seq())
    action1.init(Seq(srcSubFeed))
    action1.preExec(Seq(srcSubFeed))(contextExec)
    val resultSubFeeds2 = intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec))
    assert(resultSubFeeds2.results.get.head.isSkipped)
  }

  test("copy load force saveMode") {

    // setup DataObjects
    val feed = "copy"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), Seq("lastname"), table = tgtTable, numInitialHdfsPartitions = 1, saveMode = SDLSaveMode.Append)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load - force SaveMode.Overwrite instead of Append
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, saveModeOptions = Some(SaveModeGenericOptions(SDLSaveMode.Overwrite)))
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq(PartitionValues(Map("lastname" -> "doe")),PartitionValues(Map("lastname" -> "jonson"))))
    action1.exec(Seq(srcSubFeed))(contextExec).head

    val r1 = tgtDO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r1.toSet == Set(5,3))

    // start 2nd load - data should be overwritten
    action1.exec(Seq(srcSubFeed))(contextExec).head

    val r2 = tgtDO.getSparkDataFrame()
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r2.toSet == Set(5,3))
  }

  test("fail on reading missing partition") {

    // setup DataObjects
    val feed = "copy"
    val srcTable = Table(Some("default"), "copy_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("lastname", "firstname"), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "copy_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable, partitions = Seq("lastname", "firstname"), numInitialHdfsPartitions = 1, saveMode = SDLSaveMode.Append)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())

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

    val pkDO = MockDataObject("pkTest", primaryKey = Some(Seq("id"))).register
    pkDO.writeSparkDataFrame(dfNonUniqueWithNull)

    val srcDO = PKViolatorsDataObject("src1")
    instanceRegistry.register(srcDO)
    val tgtDO = MockDataObject("tgt1").register

    // prepare & start load with positive constraint and expectation evaluation
    val customTransformerConfig1 = SQLDfTransformer(name = "sql1", code = "select * from %{inputViewName}")
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig1))
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(contextInit).head
    val tgtSubFeed1 = action1.exec(Seq(srcSubFeed))(contextExec).head
  }


  test("copy load detect no-data rowCount=0 from SparkPlan") {

    // setup DataObjects
    val feed = "copy"
    val srcDO = ParquetFileDataObject( "src1", tempPath+s"/src1")
    srcDO.deleteAll
    instanceRegistry.register(srcDO)
    val tgtDO = ParquetFileDataObject( "tgt1", tempPath+s"/tgt1")
    instanceRegistry.register(tgtDO)

    // prepare empty Parquet file & start load
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(SparkRepartitionTransformer(numberOfTasksPerPartition = 10)))
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
      .where(lit(false)) // write empty DataFrame
    Environment._enableSparkPlanNoDataCheck = Some(false)
    srcDO.writeSparkDataFrame(l1, Seq())
    Environment._enableSparkPlanNoDataCheck = Some(true)
    assert(srcDO.getFileRefs(Seq()).nonEmpty)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))(contextInit)
    intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec))

    // check that no files have been written to tgt1
    assert(tgtDO.getFileRefs(Seq()).isEmpty)
  }
}

class TestDfTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
    import session.implicits._
    df.withColumn("rating", $"rating" + 1)
  }
}

class TestOptionsDfTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
    import session.implicits._
    df.withColumn("rating", $"rating" + 1)
      .withColumn("test", lit(options("test")+"-"+options("appName")))
  }
}

class TestAggDfTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
    import session.implicits._
    df.withColumn("mt", substring($"dt",1,6))
  }
  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues,PartitionValues]] = {
    Some(partitionValues.map(pv => (pv,PartitionValues(Map("mt" -> pv("dt").toString.take(6))))).toMap)
  }
}
