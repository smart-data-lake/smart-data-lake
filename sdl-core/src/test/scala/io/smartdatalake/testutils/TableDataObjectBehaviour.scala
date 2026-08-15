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
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{ColumnStatsType, Environment, SDLSaveMode, SaveModeMergeOptions, TableStatsType}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.plainScala.ScalaTestUtil.getCommonSubFeed
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.generic.transformer.SQLDfsTransformer
import io.smartdatalake.workflow.action.{CopyAction, CustomDataFrameAction, NoDataToProcessWarning, SDLExecutionId}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.expectation.{Expectation, SQLExpectation}
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase, ProcessingLogicException}
import org.scalatest.Assertions.intercept
import org.slf4j.Logger

/**
 * Parameters for creating the table DataObject under test, see [[TableDataObjectBehaviour]].
 * Engine-specific settings like path, db or connection are defined by the factory implementation of the test suite.
 */
case class TableDataObjectTestParams(
    primaryKey: Option[Seq[String]] = None,
    partitions: Seq[String] = Seq(),
    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
    allowSchemaEvolution: Boolean = false,
    options: Map[String, String] = Map(),
    expectations: Seq[Expectation] = Seq(),
    constraints: Seq[Constraint] = Seq()
)

/**
 * Engine-agnostic tests for [[TableDataObject]] implementations, e.g. writing with different SDLSaveModes,
 * schema evolution, partition handling, metrics, incremental output and expectations.
 * Instantiated per engine and DataObject implementation, parameterized by DataObject factories.
 */
trait TableDataObjectBehaviour extends GenericTestTool {
  this: SmartDataLakeLogger =>

  implicit private val implicitLogger: Logger = logger

  import io.smartdatalake.testutils.plainScala.ScalaTestUtil.registerDataObject

  def defaultEngineConnection: Connection with EngineConnection

  /** factory for the source DataObject used to feed test data through actions */
  type SourceDataObjectFactory = (String, InstanceRegistry) => DataObject with CanCreateDataFrame with CanWriteDataFrame

  /** factory for the table DataObject under test */
  type TableDataObjectFactory = (String, TableDataObjectTestParams, InstanceRegistry) =>
    TransactionalTableDataObject with CanMergeDataFrame with CanHandlePartitions

  /** factory for the table DataObject under test, for behaviours needing incremental output */
  type IncrementalTableDataObjectFactory = (String, TableDataObjectTestParams, InstanceRegistry) =>
    TransactionalTableDataObject with CanMergeDataFrame with CanHandlePartitions with CanCreateIncrementalOutput

  private def setupRegistryAndContext(): (InstanceRegistry, ActionPipelineContext, ActionPipelineContext) = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    val contextInit = ScalaTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)
    (instanceRegistry, contextInit, contextInit.copy(phase = ExecutionPhase.Exec))
  }

  /**
   * Copy data from a source DataObject to the table DataObject using CopyAction, then check data and table statistics.
   * @param expectColumnStats set to false for engines not supporting column statistics
   */
  def testCopyLoad(createSrcDataObject: SourceDataObjectFactory, createTgtDataObject: TableDataObjectFactory, expectColumnStats: Boolean = true): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._
    srcDO.writeDataFrame(Seq((Some(0), "Foo!"), (Some(1), "Bar!")).toDF("num", "text"), Seq())(contextExec)

    // prepare & start load
    val action = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = ScalaSubFeed(None, srcDO.id, Seq())
    action.exec(Seq(srcSubFeed))(contextExec)

    val expected = srcDO.getDataFrame()(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = expected.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testCopyLoad", Seq())(actual)(expected)
    assert(resultat)

    // check statistics
    assert(tgtDO.getStats().apply(TableStatsType.NumRows.toString) == 2)
    if (expectColumnStats) {
      val colStats = tgtDO.getColumnStats()
      assert(colStats.apply("num").get(ColumnStatsType.Max.toString).contains(1))
      assert(colStats.apply("text").get(ColumnStatsType.Max.toString).contains("Foo!"))
    }
  }

  /**
   * Copy data to a partitioned table DataObject using CopyAction, then list and move partitions.
   * @param testMovePartitions set to false for DataObjects not implementing movePartitions
   */
  def testCopyLoadPartitioned(createSrcDataObject: SourceDataObjectFactory, createTgtDataObject: TableDataObjectFactory, testMovePartitions: Boolean = true): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(partitions = Seq("num")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._
    srcDO.writeDataFrame(Seq((Some(0), "Foo!"), (Some(1), "Bar!")).toDF("num", "text"), Seq())(contextExec)

    // prepare & start load
    val action = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = ScalaSubFeed(None, srcDO.id, Seq())
    action.exec(Seq(srcSubFeed))(contextExec)

    val expected = srcDO.getDataFrame()(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = actual.isEqual(expected)
    if (!resultat) printFailedTestResultGdf("testCopyLoadPartitioned", Seq())(actual)(expected)
    assert(resultat)

    // move partition
    assert(tgtDO.listPartitions.map(_.elements).toSet == Set(Map("num" -> "0"), Map("num" -> "1")))
    if (testMovePartitions) {
      tgtDO.movePartitions(Seq((PartitionValues(Map("num" -> "0")), PartitionValues(Map("num" -> "2")))))
      assert(tgtDO.listPartitions.map(_.elements).toSet == Set(Map("num" -> "1"), Map("num" -> "2")))
    }
  }

  /**
   * SaveMode overwrite with a different schema on the 2nd load (schema evolution).
   */
  def testOverwriteWithDifferentSchema(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(saveMode = SDLSaveMode.Overwrite, allowSchemaEvolution = true), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testOverwriteWithDifferentSchema 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: overwrite all with different schema
    val df2 = Seq(("ext", "doe", "john", 10, "test"), ("ext", "smith", "peter", 1, "test"))
      .toDF("type", "lastname", "firstname", "rating2", "test")
    tgtDO.writeDataFrame(df2, Seq())(contextExec)
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val resultat2 = df2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testOverwriteWithDifferentSchema 2nd load", Seq())(actual2)(df2)
    assert(resultat2)
  }

  /**
   * SaveMode append with a different schema on the 2nd load (schema evolution).
   */
  def testAppendWithDifferentSchema(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(saveMode = SDLSaveMode.Append, allowSchemaEvolution = true), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper._
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testAppendWithDifferentSchema 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: append all with different schema
    val df2 = Seq(("ext", "doe", "john", 10, "test"), ("ext", "smith", "peter", 1, "test"))
      .toDF("type", "lastname", "firstname", "rating2", "test")
    tgtDO.writeDataFrame(df2, Seq())(contextExec)
    val actual2 = tgtDO.getDataFrame()(contextExec).filter(col("lastname") === lit("doe"))
    val resultat2 = actual2.count == 2 && (df1.columns ++ df2.columns).toSet == actual2.columns.toSet
    if (!resultat2) printFailedTestResultGdf("testAppendWithDifferentSchema 2nd load", Seq())(actual2)(df2)
    assert(resultat2)
  }

  /**
   * SaveMode overwrite on a partitioned table: overwriting all partitions is not allowed with partitionOverwriteMode=static,
   * overwrite a single partition, then delete a partition.
   */
  def testOverwriteAndDeletePartition(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(partitions = Seq("type"), saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "static")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper._
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testOverwriteAndDeletePartition 1st load", Seq())(actual)(df1)
    assert(resultat)

    assert(tgtDO.listPartitions.toSet == Set(PartitionValues(Map("type" -> "ext")), PartitionValues(Map("type" -> "int"))))

    // 2nd load: overwrite partition type=ext
    val df2 = Seq(("ext", "doe", "john", 10), ("ext", "smith", "peter", 1))
      .toDF("type", "lastname", "firstname", "rating")
    intercept[ProcessingLogicException](tgtDO.writeDataFrame(df2, Seq())(contextExec)) // not allowed to overwrite all partitions
    tgtDO.writeDataFrame(df2, partitionValues = Seq(PartitionValues(Map("type" -> "ext"))))(contextExec)
    val expected2 = df2.unionByName(df1.where(col("type") =!= lit("ext")))
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val resultat2 = expected2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testOverwriteAndDeletePartition 2nd load", Seq())(actual2)(expected2)
    assert(resultat2)

    // delete partition
    tgtDO.deletePartitions(Seq(PartitionValues(Map("type" -> "int"))))
    assert(tgtDO.listPartitions == Seq(PartitionValues(Map("type" -> "ext"))))
  }

  /**
   * SaveMode overwrite on a partitioned table with partitionOverwriteMode=dynamic: partitions contained in the
   * DataFrame are overwritten without passing partition values.
   */
  def testOverwritePartitionsDynamically(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(partitions = Seq("type"), saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "dynamic")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper._
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testOverwritePartitionsDynamically 1st load", Seq())(actual)(df1)
    assert(resultat)

    assert(tgtDO.listPartitions.toSet == Set(PartitionValues(Map("type" -> "ext")), PartitionValues(Map("type" -> "int"))))

    // 2nd load: dynamically overwrite partition type=ext
    val df2 = Seq(("ext", "doe", "john", 10), ("ext", "smith", "peter", 1))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df2, Seq())(contextExec) // allowed overwriting partitions because of partitionOverwriteMode=dynamic
    val expected2 = df2.unionByName(df1.where(col("type") =!= lit("ext")))
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val resultat2 = expected2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testOverwritePartitionsDynamically 2nd load", Seq())(actual2)(expected2)
    assert(resultat2)
  }

  /**
   * SaveMode append: data of the 2nd load is appended.
   */
  def testAppend(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(saveMode = SDLSaveMode.Append), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testAppend 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: append data
    val df2 = Seq(("ext", "doe", "john", 10), ("ext", "smith", "peter", 1))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df2, Seq())(contextExec)
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val expected2 = df2.unionByName(df1)
    val resultat2 = expected2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testAppend 2nd load", Seq())(actual2)(expected2)
    assert(resultat2)
  }

  /**
   * SaveMode merge: data of the 2nd load is merged by primary key.
   */
  def testMerge(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(primaryKey = Some(Seq("type", "lastname", "firstname")), saveMode = SDLSaveMode.Merge), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testMerge 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key
    val df2 = Seq(("ext", "doe", "john", 10), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df2, Seq())(contextExec)
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val expected2 = Seq(("ext", "doe", "john", 10), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    val resultat2 = expected2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testMerge 2nd load", Seq())(actual2)(expected2)
    assert(resultat2)
  }

  /**
   * SaveMode merge with schema evolution: the 2nd load merges by primary key with a different schema.
   * - column 'rating' deleted -> existing records keep column rating untouched, new records get rating=null.
   * - column 'rating2' added -> existing records get new column rating2=null.
   */
  def testMergeWithSchemaEvolution(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(primaryKey = Some(Seq("type", "lastname", "firstname")), saveMode = SDLSaveMode.Merge,
        allowSchemaEvolution = true, options = Map("mergeSchema" -> "true")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testMergeWithSchemaEvolution 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key with different schema
    val df2 = Seq(("ext", "doe", "john", 10), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating2")
    tgtDO.writeDataFrame(df2, Seq())(contextExec)
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val expected2 = Seq(("ext", "doe", "john", Some(5), Some(10)), ("ext", "smith", "peter", Some(3), None), ("int", "emma", "brown", None, Some(7)))
      .toDF("type", "lastname", "firstname", "rating", "rating2")
    val resultat2 = expected2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testMergeWithSchemaEvolution 2nd load", Seq())(actual2)(expected2)
    assert(resultat2)
  }

  /**
   * SaveMode merge with updateColumns: on the 2nd load only the listed columns of matched records are updated.
   */
  def testMergeWithUpdateColumns(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(primaryKey = Some(Seq("type", "lastname", "firstname")), saveMode = SDLSaveMode.Merge), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testMergeWithUpdateColumns 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key, updating only column 'rating'
    val df2 = Seq(("ext", "doe", "john", 10), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df2, Seq(), saveModeOptions = Some(SaveModeMergeOptions(updateColumns = Seq("rating"))))(contextExec)
    val actual2 = tgtDO.getDataFrame()(contextExec)
    val expected2 = Seq(("ext", "doe", "john", 10), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    val resultat2 = expected2.isEqual(actual2)
    if (!resultat2) printFailedTestResultGdf("testMergeWithUpdateColumns 2nd load", Seq())(actual2)(expected2)
    assert(resultat2)
  }

  /**
   * Writing a DataFrame with a different order of columns: columns are matched by name, not by position.
   */
  def testWriteWithDifferentColumnOrder(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(saveMode = SDLSaveMode.Overwrite), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper._
    import helper.implicits._

    // first load creates the table
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df, Seq())(contextExec)

    // 2nd load: overwrite with the same data but a different order of columns
    val dfSwitched = df.select(Seq("type", "rating", "firstname", "lastname").map(col))
    tgtDO.writeDataFrame(dfSwitched, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testWriteWithDifferentColumnOrder", Seq())(actual)(df)
    assert(resultat)
  }

  /**
   * Writing an empty DataFrame with dynamic partition overwrite creates no new table version/snapshot
   * and must throw NoDataToProcessWarning.
   */
  def testNoDataToProcessWarningOnEmptyWrite(createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(partitions = Seq("type"), saveMode = SDLSaveMode.Overwrite, options = Map("partitionOverwriteMode" -> "dynamic")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper._
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = df1.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testNoDataToProcessWarningOnEmptyWrite 1st load", Seq())(actual)(df1)
    assert(resultat)

    // 2nd load: no data -> NoDataToProcessWarning
    // use a new runId, so that implementations detecting "no new version written" can distinguish from the first load
    val contextExec2 = contextExec.copy(executionId = SDLExecutionId(2))
    val df2 = Seq(("ext", "doe", "john", 10), ("ext", "smith", "peter", 1))
      .toDF("type", "lastname", "firstname", "rating")
    val enableSparkPlanNoDataCheckOrig = Environment._enableSparkPlanNoDataCheck
    Environment._enableSparkPlanNoDataCheck = Some(false) // disable triggering SparkPlanNoDataWarning, as this test is about the check on DataObject level
    try {
      intercept[NoDataToProcessWarning](tgtDO.writeDataFrame(df2.filter(lit(false)), Seq())(contextExec2))
    } finally {
      Environment._enableSparkPlanNoDataCheck = enableSparkPlanNoDataCheckOrig
    }

    // 3rd load: write data
    tgtDO.writeDataFrame(df2, Seq())(contextExec2)
  }

  /**
   * Copy data to the table DataObject with a row-level constraint defined:
   * a load with valid data succeeds, a load with a violating record fails.
   */
  def testConstraints(createSrcDataObject: SourceDataObjectFactory, createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(primaryKey = Some(Seq("lastname", "firstname")),
        constraints = Seq(Constraint("ratingRange", expression = "rating <= 5"))), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._

    // first load: all records fulfill the constraint
    srcDO.writeDataFrame(Seq(("doe", "john", 5), ("smith", "peter", 3)).toDF("lastname", "firstname", "rating"), Seq())(contextExec)
    val action = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = ScalaSubFeed(None, srcDO.id, Seq())
    action.exec(Seq(srcSubFeed))(contextExec.copy(currentAction = Some(action)))
    assert(tgtDO.getDataFrame()(contextExec).count == 2)

    // 2nd load: one record violates the constraint -> write fails
    srcDO.writeDataFrame(Seq(("emma", "brown", 7)).toDF("lastname", "firstname", "rating"), Seq())(contextExec)
    val thrown = intercept[Exception](action.exec(Seq(srcSubFeed))(contextExec.copy(currentAction = Some(action))))
    val messages = Iterator.iterate(thrown: Throwable)(_.getCause).takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage)).mkString("\n")
    assert(messages.contains("Constraint 'ratingRange' failed"), s"expected constraint validation error, but got: $messages")
  }

  /**
   * Check metrics returned by writing to the table DataObject with CopyAction.
   */
  def testWriteMetrics(createSrcDataObject: SourceDataObjectFactory, createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(saveMode = SDLSaveMode.Overwrite, allowSchemaEvolution = true), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
    import helper.implicits._
    val l1 = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeDataFrame(l1, Seq())(contextExec)

    // prepare & start load
    val action = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = ScalaSubFeed(None, srcDO.id, Seq())
    val tgtSubFeed = action.exec(Seq(srcSubFeed))(contextExec.copy(currentAction = Some(action))).head
    assert(!tgtSubFeed.metrics.flatMap(_.get("records_written")).contains(0), "records_written should be >0 or removed")
    assert(!tgtSubFeed.metrics.flatMap(_.get("bytes_written")).contains(0), "bytes_written should be >0 or removed")
    assert(!tgtSubFeed.metrics.flatMap(_.get("no_data")).contains(true), "no_data should not be true")
    assert(tgtSubFeed.metrics.flatMap(_.get("count")).contains(3))
    assert(tgtSubFeed.metrics.flatMap(_.get("rows_inserted")).contains(3))
  }

  /**
   * Normal output mode: reading after setting the state returns the full data.
   */
  def testNormalOutputModeWithoutCdc(createTgtDataObject: IncrementalTableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(primaryKey = Some(Seq("id")), saveMode = SDLSaveMode.Append), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    tgtDO.prepare
    tgtDO.init(df1, Seq())
    tgtDO.writeDataFrame(df1, Seq())(contextExec)

    // test
    val newState1 = tgtDO.getState
    tgtDO.setState(newState1)

    // check
    assert(tgtDO.getDataFrame()(contextExec).count == 4)
  }

  /**
   * Incremental output mode: only data written since the last state is returned.
   * @param stateIsOrdered set to false for DataObjects whose state is not monotonically increasing (e.g. Iceberg snapshot ids)
   */
  def testIncrementalOutputModeWithInserts(createTgtDataObject: IncrementalTableDataObjectFactory, stateIsOrdered: Boolean = true): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(primaryKey = Some(Seq("id")), saveMode = SDLSaveMode.Append), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._
    tgtDO.setState(None) // initialize incremental output with empty state

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    tgtDO.prepare
    tgtDO.init(df1, Seq())
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val newState1 = tgtDO.getState

    // test 1
    tgtDO.setState(newState1)
    assert(tgtDO.getDataFrame()(contextExec).count == 4)

    // append test data 2
    val df2 = Seq((5, "B", 5)).toDF("id", "p", "value")
    tgtDO.writeDataFrame(df2, Seq())(contextExec)
    val newState2 = tgtDO.getState

    // test 2
    tgtDO.setState(newState2)
    assert(tgtDO.getDataFrame()(contextExec).count == 1)

    // append test data 3
    val df3 = Seq((6, "T", 5), (7, "R", 7), (8, "T", 2)).toDF("id", "p", "value")
    tgtDO.writeDataFrame(df3, Seq())(contextExec)
    val newState3 = tgtDO.getState

    // test 3
    tgtDO.setState(newState3)
    assert(tgtDO.getDataFrame()(contextExec).count == 3)

    if (stateIsOrdered) {
      assert(newState1.get < newState2.get)
      assert(newState2.get < newState3.get)
    }

    tgtDO.setState(None) // to get the full dataframe
    assert(tgtDO.getDataFrame()(contextInit).count == 8)
  }

  /**
   * Incremental output mode needs a primary key defined on the table.
   */
  def testIncrementalOutputModeWithoutPrimaryKey(createTgtDataObject: IncrementalTableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(saveMode = SDLSaveMode.Append), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._
    tgtDO.setState(None) // initialize incremental output with empty state

    // write test data
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    tgtDO.prepare
    tgtDO.init(df1, Seq())
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val newState1 = tgtDO.getState

    // read incrementally
    tgtDO.setState(newState1)
    val thrown = intercept[IllegalArgumentException](tgtDO.getDataFrame()(contextExec))

    // check
    assert(thrown.getMessage.contains("PrimaryKey for table"))
  }

  /**
   * Incremental output mode with updates and inserts: only new records and the latest version of updated records are returned.
   * Updates and inserts are done with merge writes, as an engine-agnostic replacement for SQL INSERT/UPDATE statements.
   */
  def testIncrementalOutputModeWithUpdatesAndInserts(createTgtDataObject: IncrementalTableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    val tgtDO = registerDataObject(createTgtDataObject("tgt1", TableDataObjectTestParams(primaryKey = Some(Seq("id"))), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(tgtDO.getSubFeedSupportedTypes.head)
    import helper.implicits._
    tgtDO.setState(None) // initialize incremental output with empty state

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    tgtDO.prepare
    tgtDO.init(df1, Seq())
    tgtDO.writeDataFrame(df1, Seq())(contextExec)
    val newState1 = tgtDO.getState
    tgtDO.setState(newState1)
    assert(tgtDO.getDataFrame()(contextExec).count == 4)

    // do updates and inserts
    def mergeWrite(rows: Seq[(Int, String, Int)]): Unit =
      tgtDO.writeDataFrame(rows.toDF("id", "p", "value"), Seq(), saveModeOptions = Some(SaveModeMergeOptions()))(contextExec)
    mergeWrite(Seq((5, "T", 7)))
    val newState2 = tgtDO.getState
    mergeWrite(Seq((6, "U", 3)))
    mergeWrite(Seq((1, "Z", 8)))
    mergeWrite(Seq((1, "W", 1)))

    // test: 2x new insert + 1x the latest update
    val expected = Seq((5, "T", 7), (6, "U", 3), (1, "W", 1)).toDF("id", "p", "value")
    tgtDO.setState(newState2)
    val actual = tgtDO.getDataFrame()(contextExec)
    val resultat = expected.isEqual(actual)
    if (!resultat) printFailedTestResultGdf("testIncrementalOutputModeWithUpdatesAndInserts", Seq())(actual)(expected)
    assert(resultat)
  }

  /**
   * Copy data to the table DataObject with expectations defined, using CustomDataFrameAction with two inputs.
   * Checks that the expectation is evaluated and reported as metric.
   */
  def testCopyLoadWithExpectations(createSrcDataObject: SourceDataObjectFactory, createTgtDataObject: TableDataObjectFactory): Unit = {
    val (instanceRegistry, contextInit, contextExec) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = contextInit

    // setup DataObjects
    val srcDO1 = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val srcDO2 = registerDataObject(createSrcDataObject("src2", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1",
      TableDataObjectTestParams(expectations = Seq(SQLExpectation("maxRating", aggExpression = "max(rating)"))), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO1, tgtDO))
    import helper.implicits._

    // prepare
    val transformer = SQLDfsTransformer(code = Map(tgtDO.id.id -> s"select * from %{inputViewName_${srcDO1.id.id}}"))
    val action = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO.id), transformers = Seq(transformer))
    instanceRegistry.register(action)
    val dfInput = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO1.writeDataFrame(dfInput, Seq())(contextExec)
    srcDO2.writeDataFrame(dfInput, Seq())(contextExec)

    // run
    val srcSubFeeds = Seq(ScalaSubFeed(None, srcDO1.id, Seq()), ScalaSubFeed(None, srcDO2.id, Seq()))
    action.init(srcSubFeeds)
    val tgtSubFeed = action.exec(srcSubFeeds)(contextExec.copy(currentAction = Some(action))).head

    // check data and expectation metric
    assert(tgtDO.getDataFrame()(contextExec).count == 2)
    assert(tgtSubFeed.metrics.flatMap(_.get("maxRating")).contains(5))
  }
}
