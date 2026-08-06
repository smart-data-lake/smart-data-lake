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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.testutils.{TableDataObjectBehaviour, TableDataObjectTestParams}
import io.smartdatalake.util.hdfs.{HdfsUtil, SparkHdfsUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.DeltaLakeTestUtils.deltaDb
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file
import java.nio.file.Files

/**
 * Tests DeltaLakeTableDataObject with the classic Spark engine implementation (DeltaLakeTableSparkClassicEngine),
 * using the shared engine-agnostic TableDataObjectBehaviour.
 */
class DeltaLakeTableDataObjectTest extends AnyFunSuite with BeforeAndAfterAll
  with SmartDataLakeLogger with TableDataObjectBehaviour {

  // set additional spark options for delta lake
  protected implicit val session: SparkSession = DeltaLakeTestUtils.session

  val tempDir: file.Path = Files.createTempDirectory("tempHadoopDO")
  val tempPath: String = tempDir.toAbsolutePath.toString

  // registry for creating MockSparkDataObject, the behaviour methods use their own registry
  private implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  override def beforeAll(): Unit = {
    val warehousePath = new Path("spark-warehouse/delta.db")
    implicit val fs: FileSystem = SparkHdfsUtil.getHadoopFsFromSpark(warehousePath)(session)
    HdfsUtil.deletePath(path = warehousePath, doWarn = false)
  }

  private def createSrcDataObject(id: String, registry: InstanceRegistry) = MockSparkDataObject(id)(registry)

  /** creates an external table with path below tempPath */
  private def createExternalTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): DeltaLakeTableDataObject = {
    val table = Table(db = Some(deltaDb), name = s"behaviour_$id", primaryKey = params.primaryKey)
    DeltaLakeTableDataObject(id, path = Some(tempPath + s"/${table.fullName}"), partitions = params.partitions,
      options = params.options, table = table, constraints = params.constraints, expectations = params.expectations,
      saveMode = params.saveMode, allowSchemaEvolution = params.allowSchemaEvolution)(registry)
  }

  /** creates a managed table (no path defined) */
  private def createManagedTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): DeltaLakeTableDataObject = {
    val table = Table(db = Some(deltaDb), name = s"behaviour_managed_$id", primaryKey = params.primaryKey)
    DeltaLakeTableDataObject(id, partitions = params.partitions, options = params.options, table = table,
      constraints = params.constraints, expectations = params.expectations, saveMode = params.saveMode,
      allowSchemaEvolution = params.allowSchemaEvolution)(registry)
  }

  test("CustomDf2DeltaTable") {
    testCopyLoad(createSrcDataObject, createExternalTableDataObject)
  }

  test("CustomDf2DeltaTable_partitioned") {
    testCopyLoadPartitioned(createSrcDataObject, createExternalTableDataObject)
  }

  test("SaveMode overwrite with different schema") {
    testOverwriteWithDifferentSchema(createExternalTableDataObject)
  }

  test("SaveMode overwrite with different schema on managed table") {
    testOverwriteWithDifferentSchema(createManagedTableDataObject)
  }

  test("SaveMode append with different schema") {
    testAppendWithDifferentSchema(createExternalTableDataObject)
  }

  test("SaveMode append with different schema on managed table") {
    testAppendWithDifferentSchema(createManagedTableDataObject)
  }

  test("SaveMode overwrite and delete partition") {
    testOverwriteAndDeletePartition(createExternalTableDataObject)
  }

  test("SaveMode overwrite partitions dynamically") {
    testOverwritePartitionsDynamically(createExternalTableDataObject)
  }

  test("SaveMode overwrite and delete partition on managed table") {
    testOverwriteAndDeletePartition(createManagedTableDataObject)
  }

  test("SaveMode append") {
    testAppend(createExternalTableDataObject)
  }

  test("SaveMode append on managed table") {
    testAppend(createManagedTableDataObject)
  }

  test("SaveMode merge") {
    testMerge(createExternalTableDataObject)
  }

  test("SaveMode merge with schema evolution") {
    testMergeWithSchemaEvolution(createExternalTableDataObject)
  }

  test("SaveMode merge with updateCols") {
    testMergeWithUpdateColumns(createExternalTableDataObject)
  }

  test("write with different order of columns") {
    testWriteWithDifferentColumnOrder(createExternalTableDataObject)
  }

  // Note: testNoDataToProcessWarningOnEmptyWrite is not applicable to DeltaLake, as delta commits a new (empty)
  // table version even when writing an empty DataFrame, so the "no new version written" check never triggers.

  test("constraints validation") {
    testConstraints(createSrcDataObject, createExternalTableDataObject)
  }

  // Note that this is not possible with DeltaLake <= 3.2.0, as schema evolution with mergeStmt.insertExpr is not properly supported.
  // Unfortunately this is needed by HistorizeAction with merge.
  // We test for failure to be notified once it is working...
  test("SaveMode merge with updateCols and schema evolution - fails in deltalake <= 3.2.0") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    val contextExec = context.copy(phase = ExecutionPhase.Exec)
    instanceRegistry.register(defaultEngineConnection)

    val targetDO = createExternalTableDataObject("tgt1", TableDataObjectTestParams(primaryKey = Some(Seq("type", "lastname", "firstname")),
      saveMode = SDLSaveMode.Merge, allowSchemaEvolution = true, options = Map("mergeSchema" -> "true")), instanceRegistry)
    targetDO.dropTable
    instanceRegistry.register(targetDO)
    val helper = DataFrameSubFeed.getCompanion(targetDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("type", "lastname", "firstname", "rating")
    targetDO.writeDataFrame(df1, Seq())(contextExec)
    assert(df1.isEqual(targetDO.getDataFrame()(contextExec)))

    // 2nd load: merge data by primary key with different schema
    // - column 'rating' deleted -> existing records will keep column rating untouched (values are preserved and not set to null), new records will get new column rating set to null.
    // - column 'rating2' added -> existing records will get new column rating2 set to null
    val df2 = Seq(("ext", "doe", "john", 10), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating2")
    // this does not work for now, see also https://github.com/delta-io/delta/issues/2300
    intercept[AnalysisException](targetDO.writeDataFrame(df2, Seq(),
      saveModeOptions = Some(SaveModeMergeOptions(updateColumns = Seq("lastname", "firstname", "rating", "rating2"))))(contextExec))
  }

  test("returns correct metrics") {
    testWriteMetrics(createSrcDataObject, createExternalTableDataObject)
  }

  test("normal output mode without cdc activated") {
    testNormalOutputModeWithoutCdc(createExternalTableDataObject)
  }

  test("incremental output mode with inserts") {
    testIncrementalOutputModeWithInserts(createExternalTableDataObject)
  }

  test("incremental output mode without primary keys") {
    testIncrementalOutputModeWithoutPrimaryKey(createExternalTableDataObject)
  }

  test("incremental output mode with updates and inserts") {
    testIncrementalOutputModeWithUpdatesAndInserts(createExternalTableDataObject)
  }

  test("copy load expectations test") {
    testCopyLoadWithExpectations(createSrcDataObject, createExternalTableDataObject)
  }
}
