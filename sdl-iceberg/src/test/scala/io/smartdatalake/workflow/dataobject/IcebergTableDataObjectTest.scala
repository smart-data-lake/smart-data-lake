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
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestTool, SparkTestUtil}
import io.smartdatalake.testutils.{CatalogMetadataBehaviour, CatalogMetadataTestParams, TableDataObjectBehaviour, TableDataObjectTestParams}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues, SparkHdfsUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection, HadoopFileConnection, IcebergTableConnection}
import io.smartdatalake.workflow.dataobject.spark.SparkDataObjectOps._
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.nio.file.Files

/**
 * Tests IcebergTableDataObject using the shared engine-agnostic TableDataObjectBehaviour,
 * plus Iceberg-specific tests (create from parquet files, hadoop catalog, ...).
 */
class IcebergTableDataObjectTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger
  with SparkTestTool with TableDataObjectBehaviour with CatalogMetadataBehaviour {
  private implicit val implLogger: Logger = logger

  protected implicit val session: SparkSession = IcebergTestUtils.session

  import session.implicits._

  private val tempDir = Files.createTempDirectory("tempHadoopDO")
  private val tempPath = tempDir.toAbsolutePath.toString

  // registry and context for the iceberg-specific tests, the behaviour methods use their own registry
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = context.copy(phase = ExecutionPhase.Exec)

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  before {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  private def createSrcDataObject(id: String, registry: InstanceRegistry) = MockSparkDataObject(id)(registry)

  private def createTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): IcebergTableDataObject = {
    val table = Table(catalog = Some("iceberg1"), db = Some("default"), name = s"behaviour_$id", primaryKey = params.primaryKey)
    IcebergTableDataObject(id, path = Some(tempPath + s"/${table.fullName}"), partitions = params.partitions,
      options = params.options, table = table, constraints = params.constraints, expectations = params.expectations,
      saveMode = params.saveMode, allowSchemaEvolution = params.allowSchemaEvolution,
      housekeepingMode = params.housekeepingMode)(registry)
  }

  test("Write data") {
    testCopyLoad(createSrcDataObject, createTableDataObject)
  }

  test("Write data partitioned") {
    // movePartitions is not implemented by IcebergTableDataObject
    testCopyLoadPartitioned(createSrcDataObject, createTableDataObject, testMovePartitions = false)
  }

  test("SaveMode overwrite with different schema") {
    testOverwriteWithDifferentSchema(createTableDataObject)
  }

  test("SaveMode append with different schema") {
    testAppendWithDifferentSchema(createTableDataObject)
  }

  test("SaveMode overwrite and delete partition") {
    testOverwriteAndDeletePartition(createTableDataObject)
  }

  test("SaveMode overwrite partitions dynamically") {
    testOverwritePartitionsDynamically(createTableDataObject)
  }

  // PartitionArchiveMode is not tested, as IcebergTableDataObject does not implement movePartitions
  test("housekeeping partition retention") {
    testHousekeepingPartitionRetention(createTableDataObject)
  }

  test("SaveMode append") {
    testAppend(createTableDataObject)
  }

  test("throw NoDataToProcessWarning if no new snapshot created (no data)") {
    testNoDataToProcessWarningOnEmptyWrite(createTableDataObject)
  }

  test("SaveMode merge") {
    testMerge(createTableDataObject)
  }

  test("SaveMode merge with updateCols") {
    testMergeWithUpdateColumns(createTableDataObject)
  }

  test("SaveMode merge with schema evolution") {
    testMergeWithSchemaEvolution(createTableDataObject)
  }

  test("write with different order of columns") {
    testWriteWithDifferentColumnOrder(createTableDataObject)
  }

  test("returns correct metrics") {
    testWriteMetrics(createSrcDataObject, createTableDataObject)
  }

  test("copy load expectations test") {
    testCopyLoadWithExpectations(createSrcDataObject, createTableDataObject)
  }

  test("constraints validation") {
    testConstraints(createSrcDataObject, createTableDataObject)
  }

  // Note that this is not possible with DeltaLake 1.x, as schema evolution with mergeStmt.insertExpr is not properly supported.
  // We test for failure to be notified once it is working...
  // Once this works again, also enable 3rd load in IcebergHistorizeWithMergeActionTest and IcebergDeduplicateWithMergeActionTest test cases again
  test("SaveMode merge with updateCols and schema evolution - fails in deltalake 1.x") {
    val targetTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "test_merge", query = None, primaryKey = Some(Seq("tpe", "lastname", "firstname")))
    val targetTablePath = tempPath + s"/${targetTable.fullName}"
    val targetDO = IcebergTableDataObject(id = "target", path = Some(targetTablePath), table = targetTable, saveMode = SDLSaveMode.Merge, allowSchemaEvolution = true)
    targetDO.dropTable

    // first load
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    targetDO.writeSparkDataFrame(df1)
    val actual = targetDO.getSparkDataFrame()
    val resultat = df1.equal(actual)
    if (!resultat) printFailedTestResultDs("Df2HiveTable")(actual)(df1)
    assert(resultat)

    // 2nd load: merge data by primary key with different schema
    // - column 'rating' deleted -> existing records will keep column rating untouched (values are preserved and not set to null), new records will get new column rating set to null.
    // - column 'rating2' added -> existing records will get new column rating2 set to null
    val df2 = Seq(("ext", "doe", "john", 10), ("int", "emma", "brown", 7))
      .toDF("tpe", "lastname", "firstname", "rating2")
    intercept[AnalysisException](targetDO.writeSparkDataFrame(df2, saveModeOptions = Some(SaveModeMergeOptions(updateColumns = Seq("lastname", "firstname", "rating", "rating2")))))
  }

  test("normal output mode") {
    testNormalOutputModeWithoutCdc(createTableDataObject)
  }

  test("incremental output mode with inserts") {
    // iceberg snapshot ids used as state are not monotonically increasing
    testIncrementalOutputModeWithInserts(createTableDataObject, stateIsOrdered = false)
  }

  test("incremental output mode without primary keys") {
    testIncrementalOutputModeWithoutPrimaryKey(createTableDataObject)
  }

  test("incremental output mode with updates and inserts") {
    testIncrementalOutputModeWithUpdatesAndInserts(createTableDataObject)
  }

  // TODO: addFilesParallelism > 1 results in Iceberg NotSerializableException, see https://github.com/apache/iceberg/issues/11147
  test("Create from parquet files") {
    // Define Iceberg Table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_to_iceberg", query = None)
    val icebergConnection = IcebergTableConnection(id = "iceberg", db = "default", pathPrefix = tempPath, addFilesParallelism = Some(1))
    instanceRegistry.register(icebergConnection)
    val targetPath = icebergTable.name
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, connectionId = Some(icebergConnection.id))

    // Create parquet files
    val parquetConnection = HadoopFileConnection(id = "parquet", pathPrefix = tempPath)
    instanceRegistry.register(parquetConnection)
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, connectionId = Some(parquetConnection.id))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // Initialize Iceberg table
    icebergDO.prepare // does the table conversion

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }
  }

  test("Create from parquet files of legacy hive tables (c000 file ending)") {
    // Define Iceberg Table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_legacy_to_iceberg", query = None)
    val icebergConnection = IcebergTableConnection(id = "iceberg", db = "default", pathPrefix = tempPath, addFilesParallelism = Some(1))
    instanceRegistry.register(icebergConnection)
    val targetPath = icebergTable.name
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, connectionId = Some(icebergConnection.id))

    // Create parquet files
    val parquetConnection = HadoopFileConnection(id = "parquet", pathPrefix = tempPath)
    instanceRegistry.register(parquetConnection)
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, connectionId = Some(parquetConnection.id))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    parquetDO.filesystem.listStatus(parquetDO.hadoopPath)
      .filter(s => s.isFile && s.getPath.getName.endsWith(".snappy.parquet"))
      .map(_.getPath.toString)
      .foreach { p =>
        parquetDO.renameFile(p, p.stripSuffix(".snappy.parquet"))
      }

    // Initialize Iceberg table
    icebergDO.prepare // does the table conversion

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions.isEmpty)
    }
  }

  test("Create from parquet files partitioned of legacy hive tables (c000 file ending)") {
    // Define Iceberg Table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_partitioned_legacy_to_iceberg", query = None)
    val icebergConnection = IcebergTableConnection(id = "iceberg", db = "default", pathPrefix = tempPath, addFilesParallelism = Some(1))
    instanceRegistry.register(icebergConnection)
    val targetPath = icebergTable.name
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, partitions = Seq("tpe"), connectionId = Some(icebergConnection.id))

    // Create parquet files
    val parquetConnection = HadoopFileConnection(id = "parquet", pathPrefix = tempPath)
    instanceRegistry.register(parquetConnection)
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, partitions = Seq("tpe"), connectionId = Some(parquetConnection.id))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    HdfsUtil.listFiles(parquetDO.hadoopPath, recursive = true)(parquetDO.filesystem)
      .filter(s => s.isFile && s.getPath.getName.endsWith(".snappy.parquet"))
      .map(_.getPath.toString)
      .foreach { p =>
        parquetDO.renameFile(p, p.stripSuffix(".snappy.parquet"))
      }

    // Initialize Iceberg table
    icebergDO.prepare // does the table conversion

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }
  }

  test("Create from parquet files partitioned") {

    // Define Iceberg table
    val icebergTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "parquet_to_iceberg_partitioned")
    val targetPath = tempPath + s"/${icebergTable.name}"
    val icebergDO = IcebergTableDataObject(id = "iceberg", path = Some(targetPath), table = icebergTable, partitions = Seq("tpe"))

    // Create parquet files
    val parquetDO = ParquetFileDataObject(id = "parquet", path = targetPath, partitions = Seq("tpe"))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // Initialize Iceberg table
    icebergDO.prepare

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }
  }

  test("Write data with hadoop catalog to non-default db") {

    // setup DataObjects
    val sourceDO = MockSparkDataObject(id="source").register
    sourceDO.writeSparkDataFrame(
      Seq((Some(0),"Foo!"),(Some(1),"Bar!")).toDF("num","text")
    )
    val targetTable = Table(catalog = Some("iceberg_hadoop"), db = Some("test"), name = "custom_df_copy", query = None)
    val targetDO = IcebergTableDataObject(id = "target", path = None, table = targetTable)
    instanceRegistry.register(targetDO)

    // create hadoop catalog 'test' database
    val warehouseDir = new Path(session.conf.get(s"spark.sql.catalog.${targetTable.catalog.get}.warehouse"))
    val fs = SparkHdfsUtil.getHadoopFsFromSpark(warehouseDir)
    fs.mkdirs(new Path(warehouseDir, targetTable.db.get))

    // prepare DataObject
    targetDO.prepare

    // prepare & start load
    val testAction = io.smartdatalake.workflow.action.CopyAction(id = s"load", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = io.smartdatalake.workflow.dataframe.spark.SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))(contextExec)

    val expected = sourceDO.getSparkDataFrame()
    val actual = targetDO.getSparkDataFrame()
    assert(actual.equal(expected))
  }

  test("Create from parquet files on hadoop catalog") {

    // Setup Iceberg table
    val icebergTable = Table(catalog = Some("iceberg_hadoop"), db = Some("default"), name = "parquet_to_iceberg")
    val icebergDO = IcebergTableDataObject(id = "iceberg", table = icebergTable)
    // the hadoop catalog warehouse is persisted in the target directory. Drop a table left over by a previous test run,
    // otherwise writing the parquet files below would delete its metadata behind the back of the catalog.
    icebergDO.dropTable

    // Create parquet files
    val parquetDO = ParquetFileDataObject(id = "parquet", path = icebergDO.hadoopPath.toString)
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // initialize Iceberg table
    icebergDO.prepare

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
    }
  }

  test("Create from parquet files partitioned on hadoop catalog") {

    // Setup Iceberg table
    val icebergTable = Table(catalog = Some("iceberg_hadoop"), db = Some("default"), name = "parquet_to_iceberg_partitioned")
    val icebergDO = IcebergTableDataObject(id = "iceberg", table = icebergTable, partitions = Seq("tpe"))
    // drop a table left over by a previous test run, see comment in the test above
    icebergDO.dropTable

    // Create parquet files
    val parquetDO = ParquetFileDataObject(id = "parquet", path = icebergDO.hadoopPath.toString, partitions = Seq("tpe"))
    val df1 = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3))
      .toDF("tpe", "lastname", "firstname", "rating")
    parquetDO.writeSparkDataFrame(df1)

    // initialize Iceberg table
    icebergDO.prepare

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }

    icebergDO.initSparkDataFrame(df1, Seq())
    icebergDO.writeSparkDataFrame(df1, Seq())(contextExec)

    {
      val df = icebergDO.getSparkDataFrame()
      assert(df.equal(df1))
      assert(icebergDO.listPartitions == Seq(PartitionValues(Map("tpe" -> "ext"))))
    }
  }

  // shared behaviours for managing tables in the catalog at deployment time, see issue #1129.
  // Note that Iceberg has no primary or foreign key constraints.

  private def createCatalogMetadataDataObject(id: String, params: CatalogMetadataTestParams, registry: InstanceRegistry): IcebergTableDataObject = {
    val table = params.createTable(catalog = Some("iceberg1"), db = Some("default"))
    IcebergTableDataObject(id, path = Some(tempPath + s"/${table.fullName}"), table = table,
      metadata = params.dataObjectMetadata)(registry)
  }

  test("create a missing table") {
    testCreateMissingTable(createCatalogMetadataDataObject)
  }

  test("evolve the schema of an existing table") {
    testEvolveSchema(createCatalogMetadataDataObject)
  }
}
