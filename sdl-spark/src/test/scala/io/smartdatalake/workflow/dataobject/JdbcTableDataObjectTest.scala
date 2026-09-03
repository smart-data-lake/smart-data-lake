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
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.custom.TestCustomDfsTransformer
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestTool, SparkTestUtil}
import io.smartdatalake.testutils.{DataObjectTestSuite, TableDataObjectBehaviour, TableDataObjectTestParams}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.GetSession.loggEnv
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfsTransformer
import io.smartdatalake.workflow.action.{ActionMetadata, CopyAction, CustomDataFrameAction}
import io.smartdatalake.workflow.connection.jdbc.{DefaultJdbcCatalog, JdbcTableConnection}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.generic.Table
import org.slf4j.Logger

import java.nio.file.Files

/**
 * Tests JdbcTableDataObject against an in-memory HSQLDB. Behaviours shared with the other table DataObjects
 * are instantiated from TableDataObjectBehaviour, the remaining tests cover JDBC specifics like queries,
 * pre/post SQL and virtual partitions.
 */
class JdbcTableDataObjectTest extends DataObjectTestSuite with SparkTestTool
  with SmartDataLakeLogger with TableDataObjectBehaviour {

  @transient implicit private lazy val implicitLogger: Logger = logger

  import session.implicits._

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcTableDataObjectTest", "org.hsqldb.jdbcDriver")
  private val tempDir = Files.createTempDirectory("test")

  loggEnv

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  private def createSrcDataObject(id: String, registry: InstanceRegistry) = MockSparkDataObject(id)(registry)

  /**
   * Creates the JdbcTableDataObject under test and drops its table, so every behaviour starts with an empty database.
   * Note that the behaviour tests reuse the same DataObject ids, and therefore also the same table names.
   */
  private def createTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): JdbcTableDataObject = {
    registry.register(jdbcConnection)
    val table = Table(db = Some("public"), name = s"behaviour_$id", primaryKey = params.primaryKey)
    val dataObject = JdbcTableDataObject(id, table = table, connectionId = jdbcConnection.id,
      virtualPartitions = params.partitions, saveMode = params.saveMode, allowSchemaEvolution = params.allowSchemaEvolution,
      constraints = params.constraints, expectations = params.expectations, housekeepingMode = params.housekeepingMode)(registry)
    dataObject.dropTable(contextExec)
    dataObject
  }

  test("write and read jdbc table") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes" -> "type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7)).toDF("type", "lastname", "firstname", "rating")
    dataObject.initSparkDataFrame(df, Seq())
    dataObject.writeSparkDataFrame(df)
    val dfRead = dataObject.getSparkDataFrame(Seq())(contextExec)
    assert(dfRead.getSymmetricDifference(df).isEmpty)
  }

  test("write and read case insensitive jdbc table") {
    instanceRegistry.register(jdbcConnection)
    // Use double quotes for case sensitivity in HSQLDB
    val table = Table(Some("\"PUBLIC\""), "\"CaseSensitiveTable1\"")
    val dataObject = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes" -> "type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7)).toDF("type", "lastname", "firstname", "rating")
    dataObject.initSparkDataFrame(df, Seq())
    dataObject.writeSparkDataFrame(df)
    val dfRead = dataObject.getSparkDataFrame(Seq())(contextExec)
    assert(dfRead.getSymmetricDifference(df).isEmpty)
    dataObject.deleteAllData()
  }

  test("check pre/post sql") {
    instanceRegistry.register(jdbcConnection)

    val table1 = Table(Some("public"), "table1")
    val srcDO = JdbcTableDataObject("jdbcDO1", table = table1, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes" -> "type varchar(255), lastname varchar(255), firstname varchar(255)")
      , preReadSql = Some(s"insert into ${table1.fullName} values ('preRead','smith','%{feed}',3);")
      , postReadSql = Some(s"insert into ${table1.fullName} values ('postRead','smith','%{feed}',3);")
      , preWriteSql = Some(s"insert into ${table1.fullName} values ('preWrite','smith','%{feed}',3);") // should not be inserted on src
      , postWriteSql = Some(s"insert into ${table1.fullName} values ('postWrite','smith','%{feed}',3);") // should not be inserted on src
    )
    srcDO.dropTable
    val df = Seq(("ext", "doe", "john", 5)).toDF("type", "lastname", "firstname", "rating")
    srcDO.initSparkDataFrame(df, Seq())
    srcDO.writeSparkDataFrame(df)
    instanceRegistry.register(srcDO)

    val tgtDO = JdbcTableDataObject("jdbcDO2", table = Table(Some("public"), "table2"), connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes" -> "type varchar(255), lastname varchar(255), firstname varchar(255)")
      , preReadSql = Some(s"insert into ${table1.fullName} values ('preRead','emma','%{feed}',3);") // should not be inserted on tgt
      , postReadSql = Some(s"insert into ${table1.fullName} values ('postRead','emma','%{feed}',3);") // should not be inserted on tgt
      , preWriteSql = Some(s"insert into ${table1.fullName} values ('preWrite','emma','%{feed}',3);")
      , postWriteSql = Some(s"insert into ${table1.fullName} values ('postWrite','emma','%{feed}',3);")
    )
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, srcDO.id, Seq())
    action1.init(Seq(srcSubFeed))
    action1.preExec(Seq(srcSubFeed))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))

    val dfSrcExpected = Seq(("ext", "doe", "john", 5)
      , ("preRead", "smith", "feedTest", 3), ("preWrite", "emma", "feedTest", 3)
      , ("postRead", "smith", "feedTest", 3), ("postWrite", "emma", "feedTest", 3)
    ).toDF("type", "lastname", "firstname", "rating")
    assert(srcDO.getSparkDataFrame()(contextExec).getSymmetricDifference(dfSrcExpected).isEmpty)
  }

  test("read jdbc table with query") {
    instanceRegistry.register(jdbcConnection)

    // prepare data
    val table1 = Table(Some("public"), "table1")
    val dataObject1 = JdbcTableDataObject("jdbcDO1", table = table1, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes" -> "type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject1.dropTable
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7)).toDF("type", "lastname", "firstname", "rating")
    dataObject1.initSparkDataFrame(df, Seq())(contextInit)
    dataObject1.writeSparkDataFrame(df)(contextExec)

    // read prepared data
    val table2 = Table(Some("public"), "table2", query = Some("select lastname, firstname from public.table1 where type = 'ext'"))
    val dataObject2 = JdbcTableDataObject("jdbcDO2", table = table2, connectionId = "jdbcCon1")
    val actual = dataObject2.getSparkDataFrame(Seq())(contextExec)
    val expected = df.select($"lastname", $"firstname").where($"type" === "ext")
    val resultat = actual.equal(expected)
    assert(resultat)

    // assert cannot write to DataObject with query defined
    intercept[IllegalArgumentException](dataObject2.writeSparkDataFrame(df)(contextExec))
  }

  // query parameter doesn't work with hsqldb
  test("copy from jdbc table with query and where clause") {
    instanceRegistry.register(jdbcConnection)

    // prepare data
    val table1 = Table(Some("public"), "table1")
    val dataObject1 = JdbcTableDataObject("jdbcDO1", table = table1, connectionId = "jdbcCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject1.dropTable
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7))
      .toDF("type", "lastname", "firstname", "rating")
    dataObject1.initSparkDataFrame(df, Seq())(contextInit)
    dataObject1.writeSparkDataFrame(df)(contextExec)

    // prepare view dataObject
    val table2 = Table(Some("public"), "table2",
      query = Some("select lastname, firstname from public.table1 where type = 'ext'"))
    val srcDO = JdbcTableDataObject("jdbcDO2", table = table2, connectionId = "jdbcCon1")
    instanceRegistry.register(srcDO)
    val tgtDO = MockSparkDataObject("tgt").register
    instanceRegistry.register(tgtDO)

    val action = CopyAction("copy", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, srcDO.id, Seq())
    action.init(Seq(srcSubFeed))(contextInit).head
    action.exec(Seq(srcSubFeed))(contextExec).head

    val actual = srcDO.getSparkDataFrame(Seq())(contextExec)
    val expected = tgtDO.getSparkDataFrame(Seq())(contextExec)
    val resultat = actual.equal(expected)
    assert(resultat)
  }

  // query parameter doesn't work with hsqldb
  test("custom transformation of jdbc table with query and where clause") {
    instanceRegistry.register(jdbcConnection)

    // prepare data
    val allData = Table(db = Some("public"), name = "allData")
    val dataObjectAll = JdbcTableDataObject(id = "jdbcAll", table = allData, connectionId = "jdbcCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "id int, text varchar(255)"))
    dataObjectAll.dropTable
    val dfAll = List((1, "abc"), (2, "def"), (3, "abc"), (4, "ghi"), (5, "abc"), (6, "def")).toDF("id", "text")
    dataObjectAll.initSparkDataFrame(dfAll, Nil)(contextInit)
    dataObjectAll.writeSparkDataFrame(dfAll, Nil)(contextExec)

    // prepare view dataObject as source
    val filteredData = Table(db = Some("public"), name = "filteredData",
      query = Some("select id, text from public.allData where text<'g'"))
    val srcDO = JdbcTableDataObject(id = "srcData", table = filteredData, connectionId = "jdbcCon1")
    instanceRegistry.register(srcDO)

    // prepare target
    val targetTab = Table(db = Some("public"), name = "target")
    val tgtDO = JdbcTableDataObject(id = "target", table = targetTab, connectionId = "jdbcCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "id int, text varchar(255), _rnk int"))
    tgtDO.dropTable
    val expected = List((1, "abc", 1), (2, "def", 1), (3, "abc", 2), (5, "abc", 3), (6, "def", 2))
      .toDF("id", "text", "_rnk")
    tgtDO.initSparkDataFrame(expected.where($"id"===1), Nil)(contextInit)
    tgtDO.writeSparkDataFrame(expected.where($"id"===1), Nil)(contextExec)
    instanceRegistry.register(tgtDO)

    val action = CustomDataFrameAction(id = "jdbcTransform",
      inputIds = List(srcDO.id), outputIds = Seq(tgtDO.id),
      metadata = Some(ActionMetadata(feed = Some("jdbcTransform"))),
      transformers = List(ScalaClassSparkDfsTransformer(className = classOf[TestCustomDfsTransformer].getName))
    )
    val srcSubFeed = SparkSubFeed(None, srcDO.id, Nil)
    action.init(Seq(srcSubFeed))(contextInit).head
    action.exec(Seq(srcSubFeed))(contextExec).head

    val actual = tgtDO.getSparkDataFrame(Nil)(contextExec).orderBy($"id")
    assert(actual.equal(expected))
  }

  test("isTableExisting should return not only the table but also the view - read jdbc:hsqldb view and table") {
    instanceRegistry.register(jdbcConnection)
    try {
      val db = "public"
      val view = Table(Some(db), "test_view_191")
      val dataObjectView = JdbcTableDataObject("jdbcDO1", table = view, connectionId = "jdbcCon1")
      dataObjectView.dropTable
      val table = Table(Some(db), "test_table_191")
      val dataObjectTable = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1")
      dataObjectTable.dropTable

      jdbcConnection.execJdbcStatement(sql = "create view test_view_191 as (SELECT 'test_data' AS test_column from (values(0)));")
      jdbcConnection.execJdbcStatement(sql = "create table test_table_191 (test_column char(9));")
      jdbcConnection.execJdbcStatement(sql = "insert into test_table_191 (test_column) VALUES ('test_data');")

      val dfReadView = dataObjectView.getSparkDataFrame(Seq())
      val dfReadTable = dataObjectTable.getSparkDataFrame(Seq())

      val df = Seq("test_data").toDF("test_column")
      assert(jdbcConnection.catalog.asInstanceOf[DefaultJdbcCatalog].isTableExisting(s"$db.${view.name}"))
      assert(jdbcConnection.catalog.asInstanceOf[DefaultJdbcCatalog].isTableExisting(s"$db.${table.name}"))
      assert(dfReadView.getSymmetricDifference(df).isEmpty)
      assert(dfReadTable.getSymmetricDifference(df).isEmpty)

    } finally {
      jdbcConnection.execJdbcStatement(sql = "DROP view if exists test_view_191;")
      jdbcConnection.execJdbcStatement(sql = "DROP table if exists test_table_191;")
    }
  }

  test("list jdbc table virtual partitions") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1", virtualPartitions = Seq("abc"), jdbcOptions = Map("createTableColumnTypes" -> "abc varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable

    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7)).toDF("abc", "lastname", "firstname", "rating")
    dataObject.initSparkDataFrame(df, Seq())
    dataObject.writeSparkDataFrame(df)
    dataObject.prepare
    assert(dataObject.isTableExisting)
    val partitionValues = dataObject.listPartitions(contextExec)
    assert(partitionValues.toSet == Set(PartitionValues(Map("abc" -> "ext")), PartitionValues(Map("abc" -> "int"))))
  }

  test("list jdbc table virtual partitions case quoted identifier") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1",
      virtualPartitions = Seq("abc"),
      createSql = Some("""CREATE TABLE public.table1 ("aBc" varchar(255) , lastname varchar(255) , firstname varchar(255) , rating INTEGER NOT NULL)"""))
    dataObject.dropTable
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7)).toDF("abc", "lastname", "firstname", "rating")
    dataObject.prepare
    dataObject.initSparkDataFrame(df, Seq())
    dataObject.writeSparkDataFrame(df)
    assert(dataObject.isTableExisting)
    val partitionValues = dataObject.listPartitions(contextExec)
    assert(partitionValues.toSet == Set(PartitionValues(Map("abc" -> "ext")), PartitionValues(Map("abc" -> "int"))))
    dataObject.getSparkDataFrame().select($"abc").collect()
  }

  test("incremental output mode") {

    // create data object
    instanceRegistry.register(jdbcConnection)
    val targetTable = Table(db = Some("public"), name = "test_inc")
    val targetDO = JdbcTableDataObject("jdbcDO1", table = targetTable, connectionId = "jdbcCon1", incrementalOutputExpr = Some("id + 1"), saveMode = SDLSaveMode.Append)
    targetDO.dropTable

    // write test data 1
    val df1 = Seq((1, "A", 1), (2, "A", 2), (3, "B", 3), (4, "B", 4)).toDF("id", "p", "value")
    targetDO.prepare
    targetDO.initSparkDataFrame(df1, Seq())
    targetDO.writeSparkDataFrame(df1)

    // test 1
    targetDO.setState(None) // initialize incremental output with empty state
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4
    val newState1 = targetDO.getState

    // append test data 2
    val df2 = Seq((5, "B", 5)).toDF("id", "p", "value")
    targetDO.writeSparkDataFrame(df2)

    // test 2
    targetDO.setState(newState1)
    val df2result = targetDO.getSparkDataFrame()(contextExec)
    df2result.count() shouldEqual 1
    val newState2 = targetDO.getState
    assert(newState1.get < newState2.get)

    // disable incremental output and query all data
    targetDO.setState(None)
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 5
  }

  // see logs to manually assure that no temp table is created and the configuration is correct.
  test("write to jdbc table with directTableOverwrite=true") {
    instanceRegistry.register(jdbcConnection.copy(directTableOverwrite = true))
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1", saveMode = SDLSaveMode.Overwrite)
    dataObject.dropTable
    val df = Seq(("ext", "doe", "john", 5), ("ext", "smith", "peter", 3), ("int", "emma", "brown", 7)).toDF("type", "lastname", "firstname", "rating")
    dataObject.initSparkDataFrame(df, Seq())
    dataObject.writeSparkDataFrame(df)
    val dfRead = dataObject.getSparkDataFrame(Seq())(contextExec)
    assert(dfRead.getSymmetricDifference(df).isEmpty)
  }

  test("move virtual partitions with multiple partition columns") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table_move_partitions")
    val dataObject = JdbcTableDataObject("jdbcDO1", table = table, connectionId = "jdbcCon1",
      virtualPartitions = Seq("dt", "region"),
      jdbcOptions = Map("createTableColumnTypes" -> "dt varchar(255), region varchar(255), lastname varchar(255)"))
    dataObject.dropTable

    val df = Seq(("20201101", "ch", "doe", 5), ("20201201", "ch", "einstein", 2), ("20201201", "de", "smith", 3))
      .toDF("dt", "region", "lastname", "rating")
    dataObject.initSparkDataFrame(df, Seq())
    dataObject.writeSparkDataFrame(df)

    // move dt=20201101/region=ch into the existing partition dt=20201201/region=ch
    dataObject.movePartitions(Seq(
      PartitionValues(Map("dt" -> "20201101", "region" -> "ch")) -> PartitionValues(Map("dt" -> "20201201", "region" -> "ch"))
    ))(contextExec)

    assert(dataObject.listPartitions(contextExec).toSet == Set(
      PartitionValues(Map("dt" -> "20201201", "region" -> "ch")),
      PartitionValues(Map("dt" -> "20201201", "region" -> "de"))
    ))
    val actual = dataObject.getSparkDataFrame()(contextExec)
    val expected = Seq(("20201201", "ch", "doe", 5), ("20201201", "ch", "einstein", 2), ("20201201", "de", "smith", 3))
      .toDF("dt", "region", "lastname", "rating")
    assert(actual.getSymmetricDifference(expected).isEmpty)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // Behaviours shared with the other table DataObjects, see TableDataObjectBehaviour.
  //
  // Not instantiated for JdbcTableDataObject:
  // - testOverwriteAndDeletePartition / testOverwritePartitionsDynamically / testNoDataToProcessWarningOnEmptyWrite:
  //   they rely on option partitionOverwriteMode, which JdbcTableDataObject does not implement.
  // - testOverwriteWithDifferentSchema / testAppendWithDifferentSchema: they expect deleted columns to disappear,
  //   while JdbcTableDataObject keeps them and makes them nullable, see evolveTableSchema.
  // - the incremental output behaviours: JdbcTableDataObject implements incremental output with a
  //   high-water-mark expression evaluated on read (incrementalOutputExpr) instead of table versions,
  //   see the JDBC specific test "incremental output mode" above.
  //////////////////////////////////////////////////////////////////////////////////////

  test("write data") {
    // JdbcTableDataObject implements neither table nor column statistics
    testCopyLoad(createSrcDataObject, createTableDataObject, expectColumnStats = false, expectTableStats = false)
  }

  test("write data partitioned") {
    testCopyLoadPartitioned(createSrcDataObject, createTableDataObject)
  }

  test("SaveMode append") {
    testAppend(createTableDataObject)
  }

  test("SaveMode merge") {
    testMerge(createTableDataObject)
  }

  test("SaveMode merge with schema evolution") {
    testMergeWithSchemaEvolution(createTableDataObject)
  }

  test("SaveMode merge with updateCols") {
    testMergeWithUpdateColumns(createTableDataObject)
  }

  test("write with different order of columns") {
    testWriteWithDifferentColumnOrder(createTableDataObject)
  }

  test("constraints validation") {
    testConstraints(createSrcDataObject, createTableDataObject)
  }

  test("returns correct metrics") {
    // Spark reports no written bytes for JDBC writes
    testWriteMetrics(createSrcDataObject, createTableDataObject, expectBytesWritten = false)
  }

  test("copy load expectations test") {
    testCopyLoadWithExpectations(createSrcDataObject, createTableDataObject)
  }

  test("housekeeping partition retention") {
    testHousekeepingPartitionRetention(createTableDataObject)
  }

  test("housekeeping partition archive") {
    testHousekeepingPartitionArchive(createTableDataObject)
  }

}
