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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.workflow.SparkSubFeed
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.connection.{DefaultSQLCatalog, JdbcTableConnection}

class JdbcTableDataObjectTest extends DataObjectTestSuite {

  import session.implicits._

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:file:target/JdbcTableDataObjectTest/hsqldb", "org.hsqldb.jdbcDriver")

  test("write and read jdbc table") {
    import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject( "jdbcDO1", table = table, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    dataObject.writeDataFrame(df, Seq())
    val dfRead = dataObject.getDataFrame(Seq())
    assert(dfRead.symmetricDifference(df).isEmpty)
  }

  test("write and read case sensitive jdbc table") {
    import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
    instanceRegistry.register(jdbcConnection)
    // Use double quotes for case sensitivity in HSQLDB
    val table = Table(Some("\"PUBLIC\""), "\"CaseSensitiveTable1\"")
    val dataObject = JdbcTableDataObject( "jdbcDO1", table = table, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    dataObject.writeDataFrame(df, Seq())
    val dfRead = dataObject.getDataFrame(Seq())
    assert(dfRead.symmetricDifference(df).isEmpty)
  }

  test("check pre/post sql") {
    import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
    instanceRegistry.register(jdbcConnection)

    val table1 = Table(Some("public"), "table1")
    val srcDO = JdbcTableDataObject( "jdbcDO1", table = table1, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"type varchar(255), lastname varchar(255), firstname varchar(255)")
      , preReadSql = Some(s"insert into ${table1.fullName} values ('preRead','smith','%{feed}',3);")
      , postReadSql = Some(s"insert into ${table1.fullName} values ('postRead','smith','%{feed}',3);")
      , preWriteSql = Some(s"insert into ${table1.fullName} values ('preWrite','smith','%{feed}',3);") // should not be inserted on src
      , postWriteSql = Some(s"insert into ${table1.fullName} values ('postWrite','smith','%{feed}',3);") // should not be inserted on src
    )
    srcDO.dropTable
    val df = Seq(("ext","doe","john",5)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq())
    instanceRegistry.register(srcDO)

    val tgtDO = JdbcTableDataObject( "jdbcDO2", table = Table(Some("public"), "table2"), connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"type varchar(255), lastname varchar(255), firstname varchar(255)")
      , preReadSql = Some(s"insert into ${table1.fullName} values ('preRead','emma','%{feed}',3);") // should not be inserted on tgt
      , postReadSql = Some(s"insert into ${table1.fullName} values ('postRead','emma','%{feed}',3);") // should not be inserted on tgt
      , preWriteSql = Some(s"insert into ${table1.fullName} values ('preWrite','emma','%{feed}',3);")
      , postWriteSql = Some(s"insert into ${table1.fullName} values ('postWrite','emma','%{feed}',3);")
    )
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, srcDO.id, Seq())
    action1.preExec(Seq(srcSubFeed))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(session,contextExec).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))

    val dfSrcExpected = Seq(("ext", "doe", "john", 5)
      , ("preRead", "smith", "feedTest", 3), ("preWrite", "emma", "feedTest", 3)
      , ("postRead", "smith", "feedTest", 3), ("postWrite", "emma", "feedTest", 3)
    ).toDF("type", "lastname", "firstname", "rating")
    srcDO.getDataFrame().symmetricDifference(dfSrcExpected).show
    assert(srcDO.getDataFrame().symmetricDifference(dfSrcExpected).isEmpty)
  }

  // query parameter doesn't work with hsqldb
  ignore("read jdbc table with query") {
    import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
    instanceRegistry.register(jdbcConnection)

    // prepare data
    val table1 = Table(Some("public"), "table1")
    val dataObject1 = JdbcTableDataObject( "jdbcDO1", table = table1, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"type varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject1.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    dataObject1.writeDataFrame(df, Seq())

    // read prepared data
    val table2 = Table(Some("public"), "table1", query = Some("select lastname, firstname from public.table1"))
    val dataObject2 = JdbcTableDataObject( "jdbcDO2", table = table2, connectionId = "jdbcCon1", jdbcOptions = Map("createTableColumnTypes"->"type varchar(255), lastname varchar(255), firstname varchar(255)"))
    val dfRead = dataObject2.getDataFrame(Seq())
    assert(dfRead.symmetricDifference(df.select($"lastname", $"firstname")).isEmpty)

    // assert cannot write to DataObject with query defined
    intercept[IllegalArgumentException](dataObject2.writeDataFrame(df, Seq()))
  }

  test("isTableExisting should return not only the table but also the view - read jdbc:hsqldb view and table") {
    import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
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

      val dfReadView = dataObjectView.getDataFrame(Seq())
      val dfReadTable = dataObjectTable.getDataFrame(Seq())

      val df = Seq(("test_data")).toDF("test_column")
      assert(jdbcConnection.catalog.asInstanceOf[DefaultSQLCatalog].isTableExisting(db, view.name))
      assert(jdbcConnection.catalog.asInstanceOf[DefaultSQLCatalog].isTableExisting(db, table.name))
      assert(dfReadView.symmetricDifference(df).isEmpty)
      assert(dfReadTable.symmetricDifference(df).isEmpty)

    } finally {
      jdbcConnection.execJdbcStatement(sql = "DROP view if exists test_view_191;")
      jdbcConnection.execJdbcStatement(sql = "DROP table if exists test_table_191;")
    }
  }

  test("list jdbc table virtual partitions") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject( "jdbcDO1", table = table, connectionId = "jdbcCon1", virtualPartitions = Seq("abc"), jdbcOptions = Map("createTableColumnTypes"->"abc varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable
    // Be careful when writing lower case column names over Jdbc with Spark. When creating the table through Spark they will be surrounded with quotes and become case-sensitiv!
    // In consequence the virtual partition has to be surrounded with quotes as well, see next test case.
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("ABC", "lastname", "firstname", "rating")
    dataObject.writeDataFrame(df, Seq())
    dataObject.prepare
    dataObject.getDataFrame(Seq()).show
    assert(dataObject.isTableExisting)
    val partitionValues = dataObject.listPartitions
    assert(partitionValues.size == 2)
    assert(partitionValues.map(_.elements("abc")).toSet == Set("ext","int"))
  }

  test("list jdbc table virtual partitions case sensitive") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("public"), "table1")
    val dataObject = JdbcTableDataObject( "jdbcDO1", table = table, connectionId = "jdbcCon1", virtualPartitions = Seq("\"abc\""), jdbcOptions = Map("createTableColumnTypes"->"abc varchar(255), lastname varchar(255), firstname varchar(255)"))
    dataObject.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("abc", "lastname", "firstname", "rating")
    dataObject.writeDataFrame(df, Seq())
    dataObject.prepare
    dataObject.getDataFrame(Seq()).show
    assert(dataObject.isTableExisting)
    val partitionValues = dataObject.listPartitions
    assert(partitionValues.size == 2)
    assert(partitionValues.map(_.elements("abc")).toSet == Set("ext","int"))
  }
}
