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

import java.time.LocalDateTime

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.connection.JdbcTableConnection

class JdbcTableDataObjectTest extends DataObjectTestSuite {

  import testSession.implicits._

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

    val refTimestamp1 = LocalDateTime.now()
    implicit val context1: ActionPipelineContext = ActionPipelineContext("jdbcTest", "test", 1, 1, instanceRegistry, Some(refTimestamp1), SmartDataLakeBuilderConfig())
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, srcDO.id, Seq())
    action1.preExec(Seq(srcSubFeed))
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))

    val dfSrcExpected = Seq(("ext","doe","john",5)
      ,("preRead","smith","jdbcTest",3),("preWrite","emma","jdbcTest",3)
      ,("postRead","smith","jdbcTest",3),("postWrite","emma","jdbcTest",3)
    ).toDF("type", "lastname", "firstname", "rating")
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
}
