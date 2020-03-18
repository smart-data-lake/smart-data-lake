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

import com.holdenkarau.spark.testing.Utils.createTempDir
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.dataobject.PKViolatorsDataObject.colListType
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._
import scala.collection.mutable._

class PKviolatorDOtest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger{

  protected implicit val session: SparkSession = sessionHiveCatalog

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  val tempDir: java.io.File = createTempDir()
  val tempPath: String = tempDir.toPath.toAbsolutePath.toString

  def colsField(colName: String, noColumnsPossible: Boolean): StructField = StructField(colName,colListType(noColumnsPossible),nullable = false)

  val nullDataCol: Column =  lit(null).cast(colListType(true))
  val PKviolatorSchemaWithOutDataCol: StructType = StructType(StructField("data_object_id", StringType, nullable = false) ::
    StructField("db", StringType, nullable = false) ::
    StructField("table", StringType, nullable = false) ::
    StructField("schema", StringType, nullable = false) ::
    colsField("key", noColumnsPossible = false) ::
    Nil)
  val PKviolatorSchema: StructType = PKviolatorSchemaWithOutDataCol.add(colsField("data", noColumnsPossible = true))


  before { instanceRegistry.clear() }

  test("PKviolatorDO_PKid") {
    val sourceDo = createHiveTable(tableName= "source_table", dirPath= tempPath, df= dfNonUniqueWithNull, primaryKeyColumns= Some(Seq("id")))
    instanceRegistry.register(sourceDo)

    // actual: reading the table containing the PK violators
    val actual = PKViolatorsDataObject("pkViol").getDataFrame()

    // creating expected
    val rows_expected: java.util.List[Row] = ArrayBuffer(
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","2let")),Seq(("value","doublet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","2let")),Seq(("value","doublet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","3let")),Seq(("value","triplet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","3let")),Seq(("value","triplet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","3let")),Seq(("value","triplet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet"))),
      Row("source_tableDO","default","source_table","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet")))
    ).asJava
    val expected = session.createDataFrame(rows_expected,PKviolatorSchema)

    // Comparing actual with expected
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("PKviolatorDO_PKid",Seq(sourceDo.getDataFrame()))(actual)(expected)
    assert(resultat)
  }

  test("PKviolators_noDataColumns") {
    // creating and registering data object //
    val hiveTablePKidValueDO = createHiveTable(tableName= "hive_table_pk_id_Value", dirPath= tempPath, df= dfNonUniqueWithNull, primaryKeyColumns= Some(Seq("id","value")))

    // actual: reading the table containing the PK violators
    val actual = PKViolatorsDataObject("pkViol").getDataFrame()

    // creating expected
    val rows_expected: java.util.List[Row] = ArrayBuffer(
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","0let"),("value",null))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","2let"),("value","doublet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","2let"),("value","doublet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","3let"),("value","triplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","3let"),("value","triplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","3let"),("value","triplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet")))
    ).asJava
    val expected = session.createDataFrame(rows_expected,PKviolatorSchemaWithOutDataCol)
      .withColumn("data",nullDataCol)

    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("PKviolators_noDataColumns",
      Seq(hiveTablePKidValueDO.getDataFrame()))(actual)(expected)
    assert(resultat)
  }

  test("PKviolators_multipleDOs") {

    // creating and registering data objects //

    // a custom data object
    val customDO = CustomDfDataObject(id="custom_do",
      creator = CustomDfCreatorConfig(className = Some("io.smartdatalake.config.TestCustomDfNonUniqueWithNullCreator")))
    instanceRegistry.register(customDO)

    val hiveTablePKidDO = createHiveTable(tableName= "hive_table_pk_id", dirPath= tempPath, df= dfNonUniqueWithNull, primaryKeyColumns= Some(Seq("id")))
    val hiveTableNoPKDO = createHiveTable(tableName= "hive_table_no_pk", dirPath= tempPath, df= dfTwoCandidateKeys)
    val hiveTablePKidValueDO = createHiveTable(tableName= "hive_table_pk_id_Value", dirPath= tempPath, df= dfNonUniqueWithNull, primaryKeyColumns= Some(Seq("id","value")))

    // actual: reading the table containing the PK violators
    val actual = PKViolatorsDataObject("pkViol").getDataFrame()
    println("actual:")
    actual.printSchema()

    // creating expected
    val rows_expectedWithData: java.util.List[Row] = ArrayBuffer(
      // PKviolators of hiveTablePKidDO
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","2let")),Seq(("value","doublet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","2let")),Seq(("value","doublet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","3let")),Seq(("value","triplet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","3let")),Seq(("value","triplet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","3let")),Seq(("value","triplet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet"))),
      Row("hive_table_pk_idDO","default","hive_table_pk_id","`id` STRING,`value` STRING",Seq(("id","4let")),Seq(("value","quatriplet")))
    ).asJava

    // PKviolators of hiveTablePKidValueDO
    val rows_expectedWithOutData: java.util.List[Row] = ArrayBuffer(
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","0let"),("value",null))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","2let"),("value","doublet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","2let"),("value","doublet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","3let"),("value","triplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","3let"),("value","triplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","3let"),("value","triplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet"))),
      Row("hive_table_pk_id_ValueDO","default","hive_table_pk_id_Value","`id` STRING,`value` STRING",Seq(("id","4let"),("value","quatriplet")))
    ).asJava

    val expected = session.createDataFrame(rows_expectedWithData,PKviolatorSchema)
      .union(session.createDataFrame(rows_expectedWithOutData,PKviolatorSchemaWithOutDataCol).withColumn("data",nullDataCol))

    println("expected:")
    expected.printSchema()

    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("PKviolators_multipleDOs",
      Seq(customDO.getDataFrame(),hiveTablePKidDO.getDataFrame(),hiveTableNoPKDO.getDataFrame(),hiveTablePKidValueDO.getDataFrame()))(actual)(expected)
    assert(resultat)
  }

}
