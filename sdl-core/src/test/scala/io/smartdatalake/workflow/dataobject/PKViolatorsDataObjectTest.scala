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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.testutils.custom.TestCustomDfNonUniqueWithNullCreator
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.reflect.runtime.universe.typeOf

class PKViolatorsDataObjectTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger{

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  before { instanceRegistry.clear() }

  test("normal pk violations") {
    val src = MockDataObject("source_tableDO", tableName = "source_table", primaryKey = Some(Seq("id"))).register
    src.writeSparkDataFrame(dfNonUniqueWithNull)

    // actual: reading the table containing the PK violators
    val actual = PKViolatorsDataObject("pkViol").getDataFrame(Seq(), typeOf[SparkSubFeed]).asInstanceOf[SparkDataFrame]


    // creating expected
    val rows_expected = Seq(
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","2let")),Seq(TestKV("value","doublet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","2let")),Seq(TestKV("value","doublet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","3let")),Seq(TestKV("value","triplet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","3let")),Seq(TestKV("value","triplet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","3let")),Seq(TestKV("value","triplet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet"))),
      TestData("source_tableDO","mock","source_table","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet")))
    )
    val expected = SparkDataFrame(rows_expected.toDF)

    // Comparing actual with expected
    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("normal pk violations",Seq(src.getSparkDataFrame()))(actual.inner)(expected.inner)
    assert(resultat)
  }

  test("pk violations with null values") {
    // creating and registering data object //
    val src = MockDataObject("hive_table_pk_id_ValueDO", tableName = "hive_table_pk_id_Value", primaryKey = Some(Seq("id","value"))).register
    src.writeSparkDataFrame(dfNonUniqueWithNull)

    // actual: reading the table containing the PK violators
    val actual = PKViolatorsDataObject("pkViol").getDataFrame(Seq(), typeOf[SparkSubFeed]).asInstanceOf[SparkDataFrame]

    // creating expected
    val rows_expected = Seq(
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","0let"),TestKV("value",null))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","2let"),TestKV("value","doublet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","2let"),TestKV("value","doublet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","3let"),TestKV("value","triplet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","3let"),TestKV("value","triplet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","3let"),TestKV("value","triplet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet"))),
      TestData("hive_table_pk_id_ValueDO","mock","hive_table_pk_id_Value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet")))
    )
    val expected = SparkDataFrame(rows_expected.toDF)

    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("pk violations with null values",
      Seq(src.getSparkDataFrame()))(actual.inner)(expected.inner)
    assert(resultat)
  }

  test("pk violations for multiple sources") {

    // creating and registering data objects //

    // a custom data object
    val customDO = CustomDfDataObject(id="custom_do",
      creator = CustomDfCreatorConfig(className = Some(classOf[TestCustomDfNonUniqueWithNullCreator].getName)))
    instanceRegistry.register(customDO)

    val hiveTablePKidDO = MockDataObject("hive_table_pk_idDO", tableName = "hive_table_pk_id", primaryKey = Some(Seq("id"))).register
    hiveTablePKidDO.writeSparkDataFrame(dfNonUniqueWithNull)

    val hiveTableNoPKDO = MockDataObject("hive_table_no_pkDO", tableName = "hive_table_no_pk").register
    hiveTableNoPKDO.writeSparkDataFrame(dfTwoCandidateKeys)

    val hiveTablePKidValueDO = MockDataObject("hive_table_pk_id_valueDO", tableName = "hive_table_pk_id_value", primaryKey = Some(Seq("id","value"))).register
    hiveTablePKidValueDO.writeSparkDataFrame(dfNonUniqueWithNull)

    // actual: reading the table containing the PK violators
    val actual = PKViolatorsDataObject("pkViol").getDataFrame(Seq(), typeOf[SparkSubFeed]).asInstanceOf[SparkDataFrame]

    // creating expected
    val rows_expectedWithData = Seq(
      // PKviolators of hiveTablePKidDO
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","2let")),Seq(TestKV("value","doublet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","2let")),Seq(TestKV("value","doublet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","3let")),Seq(TestKV("value","triplet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","3let")),Seq(TestKV("value","triplet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","3let")),Seq(TestKV("value","triplet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet"))),
      TestData("hive_table_pk_idDO","mock","hive_table_pk_id","id STRING,value STRING",Seq(TestKV("id","4let")),Seq(TestKV("value","quatriplet")))
    )

    // PKviolators of hiveTablePKidValueDO
    val rows_expectedWithOutData = Seq(
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","0let"),TestKV("value",null))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","2let"),TestKV("value","doublet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","2let"),TestKV("value","doublet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","3let"),TestKV("value","triplet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","3let"),TestKV("value","triplet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","3let"),TestKV("value","triplet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet"))),
      TestData("hive_table_pk_id_valueDO","mock","hive_table_pk_id_value","id STRING,value STRING",Seq(TestKV("id","4let"),TestKV("value","quatriplet")))
    )

    val expected = SparkDataFrame(
      rows_expectedWithData.toDF.union(rows_expectedWithOutData.toDF)
    )

    val resultat: Boolean = expected.isEqual(actual)
    if (!resultat) printFailedTestResult("pk violations for multiple sources",
      Seq(customDO.getSparkDataFrame(),hiveTablePKidDO.getSparkDataFrame(),hiveTableNoPKDO.getSparkDataFrame(),hiveTablePKidValueDO.getSparkDataFrame()))(actual.inner)(expected.inner)
    assert(resultat)
  }



}

case class TestKV(column_name: String, column_value: String)
case class TestData(data_object_id: String, db: String, table: String, schema: String, key: Seq[TestKV], data: Seq[TestKV] = null)

