/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.spark

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.Table
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class SDLSparkExtensionTest extends FunSuite {

  private implicit val session: SparkSession = TestUtil.session

  test("fail on assertNonEmpty with empty DataFrame") {
    val df = TestUtil.dfEmptyWithSchema

    // fail after applying assertNonEmpty
    val dfWithAssert = SDLSparkExtension.assertNotEmpty(df)
    intercept[AssertNotEmptyFailure](dfWithAssert.count)
  }

  test("succeed on assertNonEmpty with non-empty DataFrame") {
    val df = TestUtil.dfComplex.repartition(10)
    val dfJoined = df.join(df, Seq("id"))

    // succeed when applying assertNonEmpty
    val dfWithAssert = SDLSparkExtension.assertNotEmpty(dfJoined)
    dfWithAssert.count
  }

  test("fail on check no-data rule with empty DataFrame") {
    val df = TestUtil.dfEmptyWithSchema

    // fail when writing to table.
    intercept[SparkPlanNoDataWarning](writeTable(df,"runtime_stats_no_data_check"))
  }

  test("fail on check no-data rule with joined empty DataFrame") {
    import session.implicits._
    val df = TestUtil.dfComplex.repartition(10)
    val dfEmpty = Seq[(Int,String)]().toDF("id","value2")
    val dfJoined = df.join(dfEmpty, Seq("id"))

    // fail when writing to table.
    intercept[SparkPlanNoDataWarning](writeTable(dfJoined,"runtime_stats_joined_no_data_check"))
  }

  test("succeed on check no-data rule with non-empty DataFrame") {
    val df = TestUtil.dfComplex.repartition(10)

    // Succeed when writing to table.
    writeTable(df,"runtime_stats_data_check")
  }

  test("succeed on check no-data rule with joined non-empty DataFrame") {
    val df = TestUtil.dfComplex.repartition(10)
    val dfJoined = df.join(df.withColumnRenamed("value", "value2"), Seq("id"))

    // Succeed when writing to table.
    writeTable(dfJoined,"runtime_stats_joined_data_check")
  }

  def writeTable(df: DataFrame, name: String) = {
    val path = new Path(s"./target/$name")
    val table = Table(Some("default"),name)
    HiveUtil.dropTable(table, path)
    df.write.option("path", path.toString).saveAsTable(table.fullName)
  }
}
