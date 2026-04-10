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
import io.smartdatalake.testutils.spark.dataset.Collection.dsComplex
import io.smartdatalake.util.spark.dataset.{StructTypeUtil, getEmptyDataFrame}
import io.smartdatalake.util.spark.hive.HiveUtil
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class SDLSparkExtensionTest extends AnyFunSuite with StructTypeUtil {

  private implicit val session: SparkSession = TestUtil.session
  private val emptyDf = getEmptyDataFrame(scheme = createStruct(fieldName = "value"))

  test("fail on assertNonEmpty with empty DataFrame") {
    // fail after applying assertNonEmpty
    val dfWithAssert: DataFrame = SDLSparkExtension.assertNotEmpty(df = emptyDf)
    intercept[AssertNotEmptyFailure](dfWithAssert.count)
  }

  test("succeed on assertNonEmpty with non-empty DataFrame") {
    val df = dsComplex.repartition(10)
    val dfJoined = df.join(df, Seq("id"))

    // succeed when applying assertNonEmpty
    val dfWithAssert = SDLSparkExtension.assertNotEmpty(dfJoined)
    dfWithAssert.count
  }

  test("fail on check no-data rule with empty DataFrame") {
    // fail when writing to table.
    intercept[SparkPlanNoDataWarning](writeTable(emptyDf, "runtime_stats_no_data_check"))
  }

  test("fail on check no-data rule with joined empty DataFrame") {
    import session.implicits._
    val df = dsComplex.repartition(10)
    val dfEmpty = Seq[(Int, String)]().toDF("id", "value2")
    val dfJoined = df.join(dfEmpty, Seq("id"))

    // fail when writing to table.
    intercept[SparkPlanNoDataWarning](writeTable(dfJoined, "runtime_stats_joined_no_data_check"))
  }

  test("succeed on check no-data rule with non-empty DataFrame") {
    val df = dsComplex.repartition(10)

    // Succeed when writing to table.
    writeTable(df, "runtime_stats_data_check")
  }

  test("succeed on check no-data rule with joined non-empty DataFrame") {
    val df = dsComplex.repartition(10)
    val dfJoined = df.join(df.withColumnRenamed("value", "value2"), Seq("id"))

    // Succeed when writing to table.
    writeTable(dfJoined, "runtime_stats_joined_data_check")
  }

  def writeTable[T](ds: Dataset[T], name: String): Unit = {
    val path = new Path(new File(s"target/$name").getAbsolutePath)
    val table = Table(Some("default"), name)
    HiveUtil.dropTable(table, path)
    ds.write.option("path", path.toString).saveAsTable(table.fullName)
  }
}
