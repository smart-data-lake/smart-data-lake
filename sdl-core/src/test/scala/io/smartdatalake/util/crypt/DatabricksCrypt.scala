/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.crypt

import io.smartdatalake.testutils.TestUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
 * Unit tests for historization
 *
 */
class DatabricksCrypt extends FunSuite {
  implicit lazy val session: SparkSession = TestUtil.session

  import session.implicits._

  test("encrypting UDF") {
    val dfSrc = Seq(("testData", "Foo", "ice"), ("bar", "Space", "water"), ("gogo", "Space", "water")).toDF("c1", "c2", "c3")
    dfSrc.show(false)
    dfSrc.createOrReplaceTempView("testTable")
    session.sql("SELECT * FROM testTable").show()

    // where does that needs to be defined?
    val encDec = new EncryptColumn()
    val bla = session.udf.register("encrypt_udf", encDec.evaluate _)

    // once the above is registered, the user can use the UDF e.g. in PowerBI as
    val key = "A%D*G-KaPdSgVkYp"
    session.sql(s"SELECT *, encrypt_udf(c2, '${key}', 'ECB') FROM testTable").show(false)
  }
}