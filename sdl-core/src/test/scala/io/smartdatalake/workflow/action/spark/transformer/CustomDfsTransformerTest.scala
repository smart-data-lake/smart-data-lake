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

package io.smartdatalake.workflow.action.spark.transformer

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

class CustomDfsTransformerTest extends FunSuite {
  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  test("CustomDfsTransformer can dynamically map parameters") {
    val transformer = new DynamicTransformer(true, Some(true), false, 1L, Some(1L))
    val options = Map("isExec" -> "true", "optionalBoolean" -> "true", "defaultBoolean" -> "false", "long" -> "1", "optionalLong" -> "1")
    val df1 = Seq(("x",1)).toDF("a","b")
    val dfs = Map("test" -> df1)
    val resultDfs = transformer.transform(session, options, dfs)
    assert(resultDfs.keys.toSeq == Seq("test"))
  }

  test("Extract method parameter default values") {
    val transformer = new DynamicTransformer(true, Some(true), false, 1L, Some(1L))
    val transformMethodsOfSubclass = CustomCodeUtil.getClassMethodsByName(transformer.getClass, "transform")
      .filter(_.owner != typeOf[CustomDfsTransformer].typeSymbol)
    val transformMethod = transformMethodsOfSubclass.head
    val params = CustomCodeUtil.analyzeMethodParameters(transformer, transformMethod)
    assert(params.find(_.name == "defaultBoolean").get.defaultValue.nonEmpty)
  }

  test("CustomDfsTransformer can dynamically map parameters with default values or optional") {
    val transformer = new DynamicTransformer(true, None, true, 1L, None)
    val options = Map("isExec" -> "true", "long" -> "1")
    val df1 = Seq(("x",1)).toDF("a","b")
    val dfs = Map("test" -> df1)
    val resultDfs = transformer.transform(session, options, dfs)
    assert(resultDfs.keys.toSeq == Seq("test"))
  }
}

class DynamicTransformer(isExecExpected: Boolean, optionalBooleanExpected: Option[Boolean], defaultBooleanExpected: Boolean = true, longExpected: Long, optionalLongExpected: Option[Long]) extends CustomDfsTransformer {
  def transform(session: SparkSession, dfTest: DataFrame, dsTest: Dataset[Test], isExec: Boolean, optionalBoolean: Option[Boolean], defaultBoolean: Boolean = true, long: Long, optionalLong: Option[Long]) = {
    assert(session != null)
    assert(dfTest.columns.toSeq == Seq("a","b"))
    assert(dsTest.toDF.columns.toSeq == Seq("a","b"))
    assert(isExec == isExecExpected)
    assert(optionalBoolean == optionalBooleanExpected)
    assert(defaultBoolean == defaultBooleanExpected)
    assert(long == longExpected)
    assert(optionalLong == optionalLongExpected)
    Map("test" -> dfTest)
  }
}

case class Test(a: String, b: Int)