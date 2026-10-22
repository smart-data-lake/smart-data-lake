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
package io.smartdatalake.workflow.action.generic.customlogic

import io.smartdatalake.util.misc.NotFoundError
import io.smartdatalake.workflow.dataframe.plainScala.{ScalaDataFrame, ScalaSubFeed}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests the dynamic transform method of the engine independent transformer interfaces, using the plain Scala
 * DataFrame implementation.
 */
class CustomGenericTransformerTest extends AnyFunSuite {

  private val functions: DataFrameFunctions = ScalaSubFeed
  private val df1 = ScalaDataFrame.fromData(Seq(Seq("x", 1)), Seq("a", "b"))
  private val df2 = ScalaDataFrame.fromData(Seq(Seq("y", 2)), Seq("a", "b"))

  test("CustomGenericDfsTransformer can dynamically map DataFrames and options") {
    val transformer = new DynamicGenericDfsTransformer
    val result = transformer.transform(functions, Map("factor" -> "3"), Map("src1" -> df1, "src2" -> df2))
    assert(result.keys.toSeq == Seq("tgt"))
    assert(result("tgt") == df2)
  }

  test("CustomGenericDfsTransformer matches DataFrame names tolerantly") {
    val transformer = new DynamicGenericDfsTransformer
    val result = transformer.transform(functions, Map("factor" -> "3"), Map("SRC_1" -> df1, "src-2" -> df2))
    assert(result.keys.toSeq == Seq("tgt"))
  }

  test("CustomGenericDfsTransformer reports missing DataFrames") {
    val transformer = new DynamicGenericDfsTransformer
    intercept[NotFoundError] {
      transformer.transform(functions, Map("factor" -> "3"), Map("other" -> df1))
    }
  }

  test("CustomGenericDfsTransformer can return a single DataFrame using option outputDataObjectId") {
    val transformer = new DynamicGenericDfsSingleReturnTransformer
    val result = transformer.transform(functions, Map("outputDataObjectId" -> "tgt"), Map("src1" -> df1))
    assert(result == Map("tgt" -> df1))
  }

  test("CustomGenericDfsTransformer with standard transform method is still supported") {
    val transformer = new StdGenericDfsTransformer
    val result = transformer.transform(functions, Map.empty[String,String], Map("src1" -> df1))
    assert(result == Map("tgt" -> df1))
  }

  test("CustomGenericDfTransformer maps the single input DataFrame independent of the parameter name") {
    val transformer = new DynamicGenericDfTransformer
    val result = transformer.transform(functions, Map("factor" -> "3"), df1, "src1")
    assert(result == df1)
  }

  test("CustomGenericDfTransformer gets dataObjectId as option") {
    val transformer = new DynamicGenericDfTransformerWithDataObjectId
    val result = transformer.transform(functions, Map.empty[String,String], df1, "src1")
    assert(result == df1)
  }

  test("CustomGenericDfTransformer with standard transform method is still supported") {
    val transformer = new StdGenericDfTransformer
    val result = transformer.transform(functions, Map.empty[String,String], df1, "src1")
    assert(result == df1)
  }

  test("Option parameters use default values if the option is not defined") {
    val transformer = new DynamicGenericDfTransformerWithDefaults
    val result = transformer.transform(functions, Map.empty[String,String], df1, "src1")
    assert(result == df1)
  }
}

class DynamicGenericDfsTransformer extends CustomGenericDfsTransformer {
  def transform(dfSrc1: GenericDataFrame, dfSrc2: GenericDataFrame, factor: Int): Map[String, GenericDataFrame] = {
    assert(factor == 3)
    assert(dfSrc1 != dfSrc2)
    Map("tgt" -> dfSrc2)
  }
}

class DynamicGenericDfsSingleReturnTransformer extends CustomGenericDfsTransformer {
  def transform(dfSrc1: GenericDataFrame): GenericDataFrame = dfSrc1
}

class StdGenericDfsTransformer extends CustomGenericDfsTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], dfs: Map[String, GenericDataFrame]): Map[String, GenericDataFrame] = {
    Map("tgt" -> dfs("src1"))
  }
}

class DynamicGenericDfTransformer extends CustomGenericDfTransformer {
  def transform(helper: DataFrameFunctions, dfSomethingElse: GenericDataFrame, factor: Int): GenericDataFrame = {
    assert(helper != null)
    assert(factor == 3)
    dfSomethingElse
  }
}

class DynamicGenericDfTransformerWithDataObjectId extends CustomGenericDfTransformer {
  def transform(df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    assert(dataObjectId == "src1")
    df
  }
}

class StdGenericDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = df
}

class DynamicGenericDfTransformerWithDefaults extends CustomGenericDfTransformer {
  def transform(df: GenericDataFrame, factor: Int = 2, optionalFlag: Option[Boolean], names: Seq[String] = Seq()): GenericDataFrame = {
    assert(factor == 2)
    assert(optionalFlag.isEmpty)
    assert(names.isEmpty)
    df
  }
}
