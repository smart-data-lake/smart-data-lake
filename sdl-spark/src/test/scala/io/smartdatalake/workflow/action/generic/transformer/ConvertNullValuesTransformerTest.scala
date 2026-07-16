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
package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.ConvertNullValuesTransformerBehaviour
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class ConvertNullValuesTransformerTest extends AnyFunSuite with ConvertNullValuesTransformerBehaviour {

  protected implicit val session: SparkSession = SparkTestUtil.session

  override def subFeedType: Type = typeOf[SparkSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

  private def withCaseSensitiveSession(block: => Unit): Unit = {
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    try {
      block
    } finally {
      session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
    }
  }

  test("exclusive include- or excludeColumns") {
    testExclusiveIncludeOrExcludeColumns()
  }

  test("default values") {
    testDefaultValues()
  }

  test("includeColumns set") {
    testIncludeColumnsSet()
  }

  test("excludeColumns set") {
    testExcludeColumnsSet()
  }

  test("custom string value check") {
    testCustomStringValueCheck()
  }

  test("custom number value check") {
    testCustomNumberValueCheck()
  }

  test("ignore other than string / number types columns") {
    testIgnoreOtherThanStringOrNumberTypesColumns()
  }

  test("no error for existing include columns (case insensitive)") {
    testNoErrorForExistingIncludeColumnsCaseInsensitive()
  }

  test("error for non existing include columns (case insensitive)") {
    testErrorForNonExistingIncludeColumnsCaseInsensitive()
  }

  test("no error for existing include columns (case sensitive)") {
    withCaseSensitiveSession(testNoErrorForExistingIncludeColumnsCaseSensitive())
  }

  test("error for non existing include columns (case sensitive)") {
    withCaseSensitiveSession(testErrorForNonExistingIncludeColumnsCaseSensitive())
  }

  test("no error for existing exclude columns (case insensitive)") {
    testNoErrorForExistingExcludeColumnsCaseInsensitive()
  }

  test("error for non existing exclude columns (case insensitive)") {
    testErrorForNonExistingExcludeColumnsCaseInsensitive()
  }

  test("no error for existing exclude columns (case sensitive)") {
    withCaseSensitiveSession(testNoErrorForExistingExcludeColumnsCaseSensitive())
  }

  test("error for non existing exclude columns (case sensitive)") {
    withCaseSensitiveSession(testErrorForNonExistingExcludeColumnsCaseSensitive())
  }
}
