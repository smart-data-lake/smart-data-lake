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
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

// ConvertNullValuesTransformer uses coalesce, which is not implemented for ScalaSubFeed
class ConvertNullValuesTransformerTest extends AnyFunSuite with ConvertNullValuesTransformerBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  ignore("exclusive include- or excludeColumns") {
    testExclusiveIncludeOrExcludeColumns()
  }

  ignore("default values") {
    testDefaultValues()
  }

  ignore("includeColumns set") {
    testIncludeColumnsSet()
  }

  ignore("excludeColumns set") {
    testExcludeColumnsSet()
  }

  ignore("custom string value check") {
    testCustomStringValueCheck()
  }

  ignore("custom number value check") {
    testCustomNumberValueCheck()
  }

  ignore("ignore other than string / number types columns") {
    testIgnoreOtherThanStringOrNumberTypesColumns()
  }

  ignore("no error for existing include columns (case insensitive)") {
    testNoErrorForExistingIncludeColumnsCaseInsensitive()
  }

  ignore("error for non existing include columns (case insensitive)") {
    testErrorForNonExistingIncludeColumnsCaseInsensitive()
  }

  ignore("no error for existing include columns (case sensitive)") {
    testNoErrorForExistingIncludeColumnsCaseSensitive()
  }

  ignore("error for non existing include columns (case sensitive)") {
    testErrorForNonExistingIncludeColumnsCaseSensitive()
  }

  ignore("no error for existing exclude columns (case insensitive)") {
    testNoErrorForExistingExcludeColumnsCaseInsensitive()
  }

  ignore("error for non existing exclude columns (case insensitive)") {
    testErrorForNonExistingExcludeColumnsCaseInsensitive()
  }

  ignore("no error for existing exclude columns (case sensitive)") {
    testNoErrorForExistingExcludeColumnsCaseSensitive()
  }

  ignore("error for non existing exclude columns (case sensitive)") {
    testErrorForNonExistingExcludeColumnsCaseSensitive()
  }
}
