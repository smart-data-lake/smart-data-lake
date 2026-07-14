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
import io.smartdatalake.testutils.SQLDfsTransformerBehaviour
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

// SQLDfsTransformer uses DataFrameFunctions.sql, which is not implemented for ScalaSubFeed
class SQLDfsTransformerTest extends AnyFunSuite with SQLDfsTransformerBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  ignore("options and view name token are replaced and sql can be parsed") {
    testOptionsAndViewNameTokenAreReplacedAndSqlCanBeParsed(id => MockScalaDataObject(id))
  }

  ignore("legacy view name without postfix is still supported and sql can be parsed") {
    testLegacyViewNameWithoutPostfixIsStillSupportedAndSqlCanBeParsed(id => MockScalaDataObject(id))
  }

}
