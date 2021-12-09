/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2020 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.testutils.custom.{TestCustomDfCreator, TestCustomDfCreatorWithSchema}
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import org.scalatest.Matchers

class CustomDfDataObjectTest extends DataObjectTestSuite with Matchers {

  private val customDfCreatorClassName = classOf[TestCustomDfCreator].getName
  private val customDfCreatorWithSchemaClassName = classOf[TestCustomDfCreatorWithSchema].getName

  private val context = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  test("During init where a schema method is provided, the schema of the schema method should be returned") {
    // prepare
    val config = CustomDfCreatorConfig(Option(customDfCreatorWithSchemaClassName))
    val customDfDataObject = CustomDfDataObject("testId", config)
    val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // run
    val df = customDfDataObject.getDataFrame(Seq())(context)

    // check
    assert(df.schema.equals(customDfDataObject.creator.schema.get))
  }

  test("During exec, the schema of the exec method should be returned") {
    // prepare
    val config = CustomDfCreatorConfig(Option(customDfCreatorWithSchemaClassName))
    val customDfDataObject = CustomDfDataObject("testId", config)

    // run
    val df = customDfDataObject.getDataFrame(Seq())(context)

    // check
    assert(df.schema.equals(customDfDataObject.creator.exec.schema))
  }

  test("During init where a schema method is provided, a DataFrame with no rows should be returned") {
    // prepare
    val config = CustomDfCreatorConfig(Option(customDfCreatorWithSchemaClassName))
    val customDfDataObject = CustomDfDataObject("testId", config)
    val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // run
    val df = customDfDataObject.getDataFrame(Seq())(context)

    // check
    assert(df.count() == 0)
  }

  test("During exec where a schema method is provided, a DataFrame with the rows from exec should be returned") {
    // prepare
    val config = CustomDfCreatorConfig(Option(customDfCreatorWithSchemaClassName))
    val customDfDataObject = CustomDfDataObject("testId", config)

    // run
    val df = customDfDataObject.getDataFrame(Seq())(context)

    // check
    assert(df.count() == 2)
  }

  test("During init where no schema method is provided, a DataFrame with the rows from exec should be returned") {
    // prepare
    val config = CustomDfCreatorConfig(Option(customDfCreatorClassName))
    val customDfDataObject = CustomDfDataObject("testId", config)
    val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // run
    val df = customDfDataObject.getDataFrame(Seq())(context)

    // check
    assert(df.count() == 2)
  }

  test("During exec where no schema method is provided, a DataFrame with the rows from exec should be returned") {
    // prepare
    val config = CustomDfCreatorConfig(Option(customDfCreatorClassName))
    val customDfDataObject = CustomDfDataObject("testId", config)

    // run
    val df = customDfDataObject.getDataFrame(Seq())(context)

    // check
    assert(df.count() == 2)
  }
}
