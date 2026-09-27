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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.SchemaExportBehaviour
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Test the schemas collected in the init phase of a "--test dry-run-with-schema-export" run with the plain-Scala
 * engine, see [[SchemaExportBehaviour]].
 */
class ScalaSchemaExportTest extends AnyFunSuite with SchemaExportBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]

  override def defaultEngineConnection: Connection with EngineConnection = ScalaTestUtil.defaultScalaConnection

  override def createDataObject(name: String)(implicit instanceRegistry: InstanceRegistry): DataObject with CanCreateDataFrame with CanWriteDataFrame =
    MockScalaDataObject(name).register

  override def defaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext =
    ScalaTestUtil.getDefaultActionPipelineContext

  override def inputSubFeed(dataObjectId: DataObjectId): DataFrameSubFeed = ScalaSubFeed(None, dataObjectId, Seq())

  test("the schema of an input only DataObject is collected") {
    testTheSchemaOfAnInputOnlyDataObjectIsCollected()
  }

  test("the schema of a DataObject written by a DataFrame-Action is not collected as input") {
    testTheSchemaOfADataObjectWrittenByADataFrameActionIsNotCollectedAsInput()
  }

  test("no input schema is collected without the schema export test mode") {
    testNoInputSchemaIsCollectedWithoutTheSchemaExportTestMode()
  }
}
