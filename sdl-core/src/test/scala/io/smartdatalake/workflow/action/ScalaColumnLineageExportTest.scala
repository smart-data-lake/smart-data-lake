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
import io.smartdatalake.testutils.ColumnLineageExportBehaviour
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Test the column level lineage of a plain-Scala Action, collected in the init phase of a
 * "--test dry-run-with-lineage-export" run, and its export, see issue #867.
 *
 * All cases are engine independent, see [[ColumnLineageExportBehaviour]]. Running them without Spark also
 * shows that the collection and the export itself do not depend on the Spark engine.
 */
class ScalaColumnLineageExportTest extends AnyFunSuite with ColumnLineageExportBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]

  override def defaultEngineConnection: Connection with EngineConnection = ScalaTestUtil.defaultScalaConnection

  override def createDataObject(name: String)(implicit instanceRegistry: InstanceRegistry): DataObject with CanCreateDataFrame with CanWriteDataFrame =
    MockScalaDataObject(name).register

  override def defaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext =
    ScalaTestUtil.getDefaultActionPipelineContext

  override def inputSubFeed(dataObjectId: DataObjectId): DataFrameSubFeed = ScalaSubFeed(None, dataObjectId, Seq())

  test("the column lineage of an Action is collected in the init phase") {
    testTheColumnLineageOfAnActionIsCollectedInTheInitPhase()
  }

  test("the column lineage of an Action with two inputs keeps both inputs apart") {
    testTheColumnLineageOfAnActionWithTwoInputsKeepsBothInputsApart()
  }

  test("the collected column lineage is exported as OpenLineage column lineage facet") {
    testTheCollectedColumnLineageIsExportedAsOpenLineageColumnLineageFacet()
  }

  test("the lineage of chained Actions stops at the intermediate DataObject") {
    testTheLineageOfChainedActionsStopsAtTheIntermediateDataObject()
  }

  test("no column lineage is collected without the lineage export test mode") {
    testNoColumnLineageIsCollectedWithoutTheLineageExportTestMode()
  }
}
