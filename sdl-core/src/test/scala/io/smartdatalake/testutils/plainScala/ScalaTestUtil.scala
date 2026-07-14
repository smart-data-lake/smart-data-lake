/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.testutils.plainScala

import com.typesafe.config.ConfigFactory
import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.{ConfigParser, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.connection.{Connection, ScalaConnection}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.file.FileRefDataObject
import io.smartdatalake.workflow.dataobject.generic.TableDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}

import java.time.LocalDateTime

/**
 * Engine-agnostic subset of [[TestUtil]], usable without a Spark dependency.
 * Kept as a separate object (rather than merged into `TestUtil`) because sdl-spark's `TestUtil`
 * lives in a different module under the same fully-qualified name; modules depending on both
 * test-jars (e.g. sdl-deltalake, sdl-iceberg) must not see two classes named `TestUtil`.
 */
object ScalaTestUtil extends SmartDataLakeLogger {

  val defaultScalaConnection: ScalaConnection = {
    implicit val dummyRegistry: InstanceRegistry = new InstanceRegistry
    // parse from config, so that connection._config value is filled for agent config serialization tests...
    ConfigParser.parseConfigObject[Connection](
      ConfigFactory.parseString(s"type = ScalaConnection, id = ${Environment.defaultEngineConnectionId}")
    ).asInstanceOf[ScalaConnection]
  }

  def getDefaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext = {
    ActionPipelineContext(
      feed = "feedTest",
      application = "appTest",
      executionId = SDLExecutionId.executionId1,
      instanceRegistry = instanceRegistry,
      referenceTimestamp = Some(LocalDateTime.now()),
      appConfig = SmartDataLakeBuilderConfig("feedTest", Some("appTest")),
      phase = ExecutionPhase.Init,
      globalConfig = GlobalConfig()
    )
  }

  def registerDataObject[A <: DataObject](dataObject: A)(implicit instanceRegistry: InstanceRegistry, context: ActionPipelineContext): A = {
    dataObject match {
      case tableDataObject: TableDataObject  => tableDataObject.dropTable
      case fileDataObject: FileRefDataObject => fileDataObject.deleteAll
      case _                                 => ()
    }
    instanceRegistry.register(dataObject)
    dataObject
  }
}
