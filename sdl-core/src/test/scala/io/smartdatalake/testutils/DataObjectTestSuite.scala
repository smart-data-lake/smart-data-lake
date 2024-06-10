/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.testutils

import java.nio.file.Files
import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}


trait DataObjectTestSuite extends FunSuite with Matchers with BeforeAndAfter {

  protected implicit lazy val session: SparkSession = TestUtil.session

  protected val escapedFilePath: String => String = (path: String) => path.replaceAll("\\\\", "\\\\\\\\")
  protected val convertFilePath: String => String = (path: String) => path.replaceAll("\\\\", "/")

  // initialize empty Global Config in Environment
  if (Environment._globalConfig == null) Environment._globalConfig = GlobalConfig()

  // initialize empty instance registry
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  // prepare contexts to reuse
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  protected def createTempDir = Files.createTempDirectory("test")

  before {
    instanceRegistry.clear()
  }
}