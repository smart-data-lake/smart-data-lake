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
package io.smartdatalake.testutils

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

trait DataObjectTestSuite extends AnyFunSuite with Matchers with BeforeAndAfter {

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

  protected def createTempDir: Path = Files.createTempDirectory("test")

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
    additionalBefore()
  }

  def additionalBefore(): Unit = ()
}