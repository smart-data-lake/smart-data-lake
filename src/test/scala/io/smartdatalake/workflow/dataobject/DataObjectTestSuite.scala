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
package io.smartdatalake.workflow.dataobject

import java.nio.file.Files

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.testutils.TestUtil
import org.apache.commons.lang3.StringUtils.replaceAll
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

trait DataObjectTestSuite extends FunSuite with Matchers with BeforeAndAfter {

  protected implicit lazy val testSession: SparkSession = TestUtil.sessionHiveCatalog

  protected val escapedFilePath: String => String = (path: String) => replaceAll(path, "\\\\", "\\\\\\\\")
  protected val convertFilePath: String => String = (path: String) => replaceAll(path, "\\\\", "/")

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  protected lazy val testContext: ActionPipelineContext = ActionPipelineContext("testFeed", "testSource", instanceRegistry)

  protected def createTempDir = Files.createTempDirectory("test")

  before {
    instanceRegistry.clear()
  }
}