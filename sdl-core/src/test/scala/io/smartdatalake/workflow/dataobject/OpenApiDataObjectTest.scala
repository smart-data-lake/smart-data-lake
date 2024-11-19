/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FunSuite

class OpenApiDataObjectTest extends FunSuite {
  protected implicit lazy val session: SparkSession = TestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  implicit val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  test("read openapi spec from file") {
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://test.com",
      apiDocsUrl = "./sdl-core/src/test/resources/openApiSpec/sampleApiDoc.json",
      operationId = "getPing",
    )
    do1.prepare
    val df = do1.getSparkDataFrame()(contextInit)

    val schemaExpected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(df.schema, schemaExpected))
  }

  test("read openapi spec from classpath") {
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://test.com",
      apiDocsUrl = "cp:/openApiSpec/sampleApiDoc.json",
      operationId = "getPing",
    )
    do1.prepare
    val df = do1.getSparkDataFrame()(contextInit)

    val schemaExpected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(df.schema, schemaExpected))
  }

}
