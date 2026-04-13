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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.scalatest.funsuite.AnyFunSuite

class OpenApiDataObjectIT extends AnyFunSuite {
  protected implicit lazy val session: SparkSession = TestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  implicit val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  test("get data.sbb.ch datasets with struct return type") {
    import session.implicits._
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://data.sbb.ch/api/explore/v2.1",
      apiDocsUrl = "swagger.json",
      operationId = "getDatasets",
    )
    do1.prepare
    val df = do1.getSparkDataFrame()
    df.withColumn("result", explode($"results")).drop("results")
      .limit(5).show(false)
  }

  test("get data.sbb.ch datasets with paging") {
    import session.implicits._
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://data.sbb.ch/api/explore/v2.1",
      apiDocsUrl = "swagger.json",
      operationId = "getDatasets",
      urlParameters = Map("include_links" -> "true"),
      pagingLinkJsonPath = Some("$._links[?(@.rel == 'next')].href")
    )
    do1.prepare
    val df = do1.getSparkDataFrame()
    df.withColumn("result", explode($"results"))
      .select($"result.*")
      .limit(5).show(false)
  }

  test("get catfact.ninja breeds with array return type") {
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://catfact.ninja",
      apiDocsUrl = "docs?api-docs.json",
      operationId = "getBreeds",
      schemaMatchJsonPath = Some("$.data"),
      urlParameters = Map("limit" -> "5")
    )
    do1.prepare
    val df = do1.getSparkDataFrame()
    df.limit(5).show(false)
  }

  test("get catfact.ninja breeds with paging and array return type") {
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://catfact.ninja",
      apiDocsUrl = "docs?api-docs.json",
      operationId = "getBreeds",
      schemaMatchJsonPath = Some("$.data"),
      pagingLinkJsonPath = Some("$.next_page_url")
    )
    do1.prepare
    val df = do1.getSparkDataFrame()
    df.limit(5).show(false)
  }
}
