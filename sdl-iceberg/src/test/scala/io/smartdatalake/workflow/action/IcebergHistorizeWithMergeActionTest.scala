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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions
import io.smartdatalake.testutils.spark.dataset.TestToolDataset
import io.smartdatalake.testutils.{HistorizeActionBehaviour, MockSparkDataObject, TestUtil}
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, IcebergTableDataObject, IcebergTestUtils, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime

class IcebergHistorizeWithMergeActionTest extends AnyFunSuite with Matchers with SmartDataLakeLogger
  with TestToolDataset with Equality with HistorizeActionBehaviour {

  // set additional spark options for delta lake
  protected implicit val session: SparkSession = IcebergTestUtils.session

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  session.sql("CREATE DATABASE IF NOT EXISTS iceberg1.default")

  testsFor(historizeWithMergeMode(
    (id, registry) => MockSparkDataObject(id),
    (id, pks, registry) => {
      val tgtTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = id.replaceAll("-", "_"), primaryKey = pks)
      IcebergTableDataObject(id, Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, allowSchemaEvolution = true)(registry)
    }
  ))

  testsFor(historizeIncrementalPipeline(
    (id, registry) => MockSparkDataObject(id),
    (id, pks, registry) => {
      val tgtTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = id.replaceAll("-", "_"), primaryKey = pks)
      IcebergTableDataObject(id, Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, allowSchemaEvolution = true)(registry)
    },
    None
  ))

}
