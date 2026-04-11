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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.{ActionMetadata, CopyAction}
import io.smartdatalake.workflow.connection.DebeziumConnection
import io.smartdatalake.workflow.connection.authMode.BasicAuthMode
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.nio.file.Files

object DebeziumCdcDataObjectPostgresqlColdStartTableIT extends App with SmartDataLakeLogger{

  /*
   * Integration test to test read of an empty table that never got any updates.
   * Make sure you setup the db according to the TEST_DB_SETUP.md
   */


  /**
   * Init tests
   */

  val sdlb = DefaultSmartDataLakeBuilder
  implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
  implicit val sparkSession: SparkSession = TestUtil.session
  Environment._instanceRegistry = instanceRegistry
  Environment._enableSparkFileDataObjectNoDataCheck = Some(false)
  implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

  val connection = DebeziumConnection(
    id = "dbzCon",
    dbEngine = "postgresql",
    hostname = sys.env("PSQL_HOSTNAME"),
    db = Some(sys.env("PSQL_DB")),
    port = sys.env("PSQL_PORT").toInt,
    authMode = BasicAuthMode(Some(StringOrSecret(sys.env("PSQL_USER"))), Some(StringOrSecret(sys.env("PSQL_PASSWORD"))))
  )

  val appName = "sdlb-debezium-empty-table-test"
  val feedName = "debezium-test-empty"
  val tempDir = Files.createTempDirectory(feedName)
  val statePath = "target/stateTestEmpty/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))
  HdfsUtil.deleteFiles(new Path(statePath), doWarn = false)

  instanceRegistry.register(connection)

  // Setup data objects

  val srcDO1 = DebeziumCdcDataObject("src1",
    connectionId = "dbzCon",
    Table(Some("demo"), "empty_table"),
    schemaMin=Some(SparkSchema(StructType(Seq(StructField("id", IntegerType), StructField("value", StringType))))),
    debeziumProperties = Some(Map("database.server.id" -> "12343453455",
      "plugin.name" -> "pgoutput",
      "schema.history.internal" -> "io.debezium.storage.file.history.FileSchemaHistory",
      "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
    ))
  )
  instanceRegistry.register(srcDO1)

  val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
  instanceRegistry.register(tgtDO1)

  // Setup copy actions

  val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
  instanceRegistry.register(action1)

  val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

  // 1. READ test

  sdlb.run(sdlConfig)

  val df = tgtDO1.getSparkDataFrame()


  assert(df.columns.contains("id") &&
    df.columns.contains("value")
  )

  assert(df.isEmpty)

}
