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
import io.smartdatalake.workflow.connection.authMode.BasicAuthMode
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.connection.DebeziumConnection
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Files

object DebeziumCdcDataObjectMySqlParallelIT extends App with SmartDataLakeLogger {

  /*
   * Integration test to validate parallel read from debezium source db and separate tables
   */

  val sdlb = DefaultSmartDataLakeBuilder
  implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
  implicit val sparkSession: SparkSession = TestUtil.session
  Environment._instanceRegistry = instanceRegistry
  implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

  import sparkSession.implicits._

  val COMMIT_TYPE_COLUMN_NAME = "__commit_event"
  val COMMIT_TIMESTAMP_COLUMN_NAME = "__event_timestamp"

  val connection = DebeziumConnection(
    id = "dbzCon",
    dbEngine = "mysql",
    hostname = sys.env("MYSQL_HOSTNAME"),
    port = sys.env("MYSQL_PORT").toInt,
    authMode = BasicAuthMode(Some(StringOrSecret(sys.env("MYSQL_USER"))), Some(StringOrSecret(sys.env("MYSQL_PASSWORD"))))
  )

  val jdbcConnection = JdbcTableConnection(
    id = "mysqlCon",
    url = s"jdbc:mysql://${sys.env("MYSQL_HOSTNAME")}:${sys.env("MYSQL_PORT").toInt}",
    driver = "com.mysql.cj.jdbc.Driver",
    authMode = Some(BasicAuthMode(Some(StringOrSecret(sys.env("MYSQL_USER"))), Some(StringOrSecret(sys.env("MYSQL_PASSWORD"))))),
    db = Some("demo")
  )

  val appName = "sdlb-debezium-parallel-integration-test"
  val feedName = "debezium-test"
  val tempDir = Files.createTempDirectory(feedName)
  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))
  HdfsUtil.deleteFiles(new Path(statePath), doWarn = false)

  instanceRegistry.register(connection)

  jdbcConnection.execJdbcStatement("TRUNCATE demo.test")
  jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('INIT 1', '1994-11-30 01:00:00', 19.94)")
  jdbcConnection.execJdbcStatement("TRUNCATE demo.test2")
  jdbcConnection.execJdbcStatement("INSERT INTO demo.test2 (value, timestampCol, decimalCol) VALUES ('INIT 1', '1994-11-30 01:00:00', 19.94)")

  // Setup data objects

  val srcDO1 = DebeziumCdcDataObject("src1", connectionId = "dbzCon", Table(Some("demo"), "test"), debeziumProperties = Some(Map("database.server.id" -> "12343453456", "database.allowPublicKeyRetrieval" -> "true", "schema.history.internal" -> "io.debezium.storage.file.history.FileSchemaHistory", "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat")))
  instanceRegistry.register(srcDO1)
  val srcDO2 = DebeziumCdcDataObject("src2", connectionId = "dbzCon", Table(Some("demo"), "test2"), debeziumProperties = Some(Map("database.server.id" -> "12343453459", "database.allowPublicKeyRetrieval" -> "true", "schema.history.internal" -> "io.debezium.storage.file.history.FileSchemaHistory", "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat")))
  instanceRegistry.register(srcDO2)

  val tgtDO1 = ParquetFileDataObject( "tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
  instanceRegistry.register(tgtDO1)
  val tgtDO2 = ParquetFileDataObject( "tgt2", tempDir.resolve("testTgt2").toString.replace('\\', '/'))
  instanceRegistry.register(tgtDO2)

  // Setup copy actions

  val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
  instanceRegistry.register(action1)
  val action2 = CopyAction("copyAction2", srcDO2.id, tgtDO2.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
  instanceRegistry.register(action2)

  val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath), parallelism = 2)

  // Initial run

  sdlb.run(sdlConfig)

  val tgtDF1 = tgtDO1.getSparkDataFrame()
  val tgtDF2 = tgtDO2.getSparkDataFrame()

  assert(tgtDF1.columns.contains("id") &&
    tgtDF1.columns.contains("value") &&
    tgtDF1.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    tgtDF1.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(tgtDF1.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("read")).filter(!$"test").isEmpty)

  assert(tgtDF2.columns.contains("id") &&
    tgtDF2.columns.contains("value") &&
    tgtDF2.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    tgtDF2.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(tgtDF2.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("read")).filter(!$"test").isEmpty)

}
