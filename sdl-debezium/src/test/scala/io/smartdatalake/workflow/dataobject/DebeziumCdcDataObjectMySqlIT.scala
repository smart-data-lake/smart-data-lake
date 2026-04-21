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

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.executionMode.DataObjectStateIncrementalMode
import io.smartdatalake.workflow.action.{ActionMetadata, CopyAction}
import io.smartdatalake.workflow.connection.DebeziumConnection
import io.smartdatalake.workflow.connection.authMode.BasicAuthMode
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Files

object DebeziumCdcDataObjectMySqlIT extends App with SmartDataLakeLogger {

  /*
   * Integration test to test basic debezium source db operations (initial read, insert, update, delete, no changes).
   */

  /**
   * Init tests
   */

  implicit val sparkSession: SparkSession = TestUtil.session
  import sparkSession.implicits._

  val COMMIT_TYPE_COLUMN_NAME = "__commit_event"
  val COMMIT_TIMESTAMP_COLUMN_NAME = "__event_timestamp"

  val jdbcConnection = JdbcTableConnection(
    id = "mysqlCon",
    url = s"jdbc:mysql://${sys.env("MYSQL_HOSTNAME")}:${sys.env("MYSQL_PORT").toInt}",
    driver = "com.mysql.cj.jdbc.Driver",
    authMode = Some(BasicAuthMode(user = StringOrSecret(sys.env("MYSQL_USER")), password = StringOrSecret(sys.env("MYSQL_PASSWORD")))),
    db = Some("demo")
  )

  val appName = "sdlb-debezium-sequential-integration-test"
  val feedName = "debezium-test-single"
  val tempDir = Files.createTempDirectory(feedName)
  val statePath = "target/stateTestSingle/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))
  HdfsUtil.deleteFiles(new Path(statePath), doWarn = false)

  def initialReadTest(): Unit = {

    println("Initial read test started")

    val sdlb = DefaultSmartDataLakeBuilder
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    Environment._instanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

    // Setup connection
    val connection = DebeziumConnection(
      id = "dbzCon",
      dbEngine = "mysql",
      hostname = sys.env("MYSQL_HOSTNAME"),
      port = sys.env("MYSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("MYSQL_USER")), password = StringOrSecret(sys.env("MYSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "database.server.id"                    -> "1234345345",
          "database.allowPublicKeyRetrieval"      -> "true",
          "schema.history.internal"               -> "io.debezium.storage.file.history.FileSchemaHistory",
          "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
        ))
    )
    instanceRegistry.register(srcDO1)

    jdbcConnection.execJdbcStatement("CREATE TABLE IF NOT EXISTS demo.test (value varchar(100), timestampCol timestamp, decimalCol decimal(38,10))")
    jdbcConnection.execJdbcStatement("TRUNCATE demo.test")
    jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('INIT 1', '1994-11-30 01:00:00', 19.94)")

    val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
    instanceRegistry.register(tgtDO1)

    // Setup copy actions

    val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, executionMode = Some(DataObjectStateIncrementalMode()),
      metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)

    val sdlConfig =
      SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // Do the initial inserts then run
    jdbcConnection.execJdbcStatement("TRUNCATE demo.test")
    jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('INIT 1', '1994-11-30 01:00:00', 19.94)")

    sdlb.run(sdlConfig)

    val df = tgtDO1.getSparkDataFrame()

    assert(df.columns.contains("id") &&
      df.columns.contains("value") &&
      df.columns.contains("timestampCol") &&
      df.columns.contains("decimalCol") &&
      df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("read")).filter(!$"test").isEmpty)

  }

  initialReadTest()

  def insertTest(): Unit = {

    println("Insert test started")

    val sdlb = DefaultSmartDataLakeBuilder
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    Environment._instanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

    // Setup connection
    val connection = DebeziumConnection(
      id = "dbzCon",
      dbEngine = "mysql",
      hostname = sys.env("MYSQL_HOSTNAME"),
      port = sys.env("MYSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("MYSQL_USER")), password = StringOrSecret(sys.env("MYSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "database.server.id"                    -> "1234345345",
          "database.allowPublicKeyRetrieval"      -> "true",
          "schema.history.internal"               -> "io.debezium.storage.file.history.FileSchemaHistory",
          "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
        ))
    )
    instanceRegistry.register(srcDO1)

    val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
    instanceRegistry.register(tgtDO1)

    // Setup copy actions

    val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, executionMode = Some(DataObjectStateIncrementalMode()),
      metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)

    val sdlConfig =
      SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // Do the insert then run sdlb
    jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('INSERT TEST', '1994-07-30 07:07:07', 30.0)")

    sdlb.run(sdlConfig)

    val df = tgtDO1.getSparkDataFrame()

    assert(df.columns.contains("id") &&
      df.columns.contains("value") &&
      df.columns.contains("timestampCol") &&
      df.columns.contains("decimalCol") &&
      df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("create")).filter(!$"test").isEmpty)

  }

  insertTest()

  def updateTest(): Unit = {

    println("Update test started")

    val sdlb = DefaultSmartDataLakeBuilder
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    Environment._instanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

    // Setup connection
    val connection = DebeziumConnection(
      id = "dbzCon",
      dbEngine = "mysql",
      hostname = sys.env("MYSQL_HOSTNAME"),
      port = sys.env("MYSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("MYSQL_USER")), password = StringOrSecret(sys.env("MYSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "database.server.id"                    -> "1234345345",
          "database.allowPublicKeyRetrieval"      -> "true",
          "schema.history.internal"               -> "io.debezium.storage.file.history.FileSchemaHistory",
          "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
        ))
    )
    instanceRegistry.register(srcDO1)

    val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
    instanceRegistry.register(tgtDO1)

    // Setup copy actions

    val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, executionMode = Some(DataObjectStateIncrementalMode()),
      metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)

    val sdlConfig =
      SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // Do the update then run sdlb
    jdbcConnection.execJdbcStatement("UPDATE demo.test SET value = 'UPDATE TEST' WHERE value = 'INSERT TEST'")

    sdlb.run(sdlConfig)

    val df = tgtDO1.getSparkDataFrame()

    assert(df.columns.contains("id") &&
      df.columns.contains("value") &&
      df.columns.contains("timestampCol") &&
      df.columns.contains("decimalCol") &&
      df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME).isin(lit("update_preimage"), lit("update_postimage"))).collect().length == 2)

  }

  updateTest()

  def deleteTest(): Unit = {

    println("Delete test started")

    val sdlb = DefaultSmartDataLakeBuilder
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    Environment._instanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

    // Setup connection
    val connection = DebeziumConnection(
      id = "dbzCon",
      dbEngine = "mysql",
      hostname = sys.env("MYSQL_HOSTNAME"),
      port = sys.env("MYSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("MYSQL_USER")), password = StringOrSecret(sys.env("MYSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "database.server.id"                    -> "1234345345",
          "database.allowPublicKeyRetrieval"      -> "true",
          "schema.history.internal"               -> "io.debezium.storage.file.history.FileSchemaHistory",
          "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
        ))
    )
    instanceRegistry.register(srcDO1)

    val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
    instanceRegistry.register(tgtDO1)

    // Setup copy actions

    val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, executionMode = Some(DataObjectStateIncrementalMode()),
      metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)

    val sdlConfig =
      SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // Do the delete then run sdlb
    jdbcConnection.execJdbcStatement("DELETE FROM demo.test WHERE value = 'UPDATE TEST'")

    sdlb.run(sdlConfig)

    val df = tgtDO1.getSparkDataFrame()

    assert(df.columns.contains("id") &&
      df.columns.contains("value") &&
      df.columns.contains("timestampCol") &&
      df.columns.contains("decimalCol") &&
      df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("delete")).filter(!$"test").isEmpty)

  }

  deleteTest()

  def noNewDataTest(): Unit = {

    println("No new data test started")

    val sdlb = DefaultSmartDataLakeBuilder
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    Environment._instanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

    // Setup connection
    val connection = DebeziumConnection(
      id = "dbzCon",
      dbEngine = "mysql",
      hostname = sys.env("MYSQL_HOSTNAME"),
      port = sys.env("MYSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("MYSQL_USER")), password = StringOrSecret(sys.env("MYSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "database.server.id"                    -> "1234345345",
          "database.allowPublicKeyRetrieval"      -> "true",
          "schema.history.internal"               -> "io.debezium.storage.file.history.FileSchemaHistory",
          "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
        ))
    )
    instanceRegistry.register(srcDO1)

    val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
    instanceRegistry.register(tgtDO1)

    // Setup copy actions

    val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, executionMode = Some(DataObjectStateIncrementalMode()),
      metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)

    val sdlConfig =
      SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // Just run sdlb again

    sdlb.run(sdlConfig)

    val df = srcDO1.getSparkDataFrame() // check src because copyAction will be skipped and target will contain the data from previous test step

    assert(df.columns.contains("id") &&
      df.columns.contains("value") &&
      df.columns.contains("timestampCol") &&
      df.columns.contains("decimalCol") &&
      df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.isEmpty)

  }

  noNewDataTest()

}
