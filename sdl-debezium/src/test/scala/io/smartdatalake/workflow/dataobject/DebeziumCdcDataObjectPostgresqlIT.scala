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
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.executionMode.DataObjectStateIncrementalMode
import io.smartdatalake.workflow.action.{ActionMetadata, CopyAction, HistorizeAction}
import io.smartdatalake.workflow.connection.DebeziumConnection
import io.smartdatalake.workflow.connection.authMode.BasicAuthMode
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Files

object DebeziumCdcDataObjectPostgresqlIT extends App with SmartDataLakeLogger {

  /*
   * Integration test to test basic debezium source db operations (initial read, insert, update, delete, no changes).
   */

  /**
   * Init tests
   */

  implicit val sparkSession: SparkSession = SparkTestUtil.session
  import sparkSession.implicits._

  val CHANGE_TYPE_COLUMN_NAME = "_change_type"
  val COMMIT_TIMESTAMP_COLUMN_NAME = "_commit_timestamp"
  val CHANGE_ORDINAL_COLUMN_NAME = "_change_ordinal"

  val jdbcConnection = JdbcTableConnection(
    id = "psqlCon",
    url = s"jdbc:postgresql://${sys.env("PSQL_HOSTNAME")}:${sys.env("PSQL_PORT").toInt}/${sys.env("PSQL_DB")}",
    driver = "org.postgresql.Driver",
    authMode = Some(BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))),
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
      dbEngine = "postgresql",
      hostname = sys.env("PSQL_HOSTNAME"),
      db = Some(sys.env("PSQL_DB")),
      port = sys.env("PSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      id = "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "plugin.name"                           -> "pgoutput",
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

    // Do the initial inserts then run
    jdbcConnection.execJdbcStatement("TRUNCATE demo.test")
    jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('INIT 1', '1994-11-30 01:00:00', 19.94)")

    sdlb.run(sdlConfig)

    val df = tgtDO1.getSparkDataFrame()

    assert(df.columns.contains("id") &&
      df.columns.contains("value") &&
      df.columns.contains("timestampcol") &&
      df.columns.contains("decimalcol") &&
      df.columns.contains(CHANGE_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(CHANGE_TYPE_COLUMN_NAME) === lit("read")).filter(!$"test").isEmpty)

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
      dbEngine = "postgresql",
      hostname = sys.env("PSQL_HOSTNAME"),
      db = Some(sys.env("PSQL_DB")),
      port = sys.env("PSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "plugin.name"                           -> "pgoutput",
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
      df.columns.contains("timestampcol") &&
      df.columns.contains("decimalcol") &&
      df.columns.contains(CHANGE_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(CHANGE_TYPE_COLUMN_NAME) === lit("insert")).filter(!$"test").isEmpty)

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
      dbEngine = "postgresql",
      hostname = sys.env("PSQL_HOSTNAME"),
      db = Some(sys.env("PSQL_DB")),
      port = sys.env("PSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "plugin.name"                           -> "pgoutput",
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
      df.columns.contains("timestampcol") &&
      df.columns.contains("decimalcol") &&
      df.columns.contains(CHANGE_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(CHANGE_TYPE_COLUMN_NAME).isin(lit("update_preimage"), lit("update_postimage"))).collect().length == 2)

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
      dbEngine = "postgresql",
      hostname = sys.env("PSQL_HOSTNAME"),
      db = Some(sys.env("PSQL_DB")),
      port = sys.env("PSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "plugin.name"                           -> "pgoutput",
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
      df.columns.contains("timestampcol") &&
      df.columns.contains("decimalcol") &&
      df.columns.contains(CHANGE_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.withColumn("test", col(CHANGE_TYPE_COLUMN_NAME) === lit("delete")).filter(!$"test").isEmpty)

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
      dbEngine = "postgresql",
      hostname = sys.env("PSQL_HOSTNAME"),
      db = Some(sys.env("PSQL_DB")),
      port = sys.env("PSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))
    )

    instanceRegistry.register(connection)

    // Setup data objects

    val srcDO1 = DebeziumCdcDataObject(
      "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "plugin.name"                           -> "pgoutput",
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
      df.columns.contains("timestampcol") &&
      df.columns.contains("decimalcol") &&
      df.columns.contains(CHANGE_TYPE_COLUMN_NAME) &&
      df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
    )

    assert(df.isEmpty)

  }

  noNewDataTest()

  /**
   * End-to-end test of change data capture with historization: the change events of demo.test are historized into
   * demo.test_hist without any CDC specific configuration on the HistorizeAction.
   */
  def historizeTest(): Unit = {

    println("Historize test started")

    val historizeFeedName = "debezium-test-historize"
    val historizeStatePath = "target/stateTestHistorize/"
    HdfsUtil.deleteFiles(new Path(historizeStatePath), doWarn = false)(HdfsUtil.getHadoopFsWithDefaultConf(new Path(historizeStatePath)))

    val sdlb = DefaultSmartDataLakeBuilder
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    Environment._instanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry)

    // Setup connections
    val connection = DebeziumConnection(
      id = "dbzCon",
      dbEngine = "postgresql",
      hostname = sys.env("PSQL_HOSTNAME"),
      db = Some(sys.env("PSQL_DB")),
      port = sys.env("PSQL_PORT").toInt,
      authMode = BasicAuthMode(user = StringOrSecret(sys.env("PSQL_USER")), password = StringOrSecret(sys.env("PSQL_PASSWORD")))
    )
    instanceRegistry.register(connection)
    instanceRegistry.register(jdbcConnection)

    // Setup data objects. Note that the source table is read with a new state, so debezium starts with a snapshot.
    val srcDO1 = DebeziumCdcDataObject(
      id = "src1",
      connectionId = "dbzCon",
      Table(Some("demo"), "test"),
      debeziumProperties = Some(Map(
          "plugin.name"                           -> "pgoutput",
          "schema.history.internal"               -> "io.debezium.storage.file.history.FileSchemaHistory",
          "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat"
        ))
    )
    instanceRegistry.register(srcDO1)

    val tgtDO1 = JdbcTableDataObject("tgt1", table = Table(Some("demo"), "test_hist", primaryKey = Some(Seq("id"))),
      connectionId = jdbcConnection.id, allowSchemaEvolution = true)
    instanceRegistry.register(tgtDO1)

    // Setup historize action. No CDC specific configuration is needed, as the input has SDLBs standard CDC columns.
    implicit val historizeLogger: org.slf4j.Logger = logger
    val action1 = HistorizeAction("historizeAction1", srcDO1.id, tgtDO1.id, executionMode = Some(DataObjectStateIncrementalMode()),
      metadata = Some(ActionMetadata(feed = Some(historizeFeedName))))
    instanceRegistry.register(action1)

    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = historizeFeedName,
      applicationName = Some(appName), statePath = Some(historizeStatePath))

    // Start with two records in the source table and an empty history
    jdbcConnection.execJdbcStatement("DROP TABLE IF EXISTS demo.test_hist")
    jdbcConnection.execJdbcStatement("TRUNCATE demo.test")
    jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('HIST 1', '1994-11-30 01:00:00', 19.94)")
    jdbcConnection.execJdbcStatement("INSERT INTO demo.test (value, timestampCol, decimalCol) VALUES ('HIST 2', '1994-11-30 02:00:00', 19.95)")

    sdlb.run(sdlConfig)

    {
      val df = tgtDO1.getSparkDataFrame()
      // the CDC metadata columns are not written to the history
      assert(!df.columns.contains(CHANGE_TYPE_COLUMN_NAME))
      assert(!df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME))
      assert(!df.columns.contains(CHANGE_ORDINAL_COLUMN_NAME))
      // both records are current versions
      assert(df.count() == 2)
      assert(df.filter(col(Environment.delimitedColumnName) === lit(Environment.historizationUpperHorizonTimestamp)).count() == 2)
    }

    // update one record and delete the other one
    jdbcConnection.execJdbcStatement("UPDATE demo.test SET value = 'HIST 1 UPDATED' WHERE value = 'HIST 1'")
    jdbcConnection.execJdbcStatement("DELETE FROM demo.test WHERE value = 'HIST 2'")

    sdlb.run(sdlConfig)

    {
      val df = tgtDO1.getSparkDataFrame()
      val currentDf = df.filter(col(Environment.delimitedColumnName) === lit(Environment.historizationUpperHorizonTimestamp))
      // the updated record has a new current version, the deleted record has none anymore
      assert(currentDf.count() == 1)
      assert(currentDf.select(col("value")).as[String].collect().toSeq == Seq("HIST 1 UPDATED"))
      // the previous version of the updated record and the deleted record are closed
      assert(df.count() == 3)
      // the update is historized only once, e.g. the preimage of the update did not create a version
      assert(df.filter(col("value") === lit("HIST 1")).count() == 1)
    }

  }

  historizeTest()

}
