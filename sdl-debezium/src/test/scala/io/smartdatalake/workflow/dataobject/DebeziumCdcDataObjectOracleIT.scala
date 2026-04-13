/*
 * sdl-debezium - Build your data lake the smart way.
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
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

import java.nio.file.Files

object DebeziumCdcDataObjectOracleIT extends App with SmartDataLakeLogger {

  /*
   * Integration test to test basic debezium source db operations (initial read, insert, update, delete, no changes).
   */


  /**
   * Init tests
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
    dbEngine = "oracle",
    hostname = sys.env("ORACLE_HOSTNAME"),
    db = Some(sys.env("ORACLE_DB")),
    port = sys.env("ORACLE_PORT").toInt,
    authMode = BasicAuthMode(Some(StringOrSecret(sys.env("ORACLE_USER"))), Some(StringOrSecret(sys.env("ORACLE_PASSWORD"))))
  )

  val jdbcConnection = JdbcTableConnection(
    id = "oracleCon",
    url = s"jdbc:oracle:thin:@${sys.env("ORACLE_HOSTNAME")}:${sys.env("ORACLE_PORT").toInt}/${sys.env("ORACLE_DB")}",
    driver = "oracle.jdbc.driver.OracleDriver",
    authMode = Some(BasicAuthMode(Some(StringOrSecret(sys.env("ORACLE_USER"))), Some(StringOrSecret(sys.env("ORACLE_PASSWORD"))))),
    db = Some("demo")
  )

  val appName = "sdlb-debezium-sequential-integration-test"
  val feedName = "debezium-test-single"
  val tempDir = Files.createTempDirectory(feedName)
  val statePath = "target/stateTestSingle/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))
  HdfsUtil.deleteFiles(new Path(statePath), doWarn = false)

  instanceRegistry.register(connection)

  jdbcConnection.execJdbcStatement("TRUNCATE TABLE C##DEMO.TEST")
  jdbcConnection.execJdbcStatement("INSERT INTO C##DEMO.TEST (VALUE, TIMESTAMPCOL, DECIMALCOL) VALUES ('INIT 1', TO_TIMESTAMP('1994-11-30 01:00:00', 'YYYY-MM-DD HH24:MI:SS'), 19.94)")

  // Setup data objects

  val srcDO1 = DebeziumCdcDataObject("src1", connectionId = "dbzCon", Table(Some("C##DEMO"), "TEST"), debeziumProperties = Some(Map("database.server.id" -> "1234345345", "topic.prefix" -> "test", "schema.history.internal" -> "io.debezium.storage.file.history.FileSchemaHistory", "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat")))
  instanceRegistry.register(srcDO1)

  val tgtDO1 = ParquetFileDataObject("tgt1", tempDir.resolve("testTgt1").toString.replace('\\', '/'))
  instanceRegistry.register(tgtDO1)

  // Setup copy actions

  val action1 = CopyAction("copyAction1", srcDO1.id, tgtDO1.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
  instanceRegistry.register(action1)

  val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

  // 1. Initial READ test

  sdlb.run(sdlConfig)

  var df = tgtDO1.getSparkDataFrame()

  assert(df.columns.contains("ID") &&
    df.columns.contains("VALUE") &&
    df.columns.contains("TIMESTAMPCOL") &&
    df.columns.contains("DECIMALCOL") &&
    df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("read")).filter(!$"test").isEmpty)

  // 2. Insert test
  jdbcConnection.execJdbcStatement("INSERT INTO C##DEMO.TEST (VALUE, TIMESTAMPCOL, DECIMALCOL) VALUES ('INSERT TEST', TO_TIMESTAMP('1994-07-30 07:07:07', 'YYYY-MM-DD HH24:MI:SS'), 30.0)")

  sdlb.run(sdlConfig)

  df = tgtDO1.getSparkDataFrame()

  assert(df.columns.contains("ID") &&
    df.columns.contains("VALUE") &&
    df.columns.contains("TIMESTAMPCOL") &&
    df.columns.contains("DECIMALCOL") &&
    df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("create")).filter(!$"test").isEmpty)

  // 3. Update test
  jdbcConnection.execJdbcStatement("UPDATE C##DEMO.TEST SET VALUE = 'UPDATE TEST' WHERE VALUE = 'INSERT TEST'")

  sdlb.run(sdlConfig)

  df = tgtDO1.getSparkDataFrame()

  assert(df.columns.contains("ID") &&
    df.columns.contains("VALUE") &&
    df.columns.contains("TIMESTAMPCOL") &&
    df.columns.contains("DECIMALCOL") &&
    df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME).isin(lit("update_preimage"), lit("update_postimage"))).collect().length == 2)

  // 4. Delete test
  jdbcConnection.execJdbcStatement("DELETE FROM C##DEMO.TEST WHERE VALUE = 'UPDATE TEST'")

  sdlb.run(sdlConfig)

  df = tgtDO1.getSparkDataFrame()

  assert(df.columns.contains("ID") &&
    df.columns.contains("VALUE") &&
    df.columns.contains("TIMESTAMPCOL") &&
    df.columns.contains("DECIMALCOL") &&
    df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(df.withColumn("test", col(COMMIT_TYPE_COLUMN_NAME) === lit("delete")).filter(!$"test").isEmpty)

  // 5. No new data test

  sdlb.run(sdlConfig)

  df = srcDO1.getSparkDataFrame() // check src because copyAction will be skipped and target will contain the data from previous test step


  assert(df.columns.contains("ID") &&
    df.columns.contains("VALUE") &&
    df.columns.contains("TIMESTAMPCOL") &&
    df.columns.contains("DECIMALCOL") &&
    df.columns.contains(COMMIT_TYPE_COLUMN_NAME) &&
    df.columns.contains(COMMIT_TIMESTAMP_COLUMN_NAME)
  )

  assert(df.isEmpty)

}
