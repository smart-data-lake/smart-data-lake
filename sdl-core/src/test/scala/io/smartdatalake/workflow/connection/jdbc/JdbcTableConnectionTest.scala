/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.connection.jdbc

import io.smartdatalake.util.misc.{ConnectionPoolConfig, JdbcClientPoolFactory, SmartDataLakeLogger}
import org.apache.log4j.builders.layout.PatternLayoutBuilder
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.LoggerConfig
import org.hsqldb.Server
import org.scalatest.FunSuite

import java.net.ConnectException
import java.sql.{SQLException, SQLTransientConnectionException}
import java.util.Properties
import scala.collection.mutable

class JdbcTableConnectionTest extends FunSuite with SmartDataLakeLogger {

  test("autocommit is disabled by default") {
    // prepare
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcTableConnectionTest",
      "org.hsqldb.jdbcDriver")

    // run
    val autoCommitEnabled = jdbcConnection.execWithJdbcConnection(_.getAutoCommit)

    // check
    assert(!autoCommitEnabled)
  }

  test("JdbcTransaction.commit returns connection back to pool") {
    // prepare
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcTableConnectionTest",
      "org.hsqldb.jdbcDriver", maxParallelConnections = 1, connectionPool = ConnectionPoolConfig(maxWaitTimeSec = 10))

    // run
    val transaction1 = jdbcConnection.beginTransaction()
    transaction1.commit()
    val transaction2 = jdbcConnection.beginTransaction()
    transaction2.commit();

    // check
    // no exception
  }

  test("JdbcTransaction.rollback returns connection back to pool") {
    // prepare
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcTableConnectionTest",
      "org.hsqldb.jdbcDriver", maxParallelConnections = 1, connectionPool = ConnectionPoolConfig(maxWaitTimeSec = 10))

    // run
    val transaction1 = jdbcConnection.beginTransaction()
    transaction1.rollback()
    val transaction2 = jdbcConnection.beginTransaction()
    transaction2.commit();

    // check
    // no exception
  }

  test("maxParallelConnections > 1 allows concurrent transactions") {
    // prepare
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcTableConnectionTest",
      "org.hsqldb.jdbcDriver", maxParallelConnections = 2, connectionPool = ConnectionPoolConfig(maxWaitTimeSec = 10))

    // run
    val transaction1 = jdbcConnection.beginTransaction()
    val transaction2 = jdbcConnection.beginTransaction()
    transaction1.commit();
    transaction2.commit();

    // check
    // no exception
  }

  test("rollback after failed statement") {
    // prepare
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcTableConnectionTest",
      "org.hsqldb.jdbcDriver")

    jdbcConnection.execJdbcStatement("drop table if exists test_rollback")
    jdbcConnection.execJdbcStatement("create table test_rollback( id int )")

    // run
    val transaction = jdbcConnection.beginTransaction()
    try {
      transaction.execJdbcStatement("insert into test_rollback(id) values(1)")
      transaction.execJdbcStatement("insert into test_rollback(id) values('bla')") // throws exception
    } catch {
      case _: SQLException => transaction.rollback()
    }

    // check
    jdbcConnection.execJdbcQuery("select count(*) from test_rollback", rs => {
      rs.next()
      assert(rs.getInt(1) == 0)
    })
  }

  test("connection pool test connection on borrow/return") {

    // starting hsqldb server
    val server = new Server()
    server.setPort(1234)
    server.setDatabasePath(0, "target/hsqldbtest")
    server.setDatabaseName(0, "hsqldbtest")
    server.setSilent(true)
    server.start()

    // prepare
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:hsql://localhost:1234/hsqldbtest",
      "org.hsqldb.jdbcDriver", connectionPool = ConnectionPoolConfig( maxIdleTimeSec = 10, testOnBorrow = true, testOnReturn = true))

    // run something -> success
    logger.info("check connection valid")
    jdbcConnection.execWithJdbcConnection(_.getClientInfo)

    // shutdown server
    server.stop()

    // setup log appender for unit test
    val loggerName = classOf[JdbcClientPoolFactory].getName
    val context = LoggerContext.getContext(false)
    val initialConfig = context.getConfiguration
    var loggerConfig = context.getConfiguration.getLoggerConfig(loggerName)
    if (loggerConfig.getName != loggerName) loggerConfig = new LoggerConfig(loggerName, Level.WARN, true)
    val appender = new ListAppender("hsqldbtest")
    appender.start()
    loggerConfig.addAppender(appender, Level.WARN, null)
    context.getConfiguration.addLogger(loggerName, loggerConfig)
    context.updateLoggers()

    // run something -> fail
    logger.info("server stopped, check connection invalid")
    intercept[SQLTransientConnectionException](jdbcConnection.execWithJdbcConnection(_.getClientInfo))

    // check log of JdbcClientPoolFactory.validateObject exists
    assert(appender.events.nonEmpty)

    // restore
    context.setConfiguration(initialConfig)
  }
}

class ListAppender(name: String) extends AbstractAppender("test", null, null, false, Array()) {
  val events = mutable.Buffer[LogEvent]()
  override def append(event: LogEvent): Unit = {
    events.append(event)
  }
}