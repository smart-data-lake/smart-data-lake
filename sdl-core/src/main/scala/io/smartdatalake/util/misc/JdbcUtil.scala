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

package io.smartdatalake.util.misc

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.workflow.connection.Connection
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.spark.sql.jdbc.JdbcDialect

import java.sql.{ResultSet, Statement, Connection => SqlConnection}
import java.time.Duration

object JdbcUtil {

  /**
   * Create a JDBC Connection Pool
   *
   * @param maxParallelConnections max number of parallel jdbc connections created by an instance of this connection, default is 3
   * @param connectionPoolMaxIdleTimeSec timeout to close unused connections in the pool
   * @param connectionPoolMaxWaitTimeSec timeout when waiting for connection in pool to become available. Default is 600 seconds (10 minutes).
   * @param factoryFun factory method for JDBC Connections
   * @param initSql sql statement to be executed on new connections
   * @param autoCommit if auto-commit should be enabled
   * @param connectionPoolTestOnBorrow if testOnBorrow should be enabled
   * @param connectionPoolTestOnCreate if testOnCreate should be enabled
   * @param connectionPoolTestOnReturn if testOnReturn should be enabled
   * @param connectionPoolTestWhileIdle if testWhileIdle should be enabled
   * @return the created {@code GenericObjectPool} object
   */
  def createConnectionPool(maxParallelConnections: Int, connectionPoolMaxIdleTimeSec: Int, connectionPoolMaxWaitTimeSec: Int, factoryFun: () => SqlConnection, initSql: Option[String], autoCommit: Boolean, connectionPoolTestOnBorrow: Boolean = false, connectionPoolTestOnCreate: Boolean = false, connectionPoolTestOnReturn: Boolean = false, connectionPoolTestWhileIdle: Boolean = false): GenericObjectPool[SqlConnection] = {
    // setup connection pool
    val pool = new GenericObjectPool[SqlConnection](new JdbcClientPoolFactory(factoryFun, initSql, autoCommit))
    pool.setMaxTotal(maxParallelConnections)
    pool.setMinEvictableIdle(Duration.ofSeconds(connectionPoolMaxIdleTimeSec)) // timeout to close jdbc connection if not in use
    pool.setMaxWait(Duration.ofSeconds(connectionPoolMaxWaitTimeSec))
    pool.setTestOnBorrow(connectionPoolTestOnBorrow)
    pool.setTestOnCreate(connectionPoolTestOnCreate)
    pool.setTestOnReturn(connectionPoolTestOnReturn)
    pool.setTestWhileIdle(connectionPoolTestWhileIdle)
    pool
  }

  def createTransaction(pool: GenericObjectPool[SqlConnection], autoCommit: Boolean, logging: Boolean, id: ConfigObjectId): JdbcTransaction = {
    new JdbcTransaction(pool, autoCommit, logging, id)
  }


  def execWithJdbcStatement[A](conn: SqlConnection, doCommit: Boolean, autoCommit: Boolean)(func: Statement => A): A = {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement
      val result = func(stmt)
      if (doCommit && !autoCommit) conn.commit()
      result
    } catch {
      case e: Exception =>
        conn.rollback()
        throw e
    } finally {
      if (stmt != null) stmt.close()
    }
  }
}

/**
 * Class for handling database transactions. If all operations succeeded call [[commit]], otherwise [[rollback]].
 */
private[smartdatalake] class JdbcTransaction(pool: GenericObjectPool[SqlConnection], autoCommit: Boolean, logging: Boolean, id: ConfigObjectId) extends SmartDataLakeLogger {
  logger.info(s"($id) begin transaction $transactionId")
  private val jdbcConnection: SqlConnection = pool.borrowObject()

  def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
    JdbcUtil.execWithJdbcStatement(jdbcConnection, doCommit = false, autoCommit = autoCommit) { stmt =>
      if (logging) logger.info(s"($id) execJdbcStatement in transaction $transactionId: $sql")
      if (autoCommit) logger.warn("autoCommit is enabled, so statement will be committed immediately")
      stmt.execute(sql)
    }
  }

  def commit(): Unit = {
    if (!autoCommit) {
      logger.info(s"($id) commit transaction $transactionId")
      jdbcConnection.commit()
    };
    close()
  }

  def rollback(): Unit = {
    try {
      if (!autoCommit) {
        logger.info(s"($id) roll back transaction $transactionId")
        jdbcConnection.rollback()
      };
    } finally {
      close()
    }
  }

  private def close(): Unit = {
    pool.returnObject(jdbcConnection)
  }

  private def transactionId: Int = {
    hashCode()
  }
}


private[smartdatalake] class JdbcClientPoolFactory(factoryFun: () => SqlConnection, initSql: Option[String], autoCommit: Boolean) extends BasePooledObjectFactory[SqlConnection] {
  override def create(): SqlConnection = {
    val connection = factoryFun()
    initConnection(connection)
  }

  private def initConnection(connection: SqlConnection): SqlConnection = {
    initSql.foreach(initSql => {
      var stmt: Statement = null
      try {
        stmt = connection.createStatement()
        stmt.execute(initSql)
      } finally {
        if (stmt != null) stmt.close()
      }
    })
    connection.setAutoCommit(autoCommit)
    connection
  }

  override def wrap(con: SqlConnection): PooledObject[SqlConnection] = new DefaultPooledObject(con)
  override def destroyObject(p: PooledObject[SqlConnection]): Unit = p.getObject.close()
}

trait JdbcExecution { this: Connection with SmartDataLakeLogger =>

  def pool: GenericObjectPool[SqlConnection]

  def autoCommit: Boolean

  def jdbcDialect: JdbcDialect

  /**
   * Get a connection from the pool and execute an arbitrary function
   */
  def execWithJdbcConnection[A]( func: SqlConnection => A ): A = {
    WithResourcePool.exec(pool){
      con => func(con)
    }
  }

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary sql statement
   * @return true if the first result is a ResultSet object; false if it is an update count or there are no results (see also Jdbc.Statement.execute())
   */
  def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
    execWithJdbcConnection(JdbcUtil.execWithJdbcStatement(_, doCommit = true, autoCommit = autoCommit) { stmt =>
      if (logging) logger.info(s"($id) execJdbcStatement: $sql")
      stmt.execute(sql)
    })
  }

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary function
   * @return row count for SQL Data Manipulation Language (DML) statements (see also Jdbc.Statement.execute())
   */
  def execJdbcDmlStatement(sql: String, logging: Boolean = true): Int = {
    execWithJdbcConnection(JdbcUtil.execWithJdbcStatement(_, doCommit = true, autoCommit = autoCommit) { stmt =>
      if (logging) logger.info(s"($id) execJdbcDmlStatement: $sql")
      stmt.executeUpdate(sql)
    })
  }

  /**
   * Execute an SQL query and evaluate its ResultSet
   * @param sql sql query to execute
   * @param evalResultSet function to evaluate the JDBC ResultSet
   * @return the evaluated result
   */
  def execJdbcQuery[A](sql:String, evalResultSet: ResultSet => A ) : A = {
    execWithJdbcConnection(JdbcUtil.execWithJdbcStatement(_, doCommit = true, autoCommit = autoCommit) { stmt =>
      var rs: ResultSet = null
      try {
        logger.info(s"($id) execJdbcQuery: $sql")
        rs = stmt.executeQuery(sql)
        evalResultSet(rs)
      } finally {
        if (rs != null) rs.close()
      }
    })
  }

  /**
   * Begin database transaction. Note that depending on the isolation level of the database, changes from concurrent
   * connections might not be available inside the transaction once it is started. So make sure that any required writes
   * from Spark are finished before beginning a transaction.
   */
  def beginTransaction(): JdbcTransaction = JdbcUtil.createTransaction(pool, autoCommit, logging = true, id)
}
