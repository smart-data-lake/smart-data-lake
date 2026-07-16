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
package io.smartdatalake.util.misc

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.workflow.connection.Connection
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.spark.sql.jdbc.JdbcDialect

import java.sql.{ResultSet, Statement, Connection => SqlConnection}
import scala.util.Success

trait JdbcExecution { this: Connection with SmartDataLakeLogger =>

  def pool: GenericObjectPool[SqlConnection]

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
    execWithJdbcConnection(JdbcUtil.execWithJdbcStatement(_, doCommit = true) { stmt =>
      if (logging) logger.info(s"($id) execJdbcStatement: $sql")
      stmt.execute(sql)
    })
  }

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary function
   * @return row count for SQL Data Manipulation Language (DML) statements (see also Jdbc.Statement.execute())
   */
  def execJdbcDmlStatement(sql: String, logging: Boolean = true): Int = {
    execWithJdbcConnection(JdbcUtil.execWithJdbcStatement(_, doCommit = true) { stmt =>
      if (logging) logger.info(s"($id) execJdbcDmlStatement: $sql")
      val returnCode = scala.util.Try(stmt.executeUpdate(sql)) match {
        case Success(value) =>
          if (logging) logger.info(s"($id) execJdbcDmlStatement succeeded: returnCode = $value")
          value
        case scala.util.Failure(e) =>
          logger.error(s"execJdbcDmlStatement failed: $sql , error message = ${e.getMessage}")
          throw e
      }
      returnCode
    })
  }

  /**
   * Execute an SQL query and evaluate its ResultSet
   * @param sql sql query to execute
   * @param evalResultSet function to evaluate the JDBC ResultSet
   * @return the evaluated result
   */
  def execJdbcQuery[A](sql:String, evalResultSet: ResultSet => A ) : A = {
    execWithJdbcConnection(JdbcUtil.execWithJdbcStatement(_, doCommit = true) { stmt =>
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
  def beginTransaction(): JdbcTransaction = JdbcUtil.createTransaction(pool, logging = true, id)
}
