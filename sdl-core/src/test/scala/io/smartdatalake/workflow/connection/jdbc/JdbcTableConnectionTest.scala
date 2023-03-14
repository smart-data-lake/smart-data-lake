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

import org.scalatest.FunSuite

class JdbcTableConnectionTest extends FunSuite {

  test("turn off autocommit with connectionInitSql") {
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:file:target/JdbcTableDataObjectTest/hsqldb",
      "org.hsqldb.jdbcDriver", connectionInitSql = Some("SET AUTOCOMMIT FALSE"))

    val autoCommitEnabled = jdbcConnection.execWithJdbcConnection(_.getAutoCommit)
    assert(!autoCommitEnabled)
  }

  test("turn off autocommit with autoCommit flag") {
    val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:file:target/JdbcTableDataObjectTest/hsqldb",
      "org.hsqldb.jdbcDriver", autoCommit = Some(false))

    val autoCommitEnabled = jdbcConnection.execWithJdbcConnection(_.getAutoCommit)
    assert(!autoCommitEnabled)
  }
}
