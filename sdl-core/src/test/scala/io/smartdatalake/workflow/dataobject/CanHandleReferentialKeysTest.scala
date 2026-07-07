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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection

/**
 * Integration tests for referential key (PK / FK) provisioning using an in-memory HSQLDB.
 *
 * These tests verify the full stack: orchestration logic in [[CanHandleReferentialKeys]],
 * JDBC delegation in [[JdbcCatalogReferentialKeys]], and DDL execution in [[JdbcTableDataObject]].
 * Unity Catalog (Delta / Iceberg) and Snowflake DDL are covered by integration tests in their
 * respective modules.
 *
 * HSQLDB normalises unquoted identifiers to uppercase, so Table / ForeignKey names in constructors
 * must be uppercase for the JDBC metadata API (getPrimaryKeys / getImportedKeys) to match stored names.
 * The constraintType / isColumnNullable helpers already apply toUpperCase internally.
 */
class CanHandleReferentialKeysTest extends DataObjectTestSuite {

  import session.implicits._

  private val jdbcUrl = "jdbc:hsqldb:mem:CanHandleReferentialKeysTest"
  private val jdbcConnection = JdbcTableConnection("rkCon1", jdbcUrl, "org.hsqldb.jdbcDriver")

  // ── Helpers ──────────────────────────────────────────────────────────────

  private def constraintType(schema: String, table: String, constraintType: String): Option[String] = {
    val df = session.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.hsqldb.jdbcDriver")
      .option("query",
        s"""SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
           |WHERE TABLE_SCHEMA = '${schema.toUpperCase}' AND TABLE_NAME = '${table.toUpperCase}'
           |AND CONSTRAINT_TYPE = '$constraintType'""".stripMargin)
      .load()
    if (df.count() > 0) Some(df.first().getString(0)) else None
  }

  private def isColumnNullable(schema: String, table: String, column: String): Boolean = {
    val df = session.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.hsqldb.jdbcDriver")
      .option("query",
        s"""SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
           |WHERE TABLE_SCHEMA = '${schema.toUpperCase}' AND TABLE_NAME = '${table.toUpperCase}'
           |AND COLUMN_NAME = '${column.toUpperCase}'""".stripMargin)
      .load()
    df.first().getString(0) == "YES"
  }

  private def df(rows: (Int, String)*) = rows.toDF("id", "name")

  // ── Primary key ───────────────────────────────────────────────────────────

  test("PK constraint is created on write") {
    instanceRegistry.register(jdbcConnection)
    // Table names must be uppercase so HSQLDB's metadata API can match stored identifiers.
    val table = Table(Some("PUBLIC"), "RK_PK_CREATE", primaryKey = Some(Seq("id")),
      createAndReplaceReferentialKeys = true)
    val doObj = JdbcTableDataObject("rkDO_pk_create", table = table, connectionId = "rkCon1")
    doObj.dropTable
    val data = df((1, "alice"), (2, "bob"))
    doObj.prepare
    doObj.initSparkDataFrame(data, Seq())
    doObj.writeSparkDataFrame(data, Seq())
    doObj.postWrite(Seq())(contextExec)

    assert(constraintType("PUBLIC", "RK_PK_CREATE", "PRIMARY KEY").isDefined,
      "PRIMARY KEY constraint should exist after write")
  }

  test("PK columns are set NOT NULL before constraint is applied") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("PUBLIC"), "RK_PK_NOTNULL", primaryKey = Some(Seq("id")),
      createAndReplaceReferentialKeys = true)
    val doObj = JdbcTableDataObject("rkDO_pk_notnull", table = table, connectionId = "rkCon1")
    doObj.dropTable
    val data = df((1, "alice"), (2, "bob"))
    doObj.prepare
    doObj.initSparkDataFrame(data, Seq())
    doObj.writeSparkDataFrame(data, Seq())
    doObj.postWrite(Seq())(contextExec)

    assert(!isColumnNullable("PUBLIC", "RK_PK_NOTNULL", "id"),
      "PK column id should be NOT NULL after constraint provisioning")
  }

  test("PK provisioning is idempotent across multiple writes") {
    instanceRegistry.register(jdbcConnection)
    val table = Table(Some("PUBLIC"), "RK_PK_IDEMPOTENT", primaryKey = Some(Seq("id")),
      createAndReplaceReferentialKeys = true)
    val doObj = JdbcTableDataObject("rkDO_pk_idempotent", table = table, connectionId = "rkCon1",
      saveMode = SDLSaveMode.Append)
    doObj.dropTable
    val data = df((1, "alice"))
    doObj.prepare
    doObj.initSparkDataFrame(data, Seq())
    doObj.writeSparkDataFrame(data, Seq())
    doObj.postWrite(Seq())(contextExec)
    // second write — must not fail or duplicate the constraint
    doObj.writeSparkDataFrame(df((2, "bob")), Seq())
    doObj.postWrite(Seq())(contextExec)

    assert(constraintType("PUBLIC", "RK_PK_IDEMPOTENT", "PRIMARY KEY").isDefined)
  }

  test("PK constraint is dropped and recreated when columns change") {
    instanceRegistry.register(jdbcConnection)
    val tbl1 = Table(Some("PUBLIC"), "RK_PK_UPDATE", primaryKey = Some(Seq("id")),
      createAndReplaceReferentialKeys = true, primaryKeyConstraintName = Some("PK_RK_PK_UPDATE"))
    val doObj1 = JdbcTableDataObject("rkDO_pk_update", table = tbl1, connectionId = "rkCon1")
    doObj1.dropTable
    val data = df((1, "alice"), (2, "bob"))
    doObj1.prepare
    doObj1.initSparkDataFrame(data, Seq())
    doObj1.writeSparkDataFrame(data, Seq())
    doObj1.postWrite(Seq())(contextExec)
    assert(constraintType("PUBLIC", "RK_PK_UPDATE", "PRIMARY KEY").isDefined)

    // Change PK to 'name' — orchestration must detect the change and drop+recreate
    val tbl2 = Table(Some("PUBLIC"), "RK_PK_UPDATE", primaryKey = Some(Seq("name")),
      createAndReplaceReferentialKeys = true, primaryKeyConstraintName = Some("PK_RK_PK_UPDATE"))
    val doObj2 = JdbcTableDataObject("rkDO_pk_update2", table = tbl2, connectionId = "rkCon1")
    doObj2.postWrite(Seq())(contextExec)

    assert(constraintType("PUBLIC", "RK_PK_UPDATE", "PRIMARY KEY").isDefined)
    assert(!isColumnNullable("PUBLIC", "RK_PK_UPDATE", "name"))
  }

  // ── Foreign key ───────────────────────────────────────────────────────────

  test("FK constraint is created on write") {
    instanceRegistry.register(jdbcConnection)
    // Populate referenced table first so the FK constraint can validate existing rows.
    val refTbl = Table(Some("PUBLIC"), "RK_FK_REF")
    val refDO = JdbcTableDataObject("rkDO_fk_ref", table = refTbl, connectionId = "rkCon1",
      createSql = Some("CREATE TABLE PUBLIC.RK_FK_REF (id INTEGER NOT NULL PRIMARY KEY, name varchar(255))"))
    refDO.dropTable
    refDO.prepare
    val refData = Seq((1, "one"), (2, "two")).toDF("id", "name")
    refDO.initSparkDataFrame(refData, Seq())
    refDO.writeSparkDataFrame(refData, Seq())

    val fk = ForeignKey(db = Some("PUBLIC"), table = "RK_FK_REF", columns = Map("ref_id" -> "id"),
      name = Some("FK_RK_FK_MAIN_REF"))
    val tbl = Table(Some("PUBLIC"), "RK_FK_MAIN",
      foreignKeys = Some(Seq(fk)),
      createAndReplaceReferentialKeys = true)
    val doObj = JdbcTableDataObject("rkDO_fk_main", table = tbl, connectionId = "rkCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "ref_id INTEGER, name varchar(255)"))
    doObj.dropTable
    val data = Seq((1, "alice"), (2, "bob")).toDF("ref_id", "name")
    doObj.prepare
    doObj.initSparkDataFrame(data, Seq())
    doObj.writeSparkDataFrame(data, Seq())
    doObj.postWrite(Seq())(contextExec)

    assert(constraintType("PUBLIC", "RK_FK_MAIN", "FOREIGN KEY").isDefined,
      "FOREIGN KEY constraint should exist after write")
  }

  test("FK provisioning is idempotent across multiple writes") {
    instanceRegistry.register(jdbcConnection)
    val refTbl = Table(Some("PUBLIC"), "RK_FK_REF2")
    val refDO = JdbcTableDataObject("rkDO_fk_ref2", table = refTbl, connectionId = "rkCon1",
      createSql = Some("CREATE TABLE PUBLIC.RK_FK_REF2 (id INTEGER NOT NULL PRIMARY KEY, name varchar(255))"))
    refDO.dropTable
    refDO.prepare
    val refData = Seq((1, "one")).toDF("id", "name")
    refDO.initSparkDataFrame(refData, Seq())
    refDO.writeSparkDataFrame(refData, Seq())

    val fk = ForeignKey(db = Some("PUBLIC"), table = "RK_FK_REF2", columns = Map("ref_id" -> "id"),
      name = Some("FK_RK_FK_IDEM_REF2"))
    val tbl = Table(Some("PUBLIC"), "RK_FK_IDEM",
      foreignKeys = Some(Seq(fk)),
      createAndReplaceReferentialKeys = true)
    val doObj = JdbcTableDataObject("rkDO_fk_idem", table = tbl, connectionId = "rkCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "ref_id INTEGER, name varchar(255)"),
      saveMode = SDLSaveMode.Append)
    doObj.dropTable
    val data = Seq((1, "alice")).toDF("ref_id", "name")
    doObj.prepare
    doObj.initSparkDataFrame(data, Seq())
    doObj.writeSparkDataFrame(data, Seq())
    doObj.postWrite(Seq())(contextExec)
    // second write — must not fail or duplicate the constraint
    doObj.writeSparkDataFrame(data, Seq())
    doObj.postWrite(Seq())(contextExec)

    assert(constraintType("PUBLIC", "RK_FK_IDEM", "FOREIGN KEY").isDefined)
  }

  test("duplicate FK constraint names throw ConfigurationException") {
    instanceRegistry.register(jdbcConnection)
    // Both FKs reference the same table with no explicit name → same default sdlb_..._fk_SOME_REF
    val fk1 = ForeignKey(db = Some("PUBLIC"), table = "SOME_REF", columns = Map("a" -> "id"), name = None)
    val fk2 = ForeignKey(db = Some("PUBLIC"), table = "SOME_REF", columns = Map("b" -> "id"), name = None)
    val tbl = Table(Some("PUBLIC"), "RK_FK_DUP",
      foreignKeys = Some(Seq(fk1, fk2)),
      createAndReplaceReferentialKeys = true)
    val doObj = JdbcTableDataObject("rkDO_fk_dup", table = tbl, connectionId = "rkCon1")
    doObj.dropTable
    val data = Seq((1, 2)).toDF("a", "b")
    doObj.prepare
    doObj.initSparkDataFrame(data, Seq())
    doObj.writeSparkDataFrame(data, Seq())

    intercept[ConfigurationException] {
      doObj.postWrite(Seq())(contextExec)
    }
  }
}
