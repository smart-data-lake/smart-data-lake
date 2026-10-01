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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.sql.SQLTestUtil
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer
import io.smartdatalake.workflow.action.{Action, CopyAction, DataFrameActionImpl}
import io.smartdatalake.workflow.connection.jdbc.{JdbcConnection, JdbcConnectionImpl, TableGrant}
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed
import io.smartdatalake.workflow.dataobject.generic.{CatalogMetadataApplier, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SchemaViolationException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterEach, Outcome}

import java.sql.ResultSet

/**
 * Tests for creating materialized views with the SQL engine on an embedded Postgres database, as DuckDB does not
 * support materialized views.
 *
 * The tests need a Python environment with sqlglot and jep, see sdl-sql/pyproject.toml, and cancel themselves if
 * there is none.
 */
class JdbcMaterializedViewSqlEngineTest extends AnyFunSuite with BeforeAndAfterEach {

  implicit var instanceRegistry: InstanceRegistry = _
  private var connection: JdbcConnection = _
  private val connectionId = ConnectionId("postgres")
  private var schema: String = _

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = SQLTestUtil.pythonUnavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

  private var testNb = 0

  override def beforeEach(): Unit = {
    testNb += 1
    instanceRegistry = new InstanceRegistry
    instanceRegistry.register(ScalaTestUtil.defaultScalaConnection)
    // a new schema for every test
    schema = s"mvtest$testNb"
    connection = SQLTestUtil.createPostgresConnection(schema, connectionId.id)
    instanceRegistry.register(connection)
    connection.execJdbcStatement(s"create table $schema.src (id int, name varchar, city varchar)")
    connection.execJdbcStatement(s"insert into $schema.src values (1, 'bob', 'Bern'), (2, 'ann', 'Basel'), (3, 'joe', 'Bern')")
  }

  override def afterEach(): Unit = {
    instanceRegistry.getConnections.collect { case c: JdbcConnectionImpl => c.pool.close() }
  }

  private def context(action: Option[Action] = None, phase: ExecutionPhase.ExecutionPhase = ExecutionPhase.Init): ActionPipelineContext = {
    val context = ScalaTestUtil.getDefaultActionPipelineContext.copy(phase = phase)
    action.map(context.withAction(_)).getOrElse(context)
  }

  private def tableDataObject(id: String): JdbcTableDataObject = {
    val dataObject = JdbcTableDataObject(DataObjectId(id), table = Table(db = Some(schema), name = id), connectionId = connectionId)
    instanceRegistry.register(dataObject)
    dataObject
  }

  private def materializedViewDataObject(id: String, allowSchemaEvolution: Boolean = true): JdbcViewDataObject = {
    val dataObject = JdbcViewDataObject(DataObjectId(id), table = Table(db = Some(schema), name = id), connectionId = connectionId,
      allowSchemaEvolution = allowSchemaEvolution, materialized = true)
    instanceRegistry.register(dataObject)
    dataObject
  }

  private def query(sql: String): Seq[Seq[Any]] = connection.execJdbcQuery(sql, (rs: ResultSet) => {
    val n = rs.getMetaData.getColumnCount
    Iterator.continually(rs).takeWhile(_.next()).map(r => (1 to n).map(i => r.getObject(i))).toList
  })

  private def materializedViewNames: Seq[String] =
    query(s"select matviewname from pg_matviews where schemaname = '$schema' order by matviewname").map(_.head.toString)

  // the oid changes if the materialized view is dropped and created again
  private def oid(name: String): Any = query(s"select '$schema.$name'::regclass::oid").head.head

  private def grants(name: String): Seq[TableGrant] = connection.catalog.getGrants(schema, name).get

  private def copyAction(inputId: String, outputId: String, sql: String): CopyAction =
    CopyAction(ActionId(s"$inputId-$outputId"), DataObjectId(inputId), DataObjectId(outputId), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some(sql))))

  private def init(action: DataFrameActionImpl, inputIds: Seq[String]): Seq[SQLSubFeed] = {
    val subFeeds = inputIds.map(id => SQLSubFeed(None, DataObjectId(id)))
    action.prepare(context(Some(action)))
    action.init(subFeeds)(context(Some(action)))
    subFeeds
  }

  private def run(action: DataFrameActionImpl, inputIds: Seq[String]): Unit = {
    val subFeeds = init(action, inputIds)
    action.exec(subFeeds)(context(Some(action), ExecutionPhase.Exec))
  }

  private val bernQuery = "select id, name from %{inputViewName} where city = 'Bern'"

  test("a materialized view is created in init phase and refreshed by every run") {
    tableDataObject("src")
    materializedViewDataObject("bern")
    val action = copyAction("src", "bern", bernQuery)
    init(action, Seq("src"))
    assert(materializedViewNames == Seq("bern"))
    assert(query(s"select id from $schema.bern order by id") == Seq(Seq(1), Seq(3)))
    val oidBefore = oid("bern")
    // the stored result changes only when the materialized view is refreshed
    connection.execJdbcStatement(s"insert into $schema.src values (4, 'eve', 'Bern')")
    assert(query(s"select id from $schema.bern order by id") == Seq(Seq(1), Seq(3)))
    run(action, Seq("src"))
    assert(query(s"select id from $schema.bern order by id") == Seq(Seq(1), Seq(3), Seq(4)))
    // the query is unchanged, so it is refreshed and not created again
    assert(oid("bern") == oidBefore)
  }

  test("a materialized view created by the init phase of a run is not refreshed by its exec phase") {
    tableDataObject("src")
    materializedViewDataObject("bern")
    val action = copyAction("src", "bern", bernQuery)
    val subFeeds = init(action, Seq("src"))
    connection.execJdbcStatement(s"insert into $schema.src values (4, 'eve', 'Bern')")
    action.exec(subFeeds)(context(Some(action), ExecutionPhase.Exec))
    assert(query(s"select id from $schema.bern order by id") == Seq(Seq(1), Seq(3)))
  }

  test("a changed query creates the materialized view again and keeps its grants") {
    tableDataObject("src")
    materializedViewDataObject("bern")
    run(copyAction("src", "bern", bernQuery), Seq("src"))
    val role = s"reader_$schema"
    connection.execJdbcStatement(s"drop role if exists $role")
    connection.execJdbcStatement(s"create role $role")
    connection.execJdbcStatement(s"grant select on $schema.bern to $role with grant option")
    connection.execJdbcStatement(s"grant select on $schema.bern to public")
    val expectedGrants = Seq(TableGrant("PUBLIC", "SELECT", grantable = false), TableGrant(role, "SELECT", grantable = true))
    assert(grants("bern") == expectedGrants)
    val oidBefore = oid("bern")
    run(copyAction("src", "bern", "select id, upper(name) as name from %{inputViewName} where city = 'Bern'"), Seq("src"))
    assert(oid("bern") != oidBefore)
    assert(query(s"select name from $schema.bern order by id") == Seq(Seq("BOB"), Seq("JOE")))
    assert(grants("bern") == expectedGrants)
    assert(query(s"select has_table_privilege('$role', '$schema.bern', 'SELECT WITH GRANT OPTION')") == Seq(Seq(true)))
  }

  test("with allowSchemaEvolution = false a changed query only refreshes the materialized view") {
    tableDataObject("src")
    materializedViewDataObject("bern", allowSchemaEvolution = false)
    run(copyAction("src", "bern", bernQuery), Seq("src"))
    val oidBefore = oid("bern")
    // same columns: the materialized view is refreshed with its existing query
    connection.execJdbcStatement(s"insert into $schema.src values (4, 'eve', 'Bern')")
    run(copyAction("src", "bern", "select id, name from %{inputViewName}"), Seq("src"))
    assert(oid("bern") == oidBefore)
    assert(query(s"select id from $schema.bern order by id") == Seq(Seq(1), Seq(3), Seq(4)))
    // other columns: the run fails
    val ex = intercept[Exception](run(copyAction("src", "bern", "select id from %{inputViewName}"), Seq("src")))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[SchemaViolationException]))
  }

  test("the definition of an existing materialized view is compared with the query") {
    tableDataObject("src")
    val viewDO = materializedViewDataObject("bern")
    val action = copyAction("src", "bern", bernQuery)
    run(action, Seq("src"))
    implicit val contextInit: ActionPipelineContext = context(Some(action))
    val definition = viewDO.getExistingViewDefinition
    assert(definition.isDefined)
    val query = s"SELECT src.id AS id, src.name AS name FROM $schema.src AS src WHERE src.city = 'Bern'"
    assert(viewDO.isSameViewQuery(definition.get, query))
    assert(!viewDO.isSameViewQuery(definition.get, s"SELECT src.id AS id, src.name AS name FROM $schema.src AS src"))
  }

  test("CatalogMetadataApplier creates and replaces a materialized view with its exported query, keeping its grants") {
    tableDataObject("src")
    val viewDO = materializedViewDataObject("v", allowSchemaEvolution = false)
    var exportedQuery = s"SELECT src.id AS id FROM $schema.src AS src WHERE src.city = 'Bern'"
    val applier = new CatalogMetadataApplier(_ => None, viewQueryReader = _ => Some(exportedQuery))
    implicit val contextInit: ActionPipelineContext = context()
    def planAndApply(): Seq[String] = {
      val changes = applier.plan(viewDO).get
      applier.applyView(viewDO, changes)
      changes.describeView
    }
    // the materialized view is created
    assert(planAndApply() == Seq(s"create or replace materialized view as $exportedQuery"))
    assert(query(s"select id from $schema.v order by id") == Seq(Seq(1), Seq(3)))
    // it is up to date
    assert(planAndApply().isEmpty)
    // a changed query replaces it, and the grants are kept
    connection.execJdbcStatement(s"grant select on $schema.v to public")
    exportedQuery = s"SELECT src.id AS id FROM $schema.src AS src"
    assert(planAndApply().nonEmpty)
    assert(query(s"select id from $schema.v order by id") == Seq(Seq(1), Seq(2), Seq(3)))
    assert(grants("v") == Seq(TableGrant("PUBLIC", "SELECT", grantable = false)))
  }

  test("the materialized view is dropped") {
    tableDataObject("src")
    val viewDO = materializedViewDataObject("bern")
    val action = copyAction("src", "bern", bernQuery)
    run(action, Seq("src"))
    viewDO.dropTable(context(Some(action)))
    assert(materializedViewNames.isEmpty)
  }
}
