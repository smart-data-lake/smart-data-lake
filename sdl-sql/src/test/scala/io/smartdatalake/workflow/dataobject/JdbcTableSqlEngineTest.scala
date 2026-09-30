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

import io.smartdatalake.app.TestMode
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.sql.SQLTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.generic.transformer.{SQLDfTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.action.{Action, CopyAction, CustomDataFrameAction, DataFrameActionImpl}
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.sql.{SQLDataFrame, SQLSimpleDataType, SQLSubFeed}
import io.smartdatalake.workflow.dataobject.generic.{AddColumn, ChangeColumnNullable, ChangeColumnType, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterEach, Outcome}

import java.sql.ResultSet
import scala.reflect.runtime.universe.typeOf

/**
 * Tests for Actions with the SQL engine, executing SQL on a DuckDB database with JdbcTableDataObjects.
 * The Actions select the SQL engine through their engineConnectionId, the default engine is the plain Scala engine.
 *
 * The tests need a Python environment with sqlglot and jep, see sdl-sql/pyproject.toml, and cancel themselves if
 * there is none.
 */
class JdbcTableSqlEngineTest extends AnyFunSuite with BeforeAndAfterEach {

  implicit var instanceRegistry: InstanceRegistry = _
  private var connection: JdbcTableConnection = _
  private val connectionId = ConnectionId("duckdb")

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
    // a new database for every test
    connection = SQLTestUtil.createEngineConnection(s"${getClass.getSimpleName}$testNb", connectionId.id)
    instanceRegistry.register(connection)
    connection.execJdbcStatement("create table src (id int, name varchar, city varchar, score decimal(5, 1))")
    connection.execJdbcStatement("insert into src values (1, 'bob', 'Bern', 3.5), (2, 'ann', 'Basel', 4.0), (3, 'joe', 'Bern', null)")
  }

  override def afterEach(): Unit = {
    instanceRegistry.getConnections.collect { case c: JdbcTableConnection => c.pool.close() }
  }

  private def context(action: Option[Action] = None, phase: ExecutionPhase.ExecutionPhase = ExecutionPhase.Init): ActionPipelineContext = {
    val context = ScalaTestUtil.getDefaultActionPipelineContext.copy(phase = phase)
    action.map(context.withAction(_)).getOrElse(context)
  }

  private def jdbcDataObject(id: String, saveMode: SDLSaveMode = SDLSaveMode.Overwrite, primaryKey: Option[Seq[String]] = None,
                             virtualPartitions: Seq[String] = Seq(), incrementalOutputExpr: Option[String] = None, connectionId: ConnectionId = connectionId,
                             allowSchemaEvolution: Boolean = false): JdbcTableDataObject = {
    val dataObject = JdbcTableDataObject(DataObjectId(id), table = Table(db = Some("main"), name = id, primaryKey = primaryKey), saveMode = saveMode,
      connectionId = connectionId, virtualPartitions = virtualPartitions, incrementalOutputExpr = incrementalOutputExpr,
      allowSchemaEvolution = allowSchemaEvolution)
    instanceRegistry.register(dataObject)
    dataObject
  }

  private def query(sql: String): Seq[Seq[Any]] = connection.execJdbcQuery(sql, (rs: ResultSet) => {
    val n = rs.getMetaData.getColumnCount
    Iterator.continually(rs).takeWhile(_.next()).map(r => (1 to n).map(i => r.getObject(i))).toList
  })

  private def run(action: DataFrameActionImpl, inputIds: Seq[String]): Unit = {
    val subFeeds = inputIds.map(id => SQLSubFeed(None, DataObjectId(id)))
    // SQLDfsTransformer looks up the Action in the registry
    if (!instanceRegistry.getActions.contains(action)) instanceRegistry.register(action)
    action.prepare(context(Some(action)))
    action.init(subFeeds)(context(Some(action)))
    action.exec(subFeeds)(context(Some(action), ExecutionPhase.Exec))
  }

  test("CopyAction with SQL transformer is executed on the database") {
    jdbcDataObject("src")
    jdbcDataObject("tgt")
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some("select id, upper(name) as name, nvl(score, 0) * 2 as score2 from %{inputViewName} where city = 'Bern'"))))
    assert(action.subFeedType =:= typeOf[SQLSubFeed])
    run(action, Seq("src"))
    assert(query("select id, name, score2 from tgt order by id") == Seq(Seq(1, "BOB", new java.math.BigDecimal("7.0")), Seq(3, "JOE", new java.math.BigDecimal("0.0"))))
    // overwrite replaces the data
    run(action, Seq("src"))
    assert(query("select count(*) from tgt") == Seq(Seq(2L)))
  }

  test("SQL transformer in another dialect") {
    jdbcDataObject("src")
    jdbcDataObject("tgt")
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some("select top 1 id from %{inputViewName} order by id desc"), sqlDialect = "tsql")))
    run(action, Seq("src"))
    assert(query("select id from tgt") == Seq(Seq(3)))
  }

  test("identifiers are resolved case-insensitively and keep their spelling") {
    connection.execJdbcStatement("""create table mixed ("Name" varchar, "CODE" int)""")
    connection.execJdbcStatement("""insert into mixed values ('bob', 1), ('ann', 2)""")
    jdbcDataObject("mixed")
    jdbcDataObject("tgt", saveMode = SDLSaveMode.Merge, primaryKey = Some(Seq("name")))
    val action = CopyAction(ActionId("a1"), DataObjectId("mixed"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some("select NAME, code * 2 as TownCode from %{inputViewName}"))))
    run(action, Seq("mixed"))
    // the table is created with the spelling of the DataFrame
    assert(query("select column_name from information_schema.columns where table_name = 'tgt' order by ordinal_position") == Seq(Seq("Name"), Seq("TownCode")))
    assert(query("select name, towncode from tgt order by name") == Seq(Seq("ann", 4), Seq("bob", 2)))
    // merge into the existing table
    connection.execJdbcStatement("""update mixed set "CODE" = 5 where "Name" = 'bob'""")
    run(action, Seq("mixed"))
    assert(query("select name, towncode from tgt order by name") == Seq(Seq("ann", 4), Seq("bob", 10)))
  }

  test("append adds data") {
    jdbcDataObject("src")
    jdbcDataObject("tgt", saveMode = SDLSaveMode.Append)
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId))
    run(action, Seq("src"))
    run(action, Seq("src"))
    assert(query("select count(*) from tgt") == Seq(Seq(6L)))
  }

  test("merge updates and inserts by primary key") {
    jdbcDataObject("src")
    jdbcDataObject("tgt", saveMode = SDLSaveMode.Merge, primaryKey = Some(Seq("id")))
    connection.execJdbcStatement("create table tgt (id int primary key, name varchar, city varchar, score decimal(5, 1))")
    connection.execJdbcStatement("insert into tgt values (1, 'old', 'Bern', 1.0), (9, 'other', 'Zurich', 2.0)")
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      saveModeOptions = Some(SaveModeMergeOptions()))
    run(action, Seq("src"))
    assert(query("select id, name from tgt order by id") == Seq(Seq(1, "bob"), Seq(2, "ann"), Seq(3, "joe"), Seq(9, "other")))
    // the temporary table is removed
    assert(!connection.catalog.isTableExisting("main.tgt_sdltmp"))
  }

  test("CustomDataFrameAction with SQLDfsTransformer joining inputs") {
    jdbcDataObject("src")
    connection.execJdbcStatement("create table cities (city varchar, canton varchar)")
    connection.execJdbcStatement("insert into cities values ('Bern', 'BE'), ('Basel', 'BS')")
    jdbcDataObject("cities")
    jdbcDataObject("tgt")
    val action = CustomDataFrameAction(ActionId("a1"), Seq(DataObjectId("src"), DataObjectId("cities")), Seq(DataObjectId("tgt")),
      engineConnectionId = Some(connectionId), transformers = Seq(SQLDfsTransformer(code = Map("tgt" ->
        "select s.id, c.canton from %{inputViewName_src} s join %{inputViewName_cities} c on s.city = c.city"))))
    run(action, Seq("src", "cities"))
    assert(query("select id, canton from tgt order by id") == Seq(Seq(1, "BE"), Seq(2, "BS"), Seq(3, "BE")))
  }

  test("inputs and outputs must use the engine connection") {
    val otherConnection = SQLTestUtil.createEngineConnection(s"${getClass.getSimpleName}${testNb}Other", "other")
    instanceRegistry.register(otherConnection)
    otherConnection.execJdbcStatement("create table src (id int)")
    jdbcDataObject("src", connectionId = otherConnection.id)
    jdbcDataObject("tgt")
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId))
    val ex = intercept[Exception](run(action, Seq("src")))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.getMessage.contains("must use its engine connection")))
  }

  test("virtual partitions are listed and overwritten") {
    val srcDO = jdbcDataObject("src")
    val tgtDO = jdbcDataObject("tgt", virtualPartitions = Seq("city"))
    // the Action is only needed to define the engine connection in the context
    val action = CopyAction(ActionId("a1"), srcDO.id, tgtDO.id, engineConnectionId = Some(connectionId))
    implicit val contextExec: ActionPipelineContext = context(Some(action), ExecutionPhase.Exec)
    val df = srcDO.getDataFrame(Seq(), typeOf[SQLSubFeed])
    tgtDO.init(df, Seq())
    tgtDO.writeDataFrame(df, Seq())
    assert(tgtDO.listPartitions.toSet == Set(PartitionValues(Map("city" -> "Bern")), PartitionValues(Map("city" -> "Basel"))))
    // overwrite partition Bern only
    connection.execJdbcStatement("update src set name = 'new'")
    val dfBern = SQLDataFrame.of(srcDO.getDataFrame(Seq(), typeOf[SQLSubFeed])).filter(SQLSubFeed.col("city") === SQLSubFeed.lit("Bern"))
    tgtDO.writeDataFrame(dfBern, Seq(PartitionValues(Map("city" -> "Bern"))))
    assert(query("select name from tgt order by id") == Seq(Seq("new"), Seq("ann"), Seq("new")))
  }

  test("incremental output reads new data only") {
    val srcDO = jdbcDataObject("src", incrementalOutputExpr = Some("id"))
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("src"), engineConnectionId = Some(connectionId))
    implicit val contextExec: ActionPipelineContext = context(Some(action), ExecutionPhase.Exec)
    srcDO.setState(None)
    assert(srcDO.getDataFrame(Seq(), typeOf[SQLSubFeed]).count == 3)
    val state = srcDO.getState
    assert(state.contains("id;3;INT"))
    connection.execJdbcStatement("insert into src values (4, 'new', 'Bern', 1.0)")
    srcDO.setState(state)
    val df = srcDO.getDataFrame(Seq(), typeOf[SQLSubFeed])
    assert(df.collect.map(_.get(0)) == Seq(4))
  }

  // column name, type and nullability from the DuckDB catalog
  private def columns(table: String): Seq[Seq[Any]] =
    query(s"select column_name, data_type, is_nullable from information_schema.columns where table_name = '$table' order by ordinal_position")

  test("schema evolution adds columns, widens types and makes deleted columns nullable") {
    connection.execJdbcStatement("create table tgt (id int not null, name varchar, old_col varchar not null, score decimal(5, 1))")
    connection.execJdbcStatement("insert into tgt values (9, 'other', 'x', 1.0)")
    jdbcDataObject("src")
    jdbcDataObject("tgt", saveMode = SDLSaveMode.Append, allowSchemaEvolution = true)
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some("select cast(id as bigint) + 1 as id, name, city, cast(score as decimal(10, 2)) as score from %{inputViewName}"))))
    run(action, Seq("src"))
    assert(columns("tgt") == Seq(
      Seq("id", "BIGINT", "NO"),
      Seq("name", "VARCHAR", "YES"),
      Seq("old_col", "VARCHAR", "YES"),
      Seq("score", "DECIMAL(10,2)", "YES"),
      Seq("city", "VARCHAR", "YES")
    ))
    assert(query("select id, old_col, city from tgt order by id") == Seq(Seq(2L, null, "Bern"), Seq(3L, null, "Basel"), Seq(4L, null, "Bern"), Seq(9L, "x", null)))
    // a second run needs no changes
    run(action, Seq("src"))
    assert(query("select count(*) from tgt") == Seq(Seq(7L)))
  }

  test("schema evolution fails for incompatible data types") {
    connection.execJdbcStatement("create table tgt (id int, name int)")
    jdbcDataObject("src")
    jdbcDataObject("tgt", saveMode = SDLSaveMode.Append, allowSchemaEvolution = true)
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some("select id, name from %{inputViewName}"))))
    val ex = intercept[Exception](run(action, Seq("src")))
    val messages = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).toSeq
    assert(messages.exists(_.contains("schema evolution of column name from INT to TEXT is not supported")), messages.mkString("\n"))
  }

  test("schema changes are applied") {
    connection.execJdbcStatement("create table tgt (id int not null, name varchar)")
    val tgtDO = jdbcDataObject("tgt")
    val action = CopyAction(ActionId("a1"), DataObjectId("tgt"), DataObjectId("tgt"), engineConnectionId = Some(connectionId))
    implicit val contextInit: ActionPipelineContext = context(Some(action))
    tgtDO.applySchemaChanges(Seq(
      AddColumn(Seq("amount"), SQLSimpleDataType("DECIMAL(10, 2)"), Some("the amount")),
      ChangeColumnType(Seq("id"), SQLSimpleDataType("BIGINT"), SQLSimpleDataType("INT")),
      ChangeColumnNullable(Seq("id"), nullable = true)
    ))
    assert(columns("tgt") == Seq(Seq("id", "BIGINT", "YES"), Seq("name", "VARCHAR", "YES"), Seq("amount", "DECIMAL(10,2)", "YES")))
    assert(query("select comment from duckdb_columns() where table_name = 'tgt' and column_name = 'amount'") == Seq(Seq("the amount")))
    assert(tgtDO.getCurrentSchema.get.columns == Seq("id", "name", "amount"))
  }

  test("column lineage of an Action is collected for the lineage export") {
    jdbcDataObject("src")
    jdbcDataObject("tgt")
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some("select id, upper(name) as name, city as town, 'x' as const from %{inputViewName}"))))
    instanceRegistry.register(action)
    val contextInit = context(Some(action))
    val contextInitExport = contextInit.copy(appConfig = contextInit.appConfig.copy(test = Some(TestMode.DryRunWithLineageExport)))
    action.init(Seq(SQLSubFeed(None, DataObjectId("src"))))(contextInitExport)
    val entry = contextInitExport.columnLineageExportRegistry.getColumnLineages.get(DataObjectId("tgt"))
    assert(entry.isDefined, "no column lineage was collected for export")
    val lineage = entry.get.lineage
    def inputsOf(column: String) = lineage.get(column).toSeq.flatMap(_.inputFields).map(f => (f.dataObjectId.id, f.column, f.transformation.subtype))
    assert(inputsOf("id") == Seq(("src", "id", "IDENTITY")))
    assert(inputsOf("name") == Seq(("src", "name", "TRANSFORMATION")))
    assert(inputsOf("town") == Seq(("src", "city", "IDENTITY")))
    assert(lineage.get("const").exists(_.inputFields.isEmpty))
    assert(lineage.unresolvedColumns.isEmpty)
  }
}
