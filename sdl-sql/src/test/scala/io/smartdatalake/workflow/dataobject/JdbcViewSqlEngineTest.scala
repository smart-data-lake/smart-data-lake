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

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeGenericOptions}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.sql.SQLTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.DataObjectStateIncrementalMode
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer
import io.smartdatalake.workflow.action.{Action, CopyAction, DataFrameActionImpl}
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SubFeed}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterEach, Outcome}

import java.sql.ResultSet
import scala.reflect.runtime.universe.typeOf

/**
 * Tests for creating views with the SQL engine on a DuckDB database with JdbcViewDataObjects.
 *
 * The tests need a Python environment with sqlglot and jep, see sdl-sql/pyproject.toml, and cancel themselves if
 * there is none.
 */
class JdbcViewSqlEngineTest extends AnyFunSuite with BeforeAndAfterEach {

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
    connection.execJdbcStatement("create table src (id int, name varchar, city varchar)")
    connection.execJdbcStatement("insert into src values (1, 'bob', 'Bern'), (2, 'ann', 'Basel'), (3, 'joe', 'Bern')")
  }

  override def afterEach(): Unit = {
    instanceRegistry.getConnections.collect { case c: JdbcTableConnection => c.pool.close() }
  }

  private def context(action: Option[Action] = None, phase: ExecutionPhase.ExecutionPhase = ExecutionPhase.Init): ActionPipelineContext = {
    val context = ScalaTestUtil.getDefaultActionPipelineContext.copy(phase = phase)
    action.map(context.withAction(_)).getOrElse(context)
  }

  private def tableDataObject(id: String, virtualPartitions: Seq[String] = Seq()): JdbcTableDataObject = {
    val dataObject = JdbcTableDataObject(DataObjectId(id), table = Table(db = Some("main"), name = id), connectionId = connectionId,
      virtualPartitions = virtualPartitions)
    instanceRegistry.register(dataObject)
    dataObject
  }

  private def viewDataObject(id: String, virtualPartitions: Seq[String] = Seq(), incrementalOutputExpr: Option[String] = None): JdbcViewDataObject = {
    val dataObject = JdbcViewDataObject(DataObjectId(id), table = Table(db = Some("main"), name = id), connectionId = connectionId,
      virtualPartitions = virtualPartitions, incrementalOutputExpr = incrementalOutputExpr)
    instanceRegistry.register(dataObject)
    dataObject
  }

  private def query(sql: String): Seq[Seq[Any]] = connection.execJdbcQuery(sql, (rs: ResultSet) => {
    val n = rs.getMetaData.getColumnCount
    Iterator.continually(rs).takeWhile(_.next()).map(r => (1 to n).map(i => r.getObject(i))).toList
  })

  private def viewNames: Seq[String] = query("select view_name from duckdb_views() where not internal order by view_name").map(_.head.toString)

  private def copyAction(inputId: String, outputId: String, sql: String): CopyAction =
    CopyAction(ActionId(s"$inputId-$outputId"), DataObjectId(inputId), DataObjectId(outputId), engineConnectionId = Some(connectionId),
      transformers = Seq(SQLDfTransformer(code = Some(sql))))

  private def init(action: DataFrameActionImpl, inputIds: Seq[String], partitionValues: Seq[PartitionValues]): Seq[SQLSubFeed] = {
    val subFeeds = inputIds.map(id => SQLSubFeed(None, DataObjectId(id), partitionValues))
    action.prepare(context(Some(action)))
    action.init(subFeeds)(context(Some(action)))
    subFeeds
  }

  private def run(action: DataFrameActionImpl, inputIds: Seq[String], partitionValues: Seq[PartitionValues] = Seq()): Unit = {
    val subFeeds = init(action, inputIds, partitionValues)
    action.exec(subFeeds)(context(Some(action), ExecutionPhase.Exec))
  }

  /**
   * Run Actions one after the other like a DAG: the output SubFeeds of an Action are the input SubFeeds of the next.
   */
  private def runChain(actions: Seq[DataFrameActionImpl], inputId: String, partitionValues: Seq[PartitionValues]): Seq[SubFeed] = {
    val startSubFeeds: Seq[SubFeed] = Seq(SQLSubFeed(None, DataObjectId(inputId), partitionValues, isDAGStart = true))
    actions.foreach(a => a.prepare(context(Some(a))))
    actions.foldLeft(startSubFeeds)((subFeeds, a) => a.init(subFeeds)(context(Some(a))))
    actions.foldLeft(startSubFeeds)((subFeeds, a) => a.exec(subFeeds)(context(Some(a), ExecutionPhase.Exec)))
  }

  test("CopyAction creates a view with the query of its transformer") {
    tableDataObject("src")
    viewDataObject("bern")
    val action = copyAction("src", "bern", "select id, upper(name) as name from %{inputViewName} where city = 'Bern'")
    assert(action.subFeedType =:= typeOf[SQLSubFeed])
    // init phase does not change the database
    init(action, Seq("src"), Seq())
    assert(viewNames.isEmpty)
    run(action, Seq("src"))
    assert(viewNames == Seq("bern"))
    assert(query("select id, name from bern order by id") == Seq(Seq(1, "BOB"), Seq(3, "JOE")))
    // the view shows new data without running the Action again
    connection.execJdbcStatement("insert into src values (4, 'kim', 'Bern')")
    assert(query("select count(*) from bern") == Seq(Seq(3L)))
  }

  test("the view is replaced if its query changes") {
    tableDataObject("src")
    viewDataObject("v")
    run(copyAction("src", "v", "select id from %{inputViewName}"), Seq("src"))
    assert(query("select * from v order by id") == Seq(Seq(1), Seq(2), Seq(3)))
    run(copyAction("src", "v", "select id, city from %{inputViewName} where id > 1"), Seq("src"))
    assert(query("select * from v order by id") == Seq(Seq(2, "Basel"), Seq(3, "Bern")))
  }

  test("a view can be read by the next Action") {
    tableDataObject("src")
    viewDataObject("v1")
    viewDataObject("v2")
    tableDataObject("tgt")
    run(copyAction("src", "v1", "select id, city from %{inputViewName} where city = 'Bern'"), Seq("src"))
    // a view on a view
    run(copyAction("v1", "v2", "select city, count(*) as cnt from %{inputViewName} group by city"), Seq("v1"))
    // materialize the view in a table
    run(copyAction("v2", "tgt", "select * from %{inputViewName}"), Seq("v2"))
    assert(query("select city, cnt from tgt") == Seq(Seq("Bern", 2L)))
    assert(viewNames == Seq("v1", "v2"))
  }

  test("partition values of the input are not applied to the view") {
    tableDataObject("src", virtualPartitions = Seq("city"))
    viewDataObject("v")
    tableDataObject("tgt")
    val partitionValues = Seq(PartitionValues(Map("city" -> "Bern")))
    run(copyAction("src", "v", "select * from %{inputViewName}"), Seq("src"), partitionValues)
    assert(query("select count(*) from v") == Seq(Seq(3L)))
    // in contrast to writing a table
    run(copyAction("src", "tgt", "select * from %{inputViewName}"), Seq("src"), partitionValues)
    assert(query("select count(*) from tgt") == Seq(Seq(2L)))
  }

  test("partition values are passed on by a partitioned view to the next Action") {
    tableDataObject("src", virtualPartitions = Seq("city"))
    val viewDO = viewDataObject("v", virtualPartitions = Seq("city"))
    tableDataObject("tgt", virtualPartitions = Seq("city"))
    val createView = copyAction("src", "v", "select * from %{inputViewName}")
    val copyView = copyAction("v", "tgt", "select * from %{inputViewName}")
    val partitionValues = Seq(PartitionValues(Map("city" -> "Bern")))
    val outputSubFeeds = runChain(Seq(createView, copyView), "src", partitionValues)
    // the view is not filtered
    assert(query("select count(*) from v") == Seq(Seq(3L)))
    assert(viewDO.listPartitions(context(Some(copyView))).toSet == Set(PartitionValues(Map("city" -> "Bern")), PartitionValues(Map("city" -> "Basel"))))
    // but the next Action reads the partition values passed on by the view
    assert(query("select id from tgt order by id") == Seq(Seq(1), Seq(3)))
    assert(outputSubFeeds.map(_.partitionValues) == Seq(partitionValues))
  }

  test("the next Action reads the view incrementally") {
    tableDataObject("src")
    viewDataObject("v", incrementalOutputExpr = Some("id"))
    tableDataObject("tgt")
    run(copyAction("src", "v", "select id, name from %{inputViewName}"), Seq("src"))
    // the Action reading the view has DataObjectStateIncrementalMode, which sets the state of the view
    val readView = CopyAction(ActionId("v-tgt"), DataObjectId("v"), DataObjectId("tgt"), engineConnectionId = Some(connectionId),
      executionMode = Some(DataObjectStateIncrementalMode()), saveModeOptions = Some(SaveModeGenericOptions(SDLSaveMode.Append)))
    val viewDO = instanceRegistry.get[JdbcViewDataObject](DataObjectId("v"))
    viewDO.setState(None)(context(Some(readView)))
    run(readView, Seq("v"))
    assert(query("select id from tgt order by id") == Seq(Seq(1), Seq(2), Seq(3)))
    assert(viewDO.getState.contains("id;3;INT"))
    // only new data is read on the next run
    connection.execJdbcStatement("insert into src values (4, 'kim', 'Bern')")
    viewDO.setState(viewDO.getState)(context(Some(readView)))
    run(readView, Seq("v"))
    assert(query("select id from tgt order by id") == Seq(Seq(1), Seq(2), Seq(3), Seq(4)))
  }

  test("the partition columns must exist in the view") {
    tableDataObject("src")
    viewDataObject("v", virtualPartitions = Seq("zip"))
    val ex = intercept[Exception](run(copyAction("src", "v", "select * from %{inputViewName}"), Seq("src")))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.getMessage.contains("zip")))
  }

  test("an Action writing a view can not have an execution mode") {
    tableDataObject("src")
    viewDataObject("v")
    val ex = intercept[ConfigurationException](CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("v"),
      engineConnectionId = Some(connectionId), executionMode = Some(DataObjectStateIncrementalMode())))
    assert(ex.getMessage.contains("executionMode is not supported"))
  }

  test("save modes other than overwrite are not supported") {
    tableDataObject("src")
    viewDataObject("v")
    val action = CopyAction(ActionId("a1"), DataObjectId("src"), DataObjectId("v"), engineConnectionId = Some(connectionId),
      saveModeOptions = Some(SaveModeGenericOptions(SDLSaveMode.Append)))
    val ex = intercept[Exception](run(action, Seq("src")))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.getMessage.contains("saveMode Append is not supported")))
  }

  test("an invalid query fails in init phase") {
    tableDataObject("src")
    viewDataObject("v")
    val action = copyAction("src", "v", "select no_such_function(id) as x from %{inputViewName}")
    val ex = intercept[Exception](init(action, Seq("src"), Seq()))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.getMessage.toLowerCase.contains("no_such_function")))
    assert(viewNames.isEmpty)
  }

  test("the view is dropped") {
    tableDataObject("src")
    val viewDO = viewDataObject("v")
    run(copyAction("src", "v", "select id from %{inputViewName}"), Seq("src"))
    val contextInit = context(Some(copyAction("src", "v", "select id from %{inputViewName}")))
    assert(viewDO.isTableExisting(contextInit))
    viewDO.dropTable(contextInit)
    assert(!viewDO.isTableExisting(contextInit))
    assert(viewNames.isEmpty)
  }

  test("JdbcViewDataObject is parsable") {
    val config = ConfigFactory.parseString(
      """
        |id = v
        |connectionId = duckdb
        |table = { name = my_view }
        |""".stripMargin)
    val dataObject = JdbcViewDataObject.fromConfig(config)
    assert(dataObject.id == DataObjectId("v"))
    // db is taken from the connection
    assert(dataObject.table.fullName == "main.my_view")
  }

  test("a query in table is not supported") {
    val ex = intercept[ConfigurationException](JdbcViewDataObject(DataObjectId("v"),
      table = Table(db = Some("main"), name = "v", query = Some("select 1")), connectionId = connectionId))
    assert(ex.getMessage.contains("table.query is not supported"))
  }
}
