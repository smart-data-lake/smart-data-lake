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
package io.smartdatalake.workflow.dataframe.sql

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.sql.SQLTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.sqlglot.SqlGlotException
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer
import io.smartdatalake.workflow.connection.SQLEngineConnection
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Tests for SQLDataFrame, executing SQL on a DuckDB database. The SQL of transformers is written in Spark SQL.
 * Tests of the rendered SQL use other dialects explicitly.
 *
 * The tests need a Python environment with sqlglot and jep, see sdl-sql/pyproject.toml, and cancel themselves if
 * there is none.
 */
class SQLDataFrameTest extends AnyFunSuite {

  import SQLSubFeed._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  private val connection: SQLEngineConnection = SQLTestUtil.createEngineConnection("SQLDataFrameTest")
  instanceRegistry.register(connection)
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  connection.execJdbcStatement("create schema db")
  connection.execJdbcStatement("create table db.test_table (a int, b int, c varchar)")
  connection.execJdbcStatement("insert into db.test_table values (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'it''s'), (4, null, null)")
  connection.execJdbcStatement("create table other (a int, z decimal(10, 2))")
  connection.execJdbcStatement("insert into other values (1, 1.5), (3, -2.25), (5, 0)")

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = SQLTestUtil.pythonUnavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

  private val schema = SQLSchema(Seq(
    SQLField("a", SQLSimpleDataType("INT")),
    SQLField("b", SQLSimpleDataType("INT")),
    SQLField("c", SQLSimpleDataType("TEXT"))
  ))

  private def testTable: SQLDataFrame = table("db.test_table", schema)

  private def otherTable: SQLDataFrame = table("other", SQLSchema(Seq(SQLField("a", SQLSimpleDataType("INT")), SQLField("z", SQLSimpleDataType("DECIMAL(10, 2)")))))

  test("transformations are merged into one statement") {
    // the example of issue #866 with sqlframe
    val df = testTable.withColumn("d", col("a") * lit(2))
    df.createOrReplaceTempView("test_table_int")
    val dfResult = SQLSubFeed.sql("select *, d * 2 as e from test_table_int", DataObjectId("do1"))
      .withColumn("x", col("e") * lit(2))
    assert(dfResult.columns == Seq("a", "b", "c", "d", "e", "x"))
    assert(dfResult.toSql(Some("postgres"), pretty = true) ==
      """SELECT
        |  "test_table"."a" AS "a",
        |  "test_table"."b" AS "b",
        |  "test_table"."c" AS "c",
        |  "test_table"."a" * 2 AS "d",
        |  "test_table"."a" * 4 AS "e",
        |  "test_table"."a" * 8 AS "x"
        |FROM db.test_table AS "test_table"""".stripMargin)
    assert(dfResult.schema.fields.map(_.dataType.sql).distinct == Seq("INT", "TEXT"))
  }

  test("filter and select with column expressions") {
    val df = testTable
      .filter(col("a") > lit(1) and col("c").isin("x", "it's"))
      .select(Seq(
        col("a").as("x"),
        ((col("a") + lit(-1)) * lit(2)).as("y"),
        when(col("a") === lit(1), lit("one")).otherwise(lit("other")).as("w"),
        col("b").cast(stringType)
      ))
    assert(df.columns == Seq("x", "y", "w", "b"))
    assert(df.toSql(Some("postgres")) ==
      """SELECT "test_table"."a" AS "x", ("test_table"."a" + -1) * 2 AS "y", """ +
        """CASE WHEN "test_table"."a" = 1 THEN 'one' ELSE 'other' END AS "w", CAST("test_table"."b" AS TEXT) AS "b" """ +
        """FROM db.test_table AS "test_table" WHERE "test_table"."a" > 1 AND "test_table"."c" IN ('x', 'it''s')""")
    assert(df.schema.fields.map(f => (f.name, f.dataType.sql)) == Seq(("x", "INT"), ("y", "INT"), ("w", "VARCHAR"), ("b", "TEXT")))
  }

  test("sql is rendered in the dialect of the database") {
    val df = testTable.orderBy(Seq(col("a").desc)).limit(3)
    assert(df.toSql(Some("tsql")) ==
      "SELECT TOP 3 [test_table].[a] AS [a], [test_table].[b] AS [b], [test_table].[c] AS [c] FROM db.test_table AS [test_table] ORDER BY [test_table].[a] DESC")
    assert(df.toSql(Some("snowflake")).endsWith("""ORDER BY "test_table"."a" DESC NULLS LAST LIMIT 3"""))
  }

  test("join on columns") {
    val df = testTable.join(otherTable, Seq("a"), "left")
    assert(df.columns == Seq("a", "b", "c", "z"))
    assert(df.toSql() ==
      """SELECT "test_table"."a" AS "a", "test_table"."b" AS "b", "test_table"."c" AS "c", "other"."z" AS "z" """ +
        """FROM db.test_table AS "test_table" LEFT JOIN other AS "other" ON "other"."a" = "test_table"."a"""")
    assert(df.schema.getDataType("z").sql == "DECIMAL(10, 2)")
  }

  test("join with condition and columns referenced by DataFrame alias") {
    val dfLeft = testTable.as("l")
    val dfRight = otherTable.as("r")
    val df = dfLeft.join(dfRight, dfLeft("a") === dfRight("a"), "inner")
      .filter(dfRight("z") > lit(0))
      .select(Seq(dfLeft("a"), dfRight("z")))
    assert(df.columns == Seq("a", "z"))
    assert(df.toSql() ==
      """SELECT "test_table"."a" AS "a", "other"."z" AS "z" FROM db.test_table AS "test_table" """ +
        // the optimizer moves the filter into the condition of the inner join
        """JOIN other AS "other" ON "other"."a" = "test_table"."a" AND "other"."z" > 0""")
  }

  test("group by and aggregate") {
    val df = testTable.groupBy(Seq(col("c"))).agg(Seq(count(col("*")).as("cnt"), max(col("a")).as("m")))
    assert(df.toSql() ==
      """SELECT "test_table"."c" AS "c", COUNT(*) AS "cnt", MAX("test_table"."a") AS "m" FROM db.test_table AS "test_table" GROUP BY "test_table"."c"""")
    assert(df.schema.fields.map(f => (f.name, f.dataType.sql)) == Seq(("c", "TEXT"), ("cnt", "BIGINT"), ("m", "INT")))
    // aggregation over all rows
    assert(testTable.agg(Seq(min(col("a")).as("m"))).toSql() == """SELECT MIN("test_table"."a") AS "m" FROM db.test_table AS "test_table"""")
  }

  test("union, except, distinct and drop duplicates") {
    val df = testTable.unionByName(otherTable, allowMissingColumns = true)
    assert(df.columns == Seq("a", "b", "c", "z"))
    assert(df.toSql().contains(" UNION ALL "))
    assert(testTable.except(testTable).toSql().contains(" EXCEPT "))
    assert(testTable.distinct.toSql().startsWith("SELECT DISTINCT"))
    val dfDedup = testTable.dropDuplicates(Seq("a"))
    assert(dfDedup.columns == Seq("a", "b", "c"))
    assert(dfDedup.toSql().contains("ROW_NUMBER() OVER (PARTITION BY"))
  }

  test("drop, rename and symmetric difference") {
    assert(testTable.drop("b").columns == Seq("a", "c"))
    assert(testTable.withColumnRenamed("a", "x").columns == Seq("x", "b", "c"))
    assert(testTable.symmetricDifference(testTable).columns == Seq("a", "b", "c", "_in_first_df"))
  }

  test("DataFrame from values") {
    import implicits._
    val df = Seq((1, "a"), (2, "it's")).toDF("num", "str").asInstanceOf[SQLDataFrame]
    assert(df.columns == Seq("num", "str"))
    assert(df.toSql(Some("postgres")) ==
      """SELECT CAST("_v"."num" AS INT) AS "num", CAST("_v"."str" AS TEXT) AS "str" FROM (VALUES (1, 'a'), (2, 'it''s')) AS "_v"("num", "str")""")
  }

  test("empty DataFrame") {
    val df = schema.getEmptyDataFrame(DataObjectId("do1"))
    assert(df.columns == Seq("a", "b", "c"))
    assert(df.toSql(Some("postgres")) == """SELECT CAST(NULL AS INT) AS "a", CAST(NULL AS INT) AS "b", CAST(NULL AS TEXT) AS "c" WHERE FALSE""")
  }

  test("SQLDfTransformer translates Spark SQL") {
    val transformer = SQLDfTransformer(code = Some("select `a`, nvl(c, 'x') as c2, %{option1} from %{inputViewName} where a > 1"))
    val df = transformer.transformWithOptions(ActionId("action1"), Seq(), testTable, DataObjectId("src1"), Map("option1" -> "b"))
      .asInstanceOf[SQLDataFrame]
    assert(df.columns == Seq("a", "c2", "b"))
    assert(df.toSql(Some("postgres")) ==
      """SELECT "test_table"."a" AS "a", COALESCE("test_table"."c", 'x') AS "c2", "test_table"."b" AS "b" """ +
        """FROM db.test_table AS "test_table" WHERE "test_table"."a" > 1""")
  }

  test("partition values and filters are applied") {
    val subFeed = getSubFeed(testTable, DataObjectId("do1"), Seq())
      .withFilters(Seq(PartitionValues(Map("c" -> "x")), PartitionValues(Map("c" -> "y"))), Seq())
    val df = subFeed.dataFrame.get.asInstanceOf[SQLDataFrame]
    assert(df.toSql().endsWith("""WHERE "test_table"."c" IN ('x', 'y')"""))
  }

  test("expressions are parsed in the default SQLGlot dialect") {
    val df = testTable.filter(expr("a > 1 and c like 'x%'"))
    assert(df.toSql().endsWith("""WHERE "test_table"."a" > 1 AND "test_table"."c" LIKE 'x%'"""))
  }

  test("column SQL round-trips through SQLGlot") {
    val bridge = connection.bridge
    val columns = Seq(
      (col("a") + lit(1)) * lit(2),
      col("a") > lit(1) and col("b").isNull or not(col("c") === lit("x")),
      col("a") <=> lit(null),
      col("a").isin(1, 2),
      when(col("a") === lit(1), lit("one")).otherwise(lit("other")),
      col("a").cast(createSimpleDataType("decimal(10,2)")),
      coalesce(col("a"), lit(-1)),
      countDistinct(col("a")),
      window(() => row_number, Seq(col("b")), col("a").desc),
      col("s")("f"),
      col("s")(0)
    )
    columns.foreach { c =>
      val sql = SQLColumn.of(c).expr
      // parsing and rendering must not change the expression, apart from redundant parentheses
      assert(bridge.transpile(sql, None, None).replace("(", "").replace(")", "") == sql.replace("(", "").replace(")", ""), sql)
    }
  }

  test("errors of SQLGlot are thrown as SqlGlotException") {
    val ex = intercept[SqlGlotException](testTable.select(col("unknown")))
    assert(ex.getMessage.contains("could not be resolved"))
    assert(ex.pythonTraceback.contains("Traceback"))
    intercept[SqlGlotException](SQLSubFeed.sql("select * from nope", DataObjectId("do1")))
  }

  test("DataFrames can be used from multiple threads") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val results = Await.result(Future.sequence((1 to 8).map { i =>
      Future(testTable.withColumn("i", lit(i)).filter(col("a") > lit(i)).toSql())
    }), 60.seconds)
    results.zipWithIndex.foreach { case (sql, idx) =>
      assert(sql.contains(s"""${idx + 1} AS "i"""") && sql.endsWith(s"""WHERE "test_table"."a" > ${idx + 1}"""))
    }
  }

  // executing SQL on the database

  private def rows(df: GenericDataFrame): Seq[Seq[Any]] = df.collect.map(_.toSeq)

  test("collect executes the SQL statement on the database") {
    val df = testTable.filter(col("a") > lit(1)).select(Seq(col("a"), (col("b") * lit(2)).as("b2"))).orderBy(Seq(col("a")))
    assert(rows(df) == Seq(Seq(2, 40), Seq(3, 60), Seq(4, null)))
    assert(df.toDatabaseSql.contains("FROM db.test_table"))
  }

  test("count and isEmpty") {
    assert(testTable.count == 4)
    assert(testTable.filter(col("b").isNull).count == 1)
    assert(!testTable.isEmpty)
    assert(testTable.filter(col("a") > lit(100)).isEmpty)
  }

  test("join and aggregate on the database") {
    val dfJoined = testTable.join(otherTable, Seq("a"), "full_outer").orderBy(Seq(col("a")))
    assert(rows(dfJoined.select(Seq(col("a"), col("c"), col("z")))) == Seq(
      Seq(1, "x", new java.math.BigDecimal("1.50")),
      Seq(2, "y", null),
      Seq(3, "it's", new java.math.BigDecimal("-2.25")),
      Seq(4, null, null),
      Seq(5, null, new java.math.BigDecimal("0.00"))
    ))
    val dfAgg = testTable.groupBy(Seq((col("a") > lit(2)).as("big"))).agg(Seq(count(col("*")).as("cnt"), max(col("b")).as("max_b")))
      .orderBy(Seq(col("big")))
    assert(rows(dfAgg) == Seq(Seq(false, 2L, 20), Seq(true, 2L, 30)))
  }

  test("SQLDfTransformer with Spark SQL is executed on the database") {
    val transformer = SQLDfTransformer(code = Some("select a, nvl(c, 'none') as c2 from %{inputViewName} where a > 2 order by a"))
    val df = transformer.transformWithOptions(ActionId("action1"), Seq(), testTable, DataObjectId("src1"), Map())
    assert(rows(df) == Seq(Seq(3, "it's"), Seq(4, "none")))
  }

  test("DataFrame from values is executed on the database") {
    import implicits._
    val df = Seq((1, "a", 1.5), (2, "b", -1.0)).toDF("num", "str", "dbl")
    assert(rows(df) == Seq(Seq(1, "a", 1.5), Seq(2, "b", -1.0)))
    assert(df.schema.fields.map(_.dataType.sql) == Seq("INT", "TEXT", "DOUBLE"))
  }

  test("arrays and structs are converted") {
    val df = testTable.filter(col("a") === lit(1)).select(Seq(array(col("a"), col("b")).as("arr"), struct(col("a"), col("c")).as("s")))
    val row = df.collect.head
    assert(row.get(0) == Seq(1, 10))
    assert(row.getStruct(1) == SQLRow(Seq(1, "x")))
  }

  test("generic DataFrame functions work on the database") {
    import implicits._
    val df = Seq((1, "a"), (1, "b"), (2, "c"), (3, null)).toDF("id", "v").asInstanceOf[SQLDataFrame]
    assert(df.getPKviolators(Seq("id")).count == 2)
    assert(df.getNulls(Seq("v")).count == 1)
    assert(df.isEqual(df))
    assert(!df.isEqual(df.filter(col("id") > lit(1))))
    assert(df.dropDuplicates(Seq("id")).count == 3)
  }

  test("observations are calculated on the database") {
    val (_, observation) = testTable.setupObservation("obs", Seq(count(col("*")).as("count"), max(col("a")).as("max_a")), isExecPhase = true)
    assert(observation.waitFor() == Map("count" -> 4L, "max_a" -> 4))
  }

  test("show formats the rows") {
    val str = testTable.orderBy(Seq(col("a"))).limit(2).showString()
    assert(str.linesIterator.toSeq == Seq("+-+--+-+", "|a|b |c|", "+-+--+-+", "|1|10|x|", "|2|20|y|", "+-+--+-+"))
  }

  test("DataFrames can be executed from multiple threads") {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val counts = Await.result(Future.sequence((0 to 4).map(i => Future(testTable.filter(col("a") > lit(i)).count))), 60.seconds)
    assert(counts == Seq(4L, 3L, 2L, 1L, 0L))
  }
}
