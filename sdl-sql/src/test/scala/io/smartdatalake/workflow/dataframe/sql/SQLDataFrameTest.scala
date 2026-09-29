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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.python.JepInterpreter
import io.smartdatalake.util.sqlglot.SqlGlotException
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer
import io.smartdatalake.workflow.connection.SQLEngineConnection
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Tests for SQLDataFrame. They need a Python environment with sqlglot and jep, see sdl-sql/pyproject.toml,
 * and cancel themselves if there is none.
 */
class SQLDataFrameTest extends AnyFunSuite {

  import SQLSubFeed._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  // SQL of transformers is written in Spark SQL, the target database is Postgres
  instanceRegistry.register(SQLEngineConnection(ConnectionId(Environment.defaultEngineConnectionId), dialect = "postgres", sqlDialect = Some("spark")))
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = JepInterpreter.unavailableReason
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
      """SELECT "_v"."num" AS "num", CAST("_v"."str" AS TEXT) AS "str" FROM (VALUES (1, 'a'), (2, 'it''s')) AS "_v"("num", "str")""")
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
    val bridge = getBridge
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

  test("reading data is not supported yet") {
    intercept[NotImplementedError](testTable.count)
    intercept[NotImplementedError](testTable.collect)
  }
}
