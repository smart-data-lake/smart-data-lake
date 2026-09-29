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

import io.smartdatalake.util.sqlglot.{DataFrameInfo, SqlGlotBridge}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * DataFrame of the SQL engine. It remote-controls an SQLGlot query in the embedded Python interpreter, and renders
 * it as SQL statement for a database with [[toSql]].
 *
 * Operations that need to read data (collect, count, isEmpty, show) are not supported yet, they need the SQL
 * statement to be executed on the database (see issue #866).
 */
class SQLDataFrame private(val info: DataFrameInfo, @transient val bridge: SqlGlotBridge) extends GenericDataFrame {

  override def subFeedType: Type = typeOf[SQLSubFeed]

  def id: Long = info.id

  def alias: String = info.alias

  override lazy val schema: SQLSchema = SQLSchema.fromBridge(bridge.schema(id))

  override def columns: Seq[String] = info.columns

  /**
   * Render the SQL statement of this DataFrame.
   *
   * @param dialect   SQLGlot dialect of the database, e.g. postgres, tsql, oracle or snowflake. Default is the default SQLGlot dialect.
   * @param optimized if true, the statement is optimized with SQLGlot. This merges the nested subqueries created by
   *                  DataFrame operations and qualifies all columns.
   * @param pretty    if true, the statement is formatted on multiple lines
   */
  def toSql(dialect: Option[String] = None, optimized: Boolean = true, pretty: Boolean = false): String =
    bridge.toSql(id, dialect, optimized, pretty)

  private def op(name: String, args: (String, Any)*): SQLDataFrame =
    SQLDataFrame(bridge.callDataFrame(name, ("df" -> id) +: args: _*), bridge)

  private def other(df: GenericDataFrame): SQLDataFrame = df match {
    case sqlDf: SQLDataFrame => sqlDf
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
  }

  override def join(other: GenericDataFrame, joinCols: Seq[String], joinType: String): SQLDataFrame =
    op("join", "other" -> this.other(other).id, "how" -> joinType, "on" -> joinCols)

  override def join(other: GenericDataFrame, condition: GenericColumn, joinType: String): SQLDataFrame =
    op("join", "other" -> this.other(other).id, "how" -> joinType, "condition" -> SQLColumn.of(condition).expr)

  override def select(columns: Seq[GenericColumn]): SQLDataFrame =
    op("select", "columns" -> columns.map(SQLColumn.of(_).projectionSql))

  override def groupBy(columns: Seq[GenericColumn]): SQLGroupedDataFrame = SQLGroupedDataFrame(this, columns.map(SQLColumn.of))

  override def agg(columns: Seq[GenericColumn]): SQLDataFrame = groupBy(Seq()).agg(columns)

  override def unionByName(other: GenericDataFrame, allowMissingColumns: Boolean): SQLDataFrame =
    op("union_by_name", "other" -> this.other(other).id, "allow_missing_columns" -> allowMissingColumns)

  override def except(other: GenericDataFrame): SQLDataFrame = op("except", "other" -> this.other(other).id)

  override def filter(expression: GenericColumn): SQLDataFrame = op("filter", "condition" -> SQLColumn.of(expression).expr)

  override def limit(n: Int): SQLDataFrame = op("limit", "n" -> n)

  override def orderBy(columns: Seq[GenericColumn]): SQLDataFrame = op("order_by", "columns" -> columns.map(SQLColumn.of(_).orderSql))

  override def distinct: SQLDataFrame = op("distinct")

  override def withColumn(colName: String, expression: GenericColumn): SQLDataFrame =
    op("with_column", "name" -> colName, "column" -> SQLColumn.of(expression).expr)

  override def withColumnRenamed(colName: String, newName: String): SQLDataFrame =
    op("with_column_renamed", "name" -> colName, "new_name" -> newName)

  override def drop(colName: String): SQLDataFrame = drop(Seq(colName))

  override def drop(cols: Seq[String]): SQLDataFrame = op("drop", "names" -> cols)

  override def drop(col: GenericColumn): SQLDataFrame = drop(SQLColumn.of(col).getName
    .getOrElse(throw new IllegalArgumentException(s"Can only drop named columns, but got ${col.exprSql}")))

  override def createOrReplaceTempView(viewName: String): Unit = bridge.registerView(viewName, id)

  override def dropDuplicates(cols: Seq[String]): SQLDataFrame = op("drop_duplicates", "columns" -> cols)

  override def as(alias: String): SQLDataFrame = op("alias", "alias" -> alias)

  override def apply(columnName: String): SQLColumn =
    SQLColumn(s"${SQLColumn.quoteIdentifier(alias)}.${SQLColumn.quoteIdentifier(columnName)}", name = Some(columnName))

  // caching is left to the database
  override def cache: SQLDataFrame = this
  override def uncache: SQLDataFrame = this

  override def explainString(options: Map[String, String]): String = toSql(options.get("dialect"), pretty = true)

  override def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean): (SQLDataFrame, DataFrameObservation) =
    (this, GenericCalculatedObservation(this, aggregateColumns: _*))

  override def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): SQLDataFrame = this

  // reading data needs the SQL statement to be executed on the database, which is not implemented yet.
  private def notSupported(operation: String): Nothing =
    throw new NotImplementedError(s"SQLDataFrame.$operation needs executing SQL on the database, which is not implemented yet (#866)")
  override def collect: Seq[GenericRow] = notSupported("collect")
  override def isEmpty: Boolean = notSupported("isEmpty")
  override def count: Long = notSupported("count")
  override def showString(options: Map[String, String]): String = notSupported("show")

  override def toString: String = s"SQLDataFrame($id, ${columns.mkString(", ")})"
}

object SQLDataFrame {
  /**
   * Create a SQLDataFrame for a DataFrame of the bridge. The DataFrame in Python is released when the SQLDataFrame is
   * garbage collected.
   */
  private[sql] def apply(info: DataFrameInfo, bridge: SqlGlotBridge): SQLDataFrame = {
    val df = new SQLDataFrame(info, bridge)
    bridge.registerForRelease(df, info.id)
    df
  }

  /**
   * Create a SQLDataFrame reading a database table.
   *
   * @param bridge    the SQLGlot bridge
   * @param tableName name of the table, optionally qualified with database and catalog, in the given dialect
   * @param schema    schema of the table
   * @param dialect   SQLGlot dialect of `tableName`
   */
  def table(bridge: SqlGlotBridge, tableName: String, schema: SQLSchema, dialect: Option[String] = None): SQLDataFrame =
    SQLDataFrame(bridge.callDataFrame("table", "name" -> tableName, "columns" -> schema.toBridge, "dialect" -> dialect), bridge)
}

case class SQLGroupedDataFrame(df: SQLDataFrame, groupColumns: Seq[SQLColumn]) extends GenericGroupedDataFrame {
  override def subFeedType: Type = typeOf[SQLSubFeed]
  override def agg(columns: Seq[GenericColumn]): SQLDataFrame =
    SQLDataFrame(df.bridge.callDataFrame("group_by_agg", "df" -> df.id,
      "group_columns" -> groupColumns.map(_.projectionSql), "aggregate_columns" -> columns.map(SQLColumn.of(_).projectionSql)), df.bridge)
}

case class SQLRow(values: Seq[Any]) extends GenericRow {
  override def subFeedType: Type = typeOf[SQLSubFeed]
  override def get(index: Int): Any = values(index)
  override def getAs[T: ClassTag](index: Int): T = values(index).asInstanceOf[T]
  override def getStruct(index: Int): SQLRow = values(index) match {
    case row: SQLRow => row
    case x => throw new IllegalArgumentException(s"Value at index $index is not a struct: $x")
  }
  override def toSeq: Seq[Any] = values
}
