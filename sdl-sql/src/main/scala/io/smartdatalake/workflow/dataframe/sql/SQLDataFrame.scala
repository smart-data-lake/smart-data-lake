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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.sqlglot.{DataFrameInfo, SqlGlotBridge}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.connection.jdbc.JdbcConnectionImpl
import io.smartdatalake.workflow.dataframe._

import org.json4s.{DefaultFormats, Formats}

import java.sql.ResultSet
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * DataFrame of the SQL engine. It remote-controls an SQLGlot query in the embedded Python interpreter, and renders
 * it as SQL statement for a database with [[toSql]].
 *
 * Operations that need to read data (collect, count, isEmpty, show) execute the SQL statement on the database of
 * the [[JdbcConnection]]. All DataFrames combined, e.g. by a join, must belong to the same connection.
 */
class SQLDataFrame private(val info: DataFrameInfo, @transient val bridge: SqlGlotBridge, @transient val connection: JdbcConnectionImpl) extends GenericDataFrame {

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
    SQLDataFrame(bridge.callDataFrame(name, ("df" -> id) +: args: _*), bridge, connection)

  private def other(df: GenericDataFrame): SQLDataFrame = df match {
    case sqlDf: SQLDataFrame =>
      if (sqlDf.connection.id != connection.id) throw new IllegalArgumentException(
        s"DataFrames of different connections can not be combined, ${connection.id} and ${sqlDf.connection.id}")
      sqlDf
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

  /**
   * Drop a column. Like in Spark, a column qualified with the alias of an input of a join, e.g. `col("existing.a")`,
   * only drops the column of this input.
   */
  override def drop(col: GenericColumn): SQLDataFrame = op("drop", "columns" -> Seq(SQLColumn.of(col).expr))

  override def createOrReplaceTempView(viewName: String): Unit = bridge.registerView(viewName, id)

  override def dropDuplicates(cols: Seq[String]): SQLDataFrame = op("drop_duplicates", "columns" -> cols)

  override def as(alias: String): SQLDataFrame = op("alias", "alias" -> alias)

  override def apply(columnName: String): SQLColumn =
    SQLColumn(s"${SQLColumn.quoteIdentifier(alias)}.${SQLColumn.quoteIdentifier(columnName)}", name = Some(columnName))

  // caching is left to the database
  override def cache: SQLDataFrame = this
  override def uncache: SQLDataFrame = this

  /**
   * Column level lineage, extracted with the lineage module of SQLGlot, see `column_lineage` in sdlb_sql/bridge.py.
   * Like for the other engines only DIRECT lineage is reported.
   */
  override def getColumnLineage(inputs: Seq[(DataObjectId, GenericDataFrame)]): Option[ColumnLineage] = {
    implicit val formats: Formats = DefaultFormats
    val sqlInputs = inputs.collect { case (dataObjectId, df: SQLDataFrame) => Seq(dataObjectId.id, df.id) }
    val result = bridge.call("column_lineage", "df" -> id, "inputs" -> sqlInputs)
    val fields = (result \ "fields").children.map { field =>
      val description = (field \ "description").extractOpt[String]
      val inputFields = (field \ "inputs").children.map { input =>
        val Seq(dataObjectId, column, isIdentity) = input.children
        ColumnLineageInputField(DataObjectId(dataObjectId.extract[String]), column.extract[String],
          ColumnTransformation.direct(isIdentity.extract[Boolean], description))
      }
      ColumnLineageField((field \ "column").extract[String], inputFields, (field \ "expression").extractOpt[String])
    }
    val unresolved = (result \ "unresolved").extract[Seq[String]]
    val debugInfo = if (unresolved.nonEmpty && Environment.columnLineageDebug) Some(ColumnLineageDebug(
      engine = "SQL",
      inputs = (result \ "inputs").children.map(i => ColumnLineageDebugInput(DataObjectId((i \ "dataObjectId").extract[String]),
        (i \ "columns").extract[Seq[String]], (i \ "columnsNotInPlan").extract[Seq[String]])),
      unresolvedColumns = unresolved.map(column => ColumnLineageDebugColumn(column, (result \ "dead_ends" \ column).children.map(d =>
        ColumnLineageDebugDeadEnd((d \ "attribute").extract[String], (d \ "path").extract[Seq[String]],
          (d \ "producedBy").extractOpt[String], (d \ "producedByNode").extractOpt[String])
      ))),
      plan = (result \ "plan").extract[Seq[String]]
    )) else None
    Some(ColumnLineage(fields, unresolved, debugInfo))
  }

  override def explainString(options: Map[String, String]): String = toSql(options.get("dialect"), pretty = true)

  /**
   * The metrics are calculated with a separate query. In exec phase this is done immediately, i.e. before the DataFrame
   * is written: calculated afterwards, the query might give a different result if it reads its output DataObject,
   * e.g. the existing history of HistorizeAction.
   */
  override def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean): (SQLDataFrame, DataFrameObservation) = {
    val observation = GenericCalculatedObservation(this, aggregateColumns: _*)
    if (isExecPhase && aggregateColumns.nonEmpty) (this, SQLCalculatedObservation(observation.waitFor()))
    else (this, observation)
  }

  override def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): SQLDataFrame = this

  /**
   * The SQL statement of this DataFrame in the dialect of the database
   */
  def toDatabaseSql: String = toSql(Some(connection.sqlGlotDialect))

  // reading data executes the SQL statement on the database

  override def collect: Seq[SQLRow] = connection.execJdbcQuery(toDatabaseSql, SQLDataFrame.readRows)

  override def count: Long = agg(Seq(SQLSubFeed.count(SQLSubFeed.col("*")).as("count"))).collect.head.get(0) match {
    case n: Number => n.longValue
    case x => throw new IllegalStateException(s"Unexpected result of count: $x")
  }

  override def isEmpty: Boolean = limit(1).collect.isEmpty

  override def showString(options: Map[String, String]): String = {
    val numRows = options.get("numRows").map(_.toInt).getOrElse(20)
    val rows = limit(numRows).collect.map(_.values.map(v => String.valueOf(v)))
    val widths = columns.indices.map(i => (columns(i) +: rows.map(_(i))).map(_.length).max)
    def line(values: Seq[String]) = values.zip(widths).map { case (v, w) => v.padTo(w, ' ') }.mkString("|", "|", "|")
    val separator = widths.map("-" * _).mkString("+", "+", "+")
    (Seq(separator, line(columns), separator) ++ rows.map(line) :+ separator).mkString(System.lineSeparator())
  }

  override def toString: String = s"SQLDataFrame($id, ${columns.mkString(", ")})"
}

object SQLDataFrame {
  /**
   * Create a SQLDataFrame for a DataFrame of the bridge. The DataFrame in Python is released when the SQLDataFrame is
   * garbage collected.
   */
  private[sql] def apply(info: DataFrameInfo, bridge: SqlGlotBridge, connection: JdbcConnectionImpl): SQLDataFrame = {
    val df = new SQLDataFrame(info, bridge, connection)
    bridge.registerForRelease(df, info.id)
    df
  }

  /**
   * Create a SQLDataFrame reading a database table.
   *
   * @param connection connection to the database
   * @param tableName  name of the table, optionally qualified with database and catalog, in the dialect of the database
   * @param schema     schema of the table
   */
  def table(connection: JdbcConnectionImpl, tableName: String, schema: SQLSchema): SQLDataFrame = {
    val bridge = SqlGlotBridge.get()
    SQLDataFrame(bridge.callDataFrame("table", "name" -> tableName, "columns" -> schema.toBridge, "dialect" -> connection.sqlGlotDialect), bridge, connection)
  }

  /**
   * Create a SQLDataFrame reading the result of a query.
   *
   * @param connection connection to the database
   * @param query      the query in the dialect of the database
   * @param schema     schema of the result of the query
   */
  def query(connection: JdbcConnectionImpl, query: String, schema: SQLSchema): SQLDataFrame = {
    val bridge = SqlGlotBridge.get()
    SQLDataFrame(bridge.callDataFrame("query", "query" -> query, "columns" -> schema.toBridge, "dialect" -> connection.sqlGlotDialect), bridge, connection)
  }

  def of(df: GenericDataFrame): SQLDataFrame = df match {
    case sqlDf: SQLDataFrame => sqlDf
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
  }

  private[sql] def readRows(rs: ResultSet): Seq[SQLRow] = {
    val numColumns = rs.getMetaData.getColumnCount
    val rows = Seq.newBuilder[SQLRow]
    while (rs.next()) rows += SQLRow((1 to numColumns).map(i => fromJdbcValue(rs.getObject(i))))
    rows.result()
  }

  /**
   * Convert JDBC values of complex types to Scala: arrays to Seq, structs to SQLRow and maps to Map
   */
  private[sql] def fromJdbcValue(value: Any): Any = value match {
    case array: java.sql.Array => array.getArray match {
      case values: Array[_] => values.toSeq.map(fromJdbcValue)
      case x => x
    }
    case struct: java.sql.Struct => SQLRow(struct.getAttributes.toSeq.map(fromJdbcValue))
    case map: java.util.Map[_, _] => map.asScala.map { case (k, v) => (fromJdbcValue(k), fromJdbcValue(v)) }.toMap
    case x => x
  }
}

/**
 * Observation with metrics calculated already, see [[SQLDataFrame.setupObservation]]
 */
case class SQLCalculatedObservation(metrics: Map[String, _]) extends DataFrameObservation {
  override def waitFor(timeoutSec: Int): Map[String, _] = metrics
}

case class SQLGroupedDataFrame(df: SQLDataFrame, groupColumns: Seq[SQLColumn]) extends GenericGroupedDataFrame {
  override def subFeedType: Type = typeOf[SQLSubFeed]
  override def agg(columns: Seq[GenericColumn]): SQLDataFrame =
    SQLDataFrame(df.bridge.callDataFrame("group_by_agg", "df" -> df.id,
      "group_columns" -> groupColumns.map(_.projectionSql), "aggregate_columns" -> columns.map(SQLColumn.of(_).projectionSql)), df.bridge, df.connection)
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
