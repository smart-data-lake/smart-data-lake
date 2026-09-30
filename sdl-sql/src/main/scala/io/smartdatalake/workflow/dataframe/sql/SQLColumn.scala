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

import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataType, GenericWhen, SqlExpressionColumn}

import java.time.{Instant, LocalDate, LocalDateTime}
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Common implementation of the column operators of the SQL engine.
 * All operators create a new [[SQLColumn]] by composing SQL text, no call to Python is needed.
 */
sealed trait SQLExpression extends GenericColumn {

  /**
   * The expression as [[SQLColumn]]
   */
  def column: SQLColumn

  override def subFeedType: Type = typeOf[SQLSubFeed]

  private def binary(op: String, other: GenericColumn): SQLColumn =
    SQLColumn(s"${column.operand} $op ${SQLColumn.of(other).operand}", atomic = false)

  override def ===(other: GenericColumn): SQLColumn = binary("=", other)
  override def =!=(other: GenericColumn): SQLColumn = binary("<>", other)
  override def <=>(other: GenericColumn): SQLColumn = binary("IS NOT DISTINCT FROM", other)
  override def >(other: GenericColumn): SQLColumn = binary(">", other)
  override def <(other: GenericColumn): SQLColumn = binary("<", other)
  override def >=(other: GenericColumn): SQLColumn = binary(">=", other)
  override def <=(other: GenericColumn): SQLColumn = binary("<=", other)
  override def +(other: GenericColumn): SQLColumn = binary("+", other)
  override def -(other: GenericColumn): SQLColumn = binary("-", other)
  override def /(other: GenericColumn): SQLColumn = binary("/", other)
  override def *(other: GenericColumn): SQLColumn = binary("*", other)
  override def and(other: GenericColumn): SQLColumn = binary("AND", other)
  override def or(other: GenericColumn): SQLColumn = binary("OR", other)

  override def isin(list: Any*): SQLColumn =
    if (list.isEmpty) SQLColumn("FALSE")
    else SQLColumn(s"${column.operand} IN (${list.map(v => SQLColumn.literal(v).expr).mkString(", ")})", atomic = false)

  override def isNull: SQLColumn = SQLColumn(s"${column.operand} IS NULL", atomic = false)
  override def isNotNull: SQLColumn = SQLColumn(s"${column.operand} IS NOT NULL", atomic = false)

  override def as(name: String): SQLColumn = column.copy(alias = Some(name), sortOrder = None)

  // a cast keeps the name of the column, as in Spark
  override def cast(dataType: GenericDataType): SQLColumn =
    SQLColumn(s"CAST(${column.expr} AS ${SQLDataType.of(dataType).sparkSql})", name = column.name)

  override def exprSql: String = column.expr

  override def desc: SQLColumn = column.copy(sortOrder = Some("DESC"))

  override def apply(extraction: Any): SQLColumn = extraction match {
    case field: String => SQLColumn(s"${column.operand}.${SQLColumn.quoteIdentifier(field)}", name = Some(field))
    case index: Int => SQLColumn(s"${column.operand}[$index]")
    case x => throw new IllegalArgumentException(s"Unsupported extraction $x, only field names and array indexes are supported")
  }

  override def getName: Option[String] = column.alias.orElse(column.name)
}

/**
 * A column expression of the SQL engine, as Spark SQL text. It is parsed with the SQLGlot dialect databricks, which is
 * Spark SQL with ANSI casts. Like in Spark, identifiers are resolved case-insensitively, unless
 * `Environment.caseSensitive` is set.
 *
 * @param expr      the expression as SQL text, without alias
 * @param name      name of the column if it is a column reference or keeps the name of one (e.g. a cast)
 * @param alias     alias given with `as`
 * @param atomic    false if the expression needs parentheses to be used as operand, e.g. `a + b`
 * @param sortOrder sort order given with `desc`
 */
case class SQLColumn(expr: String, name: Option[String] = None, alias: Option[String] = None, atomic: Boolean = true,
                     sortOrder: Option[String] = None) extends SQLExpression {

  override def column: SQLColumn = this

  private[sql] def operand: String = if (atomic) expr else s"($expr)"

  /**
   * SQL to use this column in a select list. The column is aliased with its name if it is not a plain column
   * reference, so that the name is kept as in Spark.
   */
  private[sql] def projectionSql: String = alias.orElse(name.filter(_ => !isReference && expr != "*")) match {
    case Some(a) => s"$expr AS ${SQLColumn.quoteIdentifier(a)}"
    case None => expr
  }

  /**
   * SQL to use this column in an order by clause
   */
  private[sql] def orderSql: String = sortOrder.map(o => s"$expr $o").getOrElse(expr)

  private def isReference: Boolean = name.exists(n => expr == SQLColumn.quoteIdentifier(n) || expr.endsWith("." + SQLColumn.quoteIdentifier(n)))

  override def toString: String = projectionSql
}

/**
 * A CASE WHEN expression, which can be extended with further `when` branches.
 */
case class SQLWhenColumn(branches: Seq[(SQLColumn, SQLColumn)], otherwiseValue: Option[SQLColumn] = None) extends SQLExpression with GenericWhen {

  override def column: SQLColumn = {
    val whens = branches.map { case (condition, value) => s"WHEN ${condition.expr} THEN ${value.expr}" }
    SQLColumn(s"CASE ${whens.mkString(" ")}${otherwiseValue.map(v => s" ELSE ${v.expr}").getOrElse("")} END")
  }

  override def when(condition: GenericColumn, value: GenericColumn): SQLWhenColumn =
    copy(branches = branches :+ (SQLColumn.of(condition), SQLColumn.of(value)))

  override def otherwise(value: GenericColumn): SQLColumn = copy(otherwiseValue = Some(SQLColumn.of(value))).column
}

object SQLColumn {

  def of(column: GenericColumn): SQLColumn = column match {
    case c: SQLExpression => c.column
    case SqlExpressionColumn(sql) => SQLColumn(sql, atomic = false)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
  }

  /**
   * Quote an identifier for Spark SQL with backticks, if it is not a simple identifier.
   * Quoting does not make an identifier case sensitive in Spark SQL.
   */
  def quoteIdentifier(name: String): String =
    if (isSimpleIdentifier(name)) name else "`" + name.replace("`", "``") + "`"

  private def isSimpleIdentifier(name: String): Boolean =
    name.matches("[A-Za-z_][A-Za-z0-9_]*") && !reservedWords.contains(name.toUpperCase)

  // reserved words of Spark SQL, which can not be used as unquoted identifiers
  private val reservedWords = Set("ALL", "AND", "ANY", "AS", "AUTHORIZATION", "BOTH", "CASE", "CAST", "CHECK", "COLLATE",
    "COLUMN", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
    "DISTINCT", "ELSE", "END", "ESCAPE", "EXCEPT", "FALSE", "FETCH", "FILTER", "FOR", "FOREIGN", "FROM", "FULL", "GRANT",
    "GROUP", "HAVING", "IN", "INNER", "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEADING", "LEFT", "LIKE",
    "LIMIT", "NATURAL", "NOT", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PRIMARY", "REFERENCES",
    "RIGHT", "SELECT", "SESSION_USER", "SOME", "TABLE", "THEN", "TIME", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE",
    "UNKNOWN", "USER", "USING", "WHEN", "WHERE", "WINDOW", "WITH")

  /**
   * Create a reference to the column with exactly the given name, without interpreting dots or backticks.
   */
  def byName(colName: String): SQLColumn = SQLColumn(quoteIdentifier(colName), name = Some(colName))

  /**
   * Create a column reference. Like in Spark, the name can be qualified with a dot (`table.column` or
   * `struct.field`), and parts can be quoted with backticks (`` `my.column` ``).
   */
  def reference(colName: String): SQLColumn = {
    val parts = splitName(colName)
    if (parts.isEmpty) throw new IllegalArgumentException(s"Invalid column name '$colName'")
    val sql = parts.map(p => if (p == "*") p else quoteIdentifier(p)).mkString(".")
    SQLColumn(sql, name = Some(parts.last).filter(_ != "*"))
  }

  private def splitName(colName: String): Seq[String] = {
    if (colName == "*") return Seq("*")
    val parts = Seq.newBuilder[String]
    val current = new StringBuilder
    var inQuotes = false
    var i = 0
    while (i < colName.length) {
      colName(i) match {
        case '`' if inQuotes && i + 1 < colName.length && colName(i + 1) == '`' => current.append('`'); i += 1
        case '`' => inQuotes = !inQuotes
        case '.' if !inQuotes => parts += current.toString; current.clear()
        case c => current.append(c)
      }
      i += 1
    }
    parts += current.toString
    parts.result()
  }

  /**
   * Create a literal from a Scala value
   */
  def literal(value: Any): SQLColumn = value match {
    case null | None => SQLColumn("NULL")
    case Some(v) => literal(v)
    case c: GenericColumn => of(c)
    case s: String => SQLColumn("'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'")
    case b: Boolean => SQLColumn(if (b) "TRUE" else "FALSE")
    case d: Double if d.isNaN || d.isInfinite => SQLColumn(s"CAST('${if (d.isNaN) "NaN" else if (d > 0) "Infinity" else "-Infinity"}' AS DOUBLE)")
    case f: Float if f.isNaN || f.isInfinite => literal(f.toDouble)
    case n @ (_: Int | _: Long | _: Short | _: Byte | _: Double | _: Float | _: BigInt | _: java.math.BigInteger) => number(n.toString)
    case d: BigDecimal => number(d.bigDecimal.toPlainString)
    case d: java.math.BigDecimal => number(d.toPlainString)
    case d: java.sql.Date => SQLColumn(s"CAST('$d' AS DATE)")
    case d: LocalDate => SQLColumn(s"CAST('$d' AS DATE)")
    case t: java.sql.Timestamp => SQLColumn(s"CAST('$t' AS TIMESTAMP_NTZ)")
    case t: LocalDateTime => SQLColumn(s"CAST('${t.toString.replace('T', ' ')}' AS TIMESTAMP_NTZ)")
    case t: Instant => SQLColumn(s"CAST('$t' AS TIMESTAMPTZ)")
    case x => throw new IllegalArgumentException(s"Unsupported literal value of type ${x.getClass.getName}: $x")
  }

  private def number(str: String): SQLColumn = SQLColumn(str, atomic = !str.startsWith("-"))
}
