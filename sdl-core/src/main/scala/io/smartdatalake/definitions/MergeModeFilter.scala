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
package io.smartdatalake.definitions

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanHandlePartitions, TableDataObject}
import io.smartdatalake.workflow.dataobject.{DataObject, JdbcTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import java.sql.{Date, Timestamp}
import java.time.{Duration, Instant, LocalDate, LocalDateTime}
import scala.reflect.runtime.universe.typeOf

/**
 * Restricts the existing data of the output table considered by a merge to the data of interest, e.g. the partitions
 * written. A predicate on the filter columns is added to the join condition of the merge statement, which allows the
 * table format or database to skip partitions/files or use indexes, instead of reading the whole table.
 *
 * The predicate is created from the partition values of the incoming data if available (e.g. with PartitionDiffMode).
 * Otherwise the range [min, max] of the filter columns in the incoming data is computed in a separate evaluation of the
 * incoming data. The incoming data must therefore be deterministic, e.g. not depend on a source changing in between or on
 * non-deterministic functions, otherwise records outside the range might be merged and inserted again. Caching the
 * incoming data is not an option, as it interferes with the observation of metrics. If the incoming data is empty, no
 * predicate is added. Ranges are widened where values can not be compared exactly (timestamps by two days to be
 * independent of time zones, floating point numbers by one ulp). Range predicates are not created for string columns
 * of JDBC tables or with a non-binary collation, as the target might compare strings with a different collation.
 *
 * WARNING: the result is only correct if records never change their values in the filter columns. If a record moves to
 * another partition or value range, it is not found by the merge and inserted again, creating a duplicate primary key.
 * It is the responsibility of the user to ensure this, e.g. by choosing filter columns which are part of the primary key.
 *
 * Example to use the partition columns of the output table: `mergeModeFilter {}`
 *
 * Example for a liquid clustered DeltaLake table: `mergeModeFilter { columns = [event_date] }`
 *
 * @param columns Columns of the output table to create the predicate for. Default is to use the partition columns of
 *                the output DataObject. Configure columns explicitly if the output is not partitioned, e.g. the
 *                clustering columns of a liquid clustered DeltaLake table.
 */
case class MergeModeFilter(columns: Seq[String] = Seq()) extends SmartDataLakeLogger {

  /**
   * Get the filter columns, falling back to the partition columns of the output DataObject.
   */
  def getFilterColumns(actionId: ActionId, output: DataObject): Seq[String] = {
    if (columns.nonEmpty) columns
    else output match {
      case p: CanHandlePartitions if p.partitions.nonEmpty => p.partitions
      case _ => throw ConfigurationException(
          s"($actionId) mergeModeFilter needs columns to be configured, as output ${output.id} has no partition columns",
          Some(s"actions.${actionId.id}.mergeModeFilter.columns")
        )
    }
  }

  /**
   * Write the DataFrame with the filter predicate added to the given merge options.
   *
   * @param usePartitionValues if false, partition values are ignored and the range is always computed from the data.
   * @param write function writing the given DataFrame with the given merge options.
   */
  def writeWithPredicate[T](actionId: ActionId, output: DataObject, df: GenericDataFrame, partitionValues: Seq[PartitionValues],
                            saveModeOptions: SaveModeMergeOptions, usePartitionValues: Boolean)
                           (write: (GenericDataFrame, SaveModeMergeOptions) => T)
                           (implicit context: ActionPipelineContext): T = {
    val outputExisting = output match {
      case t: TableDataObject => t.isTableExisting
      case _ => true
    }
    if (!outputExisting) return write(df, saveModeOptions) // nothing to restrict on first load
    val filterColumns = getFilterColumns(actionId, output)
    val dialect = MergeModeFilter.getDialect(output, df)
    val exactValues = if (usePartitionValues) getExactPartitionValues(filterColumns, df, partitionValues) else Seq()
    val collatedColumns = MergeModeFilter.getCollatedColumns(output, df, filterColumns)
    val finalOptions = createPredicate(actionId, filterColumns, df, exactValues, dialect, collatedColumns) match {
      case Some(predicate) =>
        logger.info(s"($actionId) mergeModeFilter restricts existing data of ${output.id} to: $predicate")
        val combinedPredicate = (saveModeOptions.additionalMergePredicate.toSeq :+ predicate).map(p => s"($p)").mkString(" AND ")
        saveModeOptions.copy(additionalMergePredicate = Some(combinedPredicate))
      case None => saveModeOptions
    }
    write(df, finalOptions)
  }

  /**
   * Select partition values usable for an exact predicate.
   * Partition values are reduced to filter columns whose values can be rendered with the type of the column; other
   * columns get a range predicate. If an entry does not restrict any filter column, it selects all values, and no
   * partition values can be used.
   */
  private[smartdatalake] def getExactPartitionValues(filterColumns: Seq[String], df: GenericDataFrame, partitionValues: Seq[PartitionValues]): Seq[PartitionValues] = {
    val colTypes = df.schema.fields.map(f => f.name.toLowerCase -> f.dataType.typeName.toLowerCase).toMap
    def valuesOf(c: String) = partitionValues.flatMap(_.elements.get(c)).filter(_ != null)
    def isExactCapable(c: String): Boolean = colTypes.get(c.toLowerCase).exists {
      case tpe if MergeModeFilter.isStringType(tpe) => !MergeModeFilter.isCollated(tpe)
      case tpe if MergeModeFilter.isIntegralType(tpe) => valuesOf(c).forall {
        case _: Number => true
        case v => v.toString.toLongOption.isDefined
      }
      case "date" => valuesOf(c).forall {
        case _: Date | _: LocalDate => true
        case v => scala.util.Try(LocalDate.parse(v.toString)).isSuccess
      }
      case _ => false
    }
    val exactColumns = filterColumns.filter(c => partitionValues.exists(_.isDefinedAt(c)) && isExactCapable(c))
    val reduced = partitionValues.map(_.filterKeys(exactColumns))
    if (reduced.isEmpty || reduced.exists(_.elements.isEmpty)) Seq() else reduced.distinct
  }

  /**
   * Create a predicate on the existing data using table alias 'existing'.
   * Filter columns covered by the given exact partition values get an exact predicate, the remaining columns a range predicate.
   */
  private[smartdatalake] def createPredicate(actionId: ActionId, filterColumns: Seq[String], df: GenericDataFrame, exactValues: Seq[PartitionValues],
                                             dialect: MergeFilterDialect, collatedColumns: Set[String] = Set()): Option[String] = {
    def colRef(c: String) = s"existing.${dialect.quote(c)}"
    val colTypes = df.schema.fields.map(f => f.name.toLowerCase -> f.dataType.typeName.toLowerCase).toMap

    // exact predicate from partition values, rendered with the type of the column
    def exactLiteral(c: String, v: Any): String = {
      val typedValue = v match {
        case s: String if MergeModeFilter.isIntegralType(colTypes.getOrElse(c.toLowerCase, "")) => s.toLong
        case s: String if colTypes.get(c.toLowerCase).contains("date") => Date.valueOf(LocalDate.parse(s))
        case x if MergeModeFilter.isStringType(colTypes.getOrElse(c.toLowerCase, "")) => x.toString
        case x => x
      }
      dialect.literal(typedValue).getOrElse(throw new IllegalStateException(s"($actionId) Unsupported partition value $v for column $c"))
    }
    val exactPredicate = if (exactValues.nonEmpty) {
      val predicates = exactValues.map(_.elements.toSeq.sortBy(_._1).map {
        case (k, null) => s"${colRef(k)} IS NULL"
        case (k, v) => s"${colRef(k)} = ${exactLiteral(k, v)}"
      }.mkString(" AND "))
      Some(if (predicates.size > 1) predicates.map(p => s"($p)").mkString("(", " OR ", ")") else predicates.head)
    } else None

    // range predicate for remaining columns
    val coveredColumns = exactValues.flatMap(_.keys).toSet
    val rangeColumns = filterColumns.filterNot(coveredColumns.contains)
      .filter { c =>
        val tpe = colTypes.getOrElse(c.toLowerCase, "")
        val isStringRangeUnsupported = MergeModeFilter.isStringType(tpe) &&
          (!dialect.supportsStringRange || MergeModeFilter.isCollated(tpe) || collatedColumns.contains(c.toLowerCase))
        if (isStringRangeUnsupported) logger.warn(s"($actionId) mergeModeFilter creates no range predicate for string column $c, as the target might compare strings with a different collation")
        !isStringRangeUnsupported
      }
    val rangePredicates = if (rangeColumns.nonEmpty) {
      val missingColumns = rangeColumns.filterNot(c => colTypes.isDefinedAt(c.toLowerCase))
      if (missingColumns.nonEmpty) throw ConfigurationException(
        s"($actionId) mergeModeFilter columns ${missingColumns.mkString(", ")} not found in DataFrame with columns ${df.columns.mkString(", ")}",
        Some(s"actions.${actionId.id}.mergeModeFilter.columns")
      )
      MergeModeFilter.getColumnRanges(df, rangeColumns) match {
        case None => return None // incoming data is empty, don't restrict
        case Some(ranges) => ranges.flatMap {
          case (c, None, None, _) => Some(s"${colRef(c)} IS NULL") // only null values
          case (c, Some(minValue), Some(maxValue), hasNulls) =>
            // a bound is None if it is unbounded after widening, e.g. near the maximum timestamp
            val (lower, upper) = (MergeModeFilter.widenLower(minValue), MergeModeFilter.widenUpper(maxValue))
            val (minLit, maxLit) = (lower.map(dialect.literal), upper.map(dialect.literal))
            if (minLit.exists(_.isEmpty) || maxLit.exists(_.isEmpty)) {
              logger.warn(s"($actionId) mergeModeFilter cannot create predicate for column $c with values of type ${minValue.getClass.getSimpleName}, column is ignored")
              None
            } else {
              val rangeOpt = (minLit.flatten, maxLit.flatten) match {
                case (Some(l), Some(u)) => Some(s"${colRef(c)} BETWEEN $l AND $u")
                case (Some(l), None) => Some(s"${colRef(c)} >= $l")
                case (None, Some(u)) => Some(s"${colRef(c)} <= $u")
                case (None, None) => None
              }
              rangeOpt.map(range => if (hasNulls) s"(${colRef(c)} IS NULL OR $range)" else range)
            }
          case (c, minValue, maxValue, _) => throw new IllegalStateException(s"($actionId) unexpected range for column $c: min=$minValue max=$maxValue")
        }
      }
    } else Seq()

    val predicates = exactPredicate.toSeq ++ rangePredicates
    if (predicates.nonEmpty) Some(predicates.mkString(" AND ")) else None
  }
}

/**
 * SQL dialect used to render the predicate of [[MergeModeFilter]].
 */
private[smartdatalake] trait MergeFilterDialect {
  def quote(column: String): String
  def literal(value: Any): Option[String]
  def supportsStringRange: Boolean = true
}

object MergeModeFilter {

  // time zone offsets differ by up to 26 hours
  private val timestampWidening = Duration.ofDays(2)
  private val minTimestamp = Timestamp.valueOf("0001-01-01 00:00:00")
  private val maxTimestamp = Timestamp.valueOf("9999-12-31 23:59:59.999999")

  private[smartdatalake] def isCollated(typeName: String): Boolean =
    typeName.contains("collate") && !typeName.contains("utf8_binary")

  /**
   * Filter columns which have a non-binary collation in the output table, e.g. UTF8_LCASE on Spark 4.
   */
  private[smartdatalake] def getCollatedColumns(output: DataObject, df: GenericDataFrame, filterColumns: Seq[String])(implicit context: ActionPipelineContext): Set[String] = output match {
    case o: CanCreateDataFrame =>
      scala.util.Try(o.getDataFrame(Seq(), df.subFeedType).schema.fields)
        .map(_.filter(f => filterColumns.exists(_.equalsIgnoreCase(f.name)) && isCollated(f.dataType.typeName.toLowerCase)).map(_.name.toLowerCase).toSet)
        .getOrElse(Set())
    case _ => Set()
  }

  private[smartdatalake] def isStringType(typeName: String): Boolean =
    Seq("string", "varchar", "char", "text", "nvarchar", "nchar").exists(typeName.startsWith)

  private[smartdatalake] def isIntegralType(typeName: String): Boolean =
    Set("byte", "tinyint", "short", "smallint", "int", "integer", "long", "bigint").contains(typeName)

  private[smartdatalake] def getDialect(output: DataObject, df: GenericDataFrame)(implicit context: ActionPipelineContext): MergeFilterDialect = output match {
    case jdbc: JdbcTableDataObject if !jdbc.isMergeConditionSparkSql(df.subFeedType) => new MergeFilterDialect {
      override def quote(column: String): String = jdbc.quoteCaseSensitiveColumn(column)
      override def literal(value: Any): Option[String] = jdbc.connection.catalog.sqlLiteral(normalize(value))
      override def supportsStringRange: Boolean = false
    }
    case _: JdbcTableDataObject => new MergeFilterDialect {
      // the engine translates the predicate from Spark SQL to the dialect of the database
      override def quote(column: String): String = SQLUtil.sparkQuoteCaseSensitiveColumn(column)
      override def literal(value: Any): Option[String] = renderLiteral(value, isSparkSql = true)
      // the database compares strings with its own collation
      override def supportsStringRange: Boolean = false
    }
    case _ if df.subFeedType =:= typeOf[ScalaSubFeed] => new MergeFilterDialect {
      // the expression parser of the plain-Scala engine supports standard SQL string literals, but no quoted identifiers
      override def quote(column: String): String = column
      override def literal(value: Any): Option[String] = renderLiteral(value, isSparkSql = false)
    }
    case _ => new MergeFilterDialect {
      override def quote(column: String): String = SQLUtil.sparkQuoteCaseSensitiveColumn(column)
      override def literal(value: Any): Option[String] = renderLiteral(value, isSparkSql = true)
    }
  }

  /**
   * Compute (column, min, max, hasNulls) for the given columns. Min and max are None if the column only contains null values.
   * Returns None if the DataFrame is empty.
   */
  private[smartdatalake] def getColumnRanges(df: GenericDataFrame, columns: Seq[String]): Option[Seq[(String, Option[Any], Option[Any], Boolean)]] = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    val aggCols = count(col("*")) +: columns.flatMap(c => Seq(count(col(c)), min(col(c)), max(col(c))))
    val row = df.agg(aggCols).collect.head
    val totalCount = row.get(0).asInstanceOf[Number].longValue
    if (totalCount == 0) return None
    Some(columns.zipWithIndex.map { case (c, idx) =>
      val nonNullCount = row.get(1 + idx * 3).asInstanceOf[Number].longValue
      val (minValue, maxValue) = if (nonNullCount == 0) (None, None)
      else {
        val (minAgg, maxAgg) = (Option(row.get(2 + idx * 3)), Option(row.get(3 + idx * 3)))
        // not all engines ignore null values in min/max aggregations, recompute on non-null values if needed
        if (minAgg.isEmpty || maxAgg.isEmpty) {
          val nonNullRow = df.filter(not(col(c).isNull)).agg(Seq(min(col(c)), max(col(c)))).collect.head
          (Option(nonNullRow.get(0)), Option(nonNullRow.get(1)))
        } else (minAgg, maxAgg)
      }
      (c, minValue, maxValue, nonNullCount < totalCount)
    })
  }

  /**
   * Normalize java.time values to their java.sql counterparts.
   */
  private[smartdatalake] def normalize(value: Any): Any = value match {
    case d: LocalDate => Date.valueOf(d)
    case t: LocalDateTime => Timestamp.valueOf(t)
    case i: Instant => Timestamp.from(i)
    case x => x
  }

  /**
   * Widen the lower bound of a range for values which can not be compared exactly.
   * Timestamps are rendered in the JVM time zone, but parsed in the session time zone of Spark or the database.
   * Widening by two days covers any time zone offset. A bound is None if it would exceed the timestamp range. Floating point numbers are widened by one ulp.
   */
  private[smartdatalake] def widenLower(value: Any): Option[Any] = normalize(value) match {
    case t: Timestamp =>
      val widened = Timestamp.from(t.toInstant.minus(timestampWidening))
      if (widened.before(minTimestamp)) None else Some(widened)
    case f: Float => Some(Math.nextDown(f.toDouble))
    case d: Double => Some(Math.nextDown(d))
    case x => Some(x)
  }

  private[smartdatalake] def widenUpper(value: Any): Option[Any] = normalize(value) match {
    case t: Timestamp =>
      val widened = Timestamp.from(t.toInstant.plus(timestampWidening))
      if (widened.after(maxTimestamp)) None else Some(widened)
    case f: Float => Some(Math.nextUp(f.toDouble))
    case d: Double => Some(Math.nextUp(d))
    case x => Some(x)
  }

  /**
   * Render a value as SQL literal. String escaping differs between Spark SQL (backslash escapes) and standard SQL
   * (doubled single quotes).
   * Returns None for unsupported value types.
   */
  private[smartdatalake] def renderLiteral(value: Any, isSparkSql: Boolean): Option[String] = normalize(value) match {
    case s: String =>
      val escaped = if (isSparkSql) s.replace("\\", "\\\\").replace("'", "\\'") else SQLUtil.escapeSqlStringLiteral(s)
      Some(s"'$escaped'")
    case b: Boolean => Some(if (b) "TRUE" else "FALSE")
    case d: Date => Some(s"DATE '$d'")
    case t: Timestamp => Some(s"TIMESTAMP '$t'")
    case x => SQLUtil.numberLiteral(x)
  }
}
