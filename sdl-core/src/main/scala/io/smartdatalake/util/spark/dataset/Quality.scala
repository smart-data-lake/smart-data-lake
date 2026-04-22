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
package io.smartdatalake.util.spark.dataset

import io.smartdatalake.util.LogUtils.{debLogFun, debugLog}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}

trait Quality extends Transform {

  final def comment(commentString: String): Metadata = new MetadataBuilder()
    .putString("comment", commentString).build()

  final def withComment(colName: String, column: Column, commentText: String): Column = column
    .as(alias = colName, metadata = comment(commentText))

  final def withComment(colName: String, commentText: String): Column = withComment(colName.split('.').last, col(colName), commentText)

  final def withComment(column: Column, commentText: String): Column = {
    val colName = column.node match {
      case a: Alias => a.name
      case _ => throw new IllegalArgumentException(s"Cannot extract name from Column $column, as it has no direct alias. Use withComment(colName: String, column: Column, commentText: String) instead.")
    }
    withComment(colName, commentText)
  }

  /**
   *
   * @param c     numeric column
   * @param alias suffix of resulting column names
   * @return List of aggregated columns: min, avg, max, sum
   */
  final def getStatsCol(c: Column, alias: String): List[Column] = List(
    countDistinct(c).as(s"cnt_$alias"),
    sum(when(c.isNotNull, 1)).as(s"cnt_notnull_$alias"),
    min(c).as(s"min_$alias"),
    avg(c).as(s"avg_$alias"),
    max(c).as(s"max_$alias"),
    sum(c).as(s"sum_$alias")
  )

  /**
   *
   * @param cn name of numeric column
   * @return List of aggregated columns: min, avg, max, sum
   */
  final def getStatsCol(cn: String): List[Column] = getStatsCol(col(cn), cn)


  implicit class DsComment[T](ds: Dataset[T]) {

    def getColumnComments(implicit implSs: SparkSession): Dataset[(String, String, String)] = {
      import implSs.implicits._
      ds.columns.map { cn => (cn, ds.schema(cn).dataType.catalogString, ds.schema(cn).getComment().getOrElse("")) }
        .toList.toDF("column", "datatype", "comment").as[(String, String, String)]
    }

    def setColumnComments(commentMap: Map[String, String])(implicit logger: Logger): Dataset[T] = {
      def commentField(fld: StructField): StructField = commentMap.get(fld.name).map(comment => fld.withComment(comment)).getOrElse(fld)

      val superfluousComments = commentMap.keys.toSeq.diff(ds.columns)
      if (superfluousComments.nonEmpty) logger.warn(s"Superfluous comment detected for columns ${superfluousComments.mkString(", ")}")
      val commentedCols = ds.schema.map(f => col(f.name).as(f.name, commentField(f).metadata))
      ds.select(commentedCols.toIndexedSeq: _*).as[T](ds.encoder)
    }

    /**
     * Add a column include a comment
     */
    def withColumn(colName: String, expr: Column, comment: String): DataFrame = {
      ds.withColumn(colName, withComment(colName, expr, comment))
    }

    /**
     * transformCommentCols allows to not only transform columns
     * but also to add comments to the obtained columns.
     *
     * If a DataFrame is persisted as table (e.g. hive, Databricks) the column
     * comments are saved in the corresponding MetaStore and thus visible
     * and Databrick Catalog and DB Tools like Dbeaver.
     *
     * @param transformRenameCommentFun : function to transform, rename and comment columns
     * @param colFilter                 : predicate on column names to filter
     * @param keepOriginalCols          : whether to keep original cols (make sure that there is no ambiguity!)
     * @return DataFrame with transformed, renamed and commented columns
     */
    def transformCommentCols(transformRenameCommentFun: String => Iterable[CommentedColumn],
                             colFilter: String => Boolean = _ => true,
                             keepOriginalCols: Boolean = false)
                            (implicit logger: Logger): DataFrame = {
      val commentMap = ds.columns.filter(colFilter)
        .flatMap(transformRenameCommentFun(_).map(_.nameComment))
        .toMap

      ds.transformCols((cn: String) => transformRenameCommentFun(cn).map(_.defName),
          colFilter, keepOriginalCols)
        .setColumnComments(commentMap)
    }


  }


  implicit class DsQuality[T](ds: Dataset[T]) {
    /**
     * Converts maps to arrays and then counts distinct rows.
     * If the dataset has a map column ds.distinct() throws an exception
     *
     * @return number of distinct rows
     */
    final def countDistinctRows: Long = ds.castMapsToArrays.distinct().count()

    def createdLog(dsName: String, debug: Option[Boolean] = None)(implicit logger: Logger): Unit = {
      logger.info(s"DataSet $dsName created :)")
      logger.info(s"$dsName.schema: ${ds.schema.catalogString}")
      if (debug.getOrElse(logger.isDebugEnabled)) {
        debugLog(s"$dsName: number of partitions = ${ds.rdd.getNumPartitions}") // ds.rdd.getNumPartitions may take a long time
        val cntRows = ds.count()
        val cntDistinctRows = Try(countDistinctRows) match {
          case Success(n) => n
          case Failure(e) => logger.warn(s"createdLog: could not count distinct rows of $dsName")
            logger.warn(e.getMessage)
            logger.warn("createdLog: ignoring this problem and returning -1")
            -1L
        }
        if (0 < cntDistinctRows && cntRows != cntDistinctRows) {
          logger.warn(s"DataSet $dsName has duplicates! Voilà some examples:")
          ds.getNonuniqueStats().show(8, truncate = false)
        }
        debLogFun(s"$dsName.count() = $cntRows") // may take a long time
        debLogFun(s"$dsName.distinct().count() = $cntDistinctRows") // may take even much longer time
        ds.show(4, truncate = false)
      }
    }

    /**
     * @return aggregated dataFrame showing count and count distinct of each column
     */
    final def getStats: DataFrame = {
      val statCols = ds.columns.flatMap { cn =>
        List(countDistinct(cn).as(s"cnt_$cn"), min(cn).as(s"min_$cn"), max(cn).as(s"max_$cn"))
      }
      ds.agg(count("*").as("cnt_rows"), statCols.toIndexedSeq: _*)
    }


    ///// treating gaps in axis (time or space) /////

    /**
     * Fills data gaps with either the next or the previous value.
     * If the parameter `takeNextValueFirst` is `true`, it will first look for the next value.
     *
     * WARNING: All columns in the result are nullable, even if they weren't before :(
     *
     * @param keyColNames        : Columns which, together with `orderCol`, form a primary key candidate
     * @param dataColNames       : Names of columns whose gaps are to be filled
     * @param orderColName       : Name of the ordering column, e.g., "valid_from"
     * @param takeNextValueFirst : Indicates whether to take the next value first
     * @return A data frame without NULLs in the `dataColNames` columns
     */
    final def fillGaps(keyColNames: Iterable[String], dataColNames: Iterable[String],
                       orderColName: String, takeNextValueFirst: Boolean = true): DataFrame = {
      val fenestra_next = Window.partitionBy(keyColNames.head, keyColNames.tail.toSeq.toIndexedSeq: _*).orderBy(orderColName).rangeBetween(Window.currentRow, Window.unboundedFollowing)
      val fenestra_prev = Window.partitionBy(keyColNames.head, keyColNames.tail.toSeq.toIndexedSeq: _*).orderBy(orderColName).rangeBetween(Window.unboundedPreceding, Window.currentRow)

      def newColumn(colName: String): Column = if (colName.contains(colName)) {
        if (takeNextValueFirst) coalesce(first(col(colName), ignoreNulls = true).over(fenestra_next), last(col(colName), ignoreNulls = true).over(fenestra_prev)).as(colName) else {
          coalesce(last(col(colName), ignoreNulls = true).over(fenestra_prev), first(col(colName), ignoreNulls = true).over(fenestra_next)).as(colName)
        }
      } else {
        col(colName)
      }
      // TODO: find a way to declare columns as not-nullable
      ds.select(ds.columns.map(newColumn).toIndexedSeq: _*)
    }

    /**
     * adds a column which indicates whether the next interval [fromColName , toColName[ is adjacent
     *
     * @param keyColNames      : Column names of the key
     * @param fromColName      : Column names of interval starts to be checked for gaps
     * @param toColName        : Column names of interval ends to be checked for gaps
     * @param orderColNames    : Sorting columns
     * @param gapIndicatorName : Name of the result column indicating gaps
     *
     *                         Notes:
     *                         - The `keyColNames` are used as part of the primary key.
     *                         - `fromColName` and `toColName` are examined for gaps in the data.
     *                         - `orderColNames` determines the order of data points (e.g., `valid_from` and `valid_to`).
     *                         - `gapIndicatorName` holds a value indicating whether a gap was detected in `fromColName` or `toColName`.
     *
     *                         Usage:
     *                         - This method fills gaps in data intervals defined by `fromColName` and `toColName`.
     *                         - The `orderColNames` (e.g., `valid_from` and `valid_to`) are critical for correctly ordering data points.
     *                         - The `gapIndicatorName` can be used to identify areas where data is missing.
     */
    final def getGaps(keyColNames: Iterable[String],
                      fromColName: String, toColName: String,
                      orderColNames: Iterable[String], gapIndicatorName: String = "_is_adjacent"): DataFrame = {
      val nextFromColName = s"_next_$fromColName"
      val fenestra = Window.partitionBy(keyColNames.map(col).toSeq.toIndexedSeq: _*).orderBy(orderColNames.map(col).toSeq.toIndexedSeq: _*)

      ds.withColumn("_islastrow", lead(col(keyColNames.head), 1).over(fenestra).isNull)
        .withColumn(nextFromColName, lead(col(fromColName), 1).over(fenestra))
        .withColumn(gapIndicatorName, coalesce(col(toColName) === col(nextFromColName), col("_islastrow"), lit(false)))
        .drop(nextFromColName, "_islastrow")
    }

  }


  implicit class DsPk[T](ds: Dataset[T]) {

    /**
     * returns sub data frame which consists of those rows which contain at least a null in the specified columns
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return sub data frame
     */
    def getNulls(cols: Array[String] = ds.columns): Dataset[T] = {
      val nullSearch: Column = ds.columns.map(col).foldLeft(lit(false))({ case (x, y) => x.or(y.isNull) })
      ds.where(nullSearch)
    }

    /**
     * Checks whether the specified columns contain nulls
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def containsNull(cols: Array[String] = ds.columns): Boolean = !getNulls(cols).isEmpty

    /**
     * counts n-lets of this data frame with respect to specified columns cols.
     * The result data frame possesses the columns cols and an additional count column countColname.
     *
     * @param cols         : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @param countColname : name of count column, default name: cnt
     * @return subdataframe of n-lets
     */
    def getNonuniqueStats(cols: Array[String] = ds.columns, countColname: String = "_cnt_"): DataFrame = {
      val forbiddenColumnNames = Array("count", countColname)
      // for better usability we define empty Array of cols to mean all columns of df
      val colsInDs: Array[String] = if (cols.isEmpty) cols else cols.intersect(cols)
      if (colsInDs.isEmpty) throw new IllegalArgumentException(s"Argument cols must contain at least 1 name" +
        s" of a column of your dataset.\n   cols = ${cols.mkString(",")}\n   cols = ${cols.mkString(",")} ")
      val dfProjected: DataFrame = ds.select(colsInDs.map(col).toIndexedSeq: _*)
      val dfColumns: Array[String] = dfProjected.columns
      // If df contains forbidden column then the result contains two columns with the same name
      forbiddenColumnNames.foreach(str =>
        require(!dfColumns.contains(str),
          s"data frame df must not contain column named $str. cols = ${dfColumns.mkString(",")}")
      )

      dfProjected.groupBy(dfColumns.head, dfColumns.tail.toIndexedSeq: _*)
        .count().withColumnRenamed("count", countColname)
        .where(col(countColname) > 1)
    }

    /**
     * returns nLets of this data frame with an additional count column
     * overloaded version because used often to check whether columnName forms a unique key
     *
     * @param columName : names of column which is to be considered
     * @return subdataframe of nLets with an additional count column
     */
    def getNonuniqueStats(columName: String): DataFrame = getNonuniqueStats(cols = Array(columName))

    /**
     * Returns rows of this data frame which violate uniqueness for specified columns cols.
     * The result data frame possesses an additional count column countColname.
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return subdataframe of n-lets
     */
    def getNonuniqueRows(cols: Array[String] = ds.columns): Dataset[T] = {
      val dfNonUnique = getNonuniqueStats(cols, "_duplicationCount_").drop("_duplicationCount_")
      ds.join(dfNonUnique, cols).select(ds.columns.head, ds.columns.tail.toIndexedSeq: _*).as[T](ds.encoder)
    }

    /**
     * projects a data frame onto array of columns
     *
     * @param cols : names of columns on which the data frame is to be projected
     * @return projection of data frame df
     */
    def project(cols: Array[String] = ds.columns): DataFrame = ds.select(cols.map(col).toIndexedSeq: _*)

    /**
     * Checks whether the specified columns satisfy uniqueness within the data frame
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def isUnique(cols: Array[String] = ds.columns): Boolean = project(cols).getNonuniqueStats(cols).isEmpty

    /**
     * returns sub data frame which consists of those rows which violate PK condition for specfied columns
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return sub data frame
     */
    def getPKviolators(cols: Array[String] = ds.columns): Dataset[T] = getNulls(cols)
      .union(getNonuniqueRows(cols))

    /**
     * Checks whether the specified columns is a local minimal array of columns satisfying uniqueness within the data frame
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def isMinimalUnique(cols: Array[String] = ds.columns): Boolean = {
      def subFrameNotUnique(colName: String): Boolean = !ds.isUnique(cols.filter(colName != _))

      ds.isUnique(cols) && cols.forall(subFrameNotUnique)
    }

    /**
     * Checks whether the specified columns form a candidate key for the data frame
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def isCandidateKey(cols: Array[String] = ds.columns): Boolean = !containsNull(cols) && isMinimalUnique(cols)

  }

}
