/*
 * Smart Data Lake - Build your data lake the smart way.
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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

trait Quality extends Transform {

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
          getNletten().show(8, truncate = false)
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
      ds.agg(count("*").as("cnt_rows"), statCols: _*)
    }


    /** * treating gaps in axis (time or space) ** */

    /**
     * Füllt Datenlücken mit dem nächsten oder letzten vorherigen Wert auf.
     * Ist der Parameter takeNextValueFirst=true, dann wird zuerst nach einem nächsten Wert geschaut.
     *
     * ACHTUNG: Alle Spalten des Ergebnisses sind nullable, auch wenn sie es vorher nicht waren:(
     *
     * @param keyColNames        : Spalten, die zusammen mit orderCol ein Primärschlüsselkandidat sind
     * @param dataColNames       : Namen der Spalten deren Lücken zu füllen sind
     * @param orderColName       : Name der Ordnungsspalte, z.B. gueltig_ab
     * @param takeNextValueFirst : gibt an, ob zuerst der nächste und dann der vorherige Wert genommen wird
     * @return data frame ohne NULL in Spalte dataColName
     */
    final def fillGaps(keyColNames: Iterable[String], dataColNames: Iterable[String], orderColName: String, takeNextValueFirst: Boolean = true): DataFrame = {
      val dsColumns = ds.columns
      val fenestra_next = Window.partitionBy(keyColNames.head, keyColNames.tail.toSeq: _*).orderBy(orderColName).rangeBetween(Window.currentRow, Window.unboundedFollowing)
      val fenestra_prev = Window.partitionBy(keyColNames.head, keyColNames.tail.toSeq: _*).orderBy(orderColName).rangeBetween(Window.unboundedPreceding, Window.currentRow)

      def newColumn(colName: String): Column = if (colName.contains(colName)) {
        if (takeNextValueFirst) coalesce(first(col(colName), ignoreNulls = true).over(fenestra_next), last(col(colName), ignoreNulls = true).over(fenestra_prev)).as(colName) else {
          coalesce(last(col(colName), ignoreNulls = true).over(fenestra_prev), first(col(colName), ignoreNulls = true).over(fenestra_next)).as(colName)
        }
      } else {
        col(colName)
      }
      // TODO: find a way to declare columns as not-nullable
      ds.select(dsColumns.map(newColumn): _*)
    }

    /**
     * adds a column which indicates whether the next interval [fromColName , toColName[ is adjacent
     *
     * @param keyColNames      : Spaltennamen des Schlüssels
     * @param fromColName      : Spaltennamen der Intervallanfänge, die auf Lücken untersucht werden
     * @param toColName        : Spaltennamen der Intervallenden, die auf Lücken untersucht werden
     * @param orderColNames    : Sortierung
     * @param gapIndicatorName : Name der Ergebnisspalte
     * @return union of the frames
     */
    final def getGaps(keyColNames: Iterable[String],
                      fromColName: String, toColName: String,
                      orderColNames: Iterable[String], gapIndicatorName: String = "_is_adjacent"): DataFrame = {
      val nextFromColName = s"_next_$fromColName"
      val fenestra = Window.partitionBy(keyColNames.map(col).toSeq: _*).orderBy(orderColNames.map(col).toSeq: _*)

      ds.withColumn("_islastrow", lead(col(keyColNames.head), 1).over(fenestra).isNull)
        .withColumn(nextFromColName, lead(col(fromColName), 1).over(fenestra))
        .withColumn(gapIndicatorName, coalesce(col(toColName) === col(nextFromColName), col("_islastrow"), lit(false)))
        .drop(nextFromColName, "_islastrow")
    }


    /** * checking for N-letten ** */

    /**
     * returns Nletten of this data frame with an additional count column
     * overloaded version because used often to check whether columnName forms a unique key
     *
     * @param columName : names of column which is to be considered
     * @return subdataframe of Nletten with an additional count column
     */
    def getNletten(columName: String): DataFrame = getNletten(cols = Array(columName))

    /**
     * returns Nletten of this data frame with an additional count column
     * usefull to check whether cols form a unique key
     *
     * @param cols         : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @param countColname : name of count column, default name: cnt
     * @return subdataframe of Nletten with an additional count column
     */
    def getNletten(cols: Array[String] = ds.columns, countColname: String = "cnt"): DataFrame = {
      val forbiddenColumnNames = Array("count", countColname)
      // for better usability we define empty Array of cols to mean all columns of df
      val colsInDs: Array[String] = if (cols.isEmpty) ds.columns else ds.columns.intersect(cols)
      if (colsInDs.isEmpty) throw new IllegalArgumentException(s"Argument cols must contain at least 1 name of a column of your dataset.\n   ds.columns = ${ds.columns.mkString(",")}\n   cols = ${cols.mkString(",")} ")
      val dfProjected: DataFrame = ds.select(colsInDs.map(col): _*)
      val dfColumns: Array[String] = dfProjected.columns
      // If df contains forbidden column then the result contains two columns with the same name
      forbiddenColumnNames.foreach(str =>
        require(!dfColumns.contains(str), s"data frame df must not contain column named $str. df.columns = ${dfColumns.mkString(",")}")
      )

      dfProjected.groupBy(dfColumns.head, dfColumns.tail: _*)
        .count().withColumnRenamed("count", countColname)
        .where(col(countColname) > 1)
    }

  }
}
