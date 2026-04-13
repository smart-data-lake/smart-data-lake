/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.util.spark.hive

import io.smartdatalake.definitions._
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.PerformanceUtils.measureTime
import io.smartdatalake.util.misc.{EnvironmentUtil, SchemaUtil, SmartDataLakeLogger}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.net.URI
import java.time.Instant
import scala.sys.process.{ProcessLogger, _}
import scala.util.{Failure, Success, Try}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.generic.Table

/**
 * Provides utility functions for Hive.
 */
private[smartdatalake] object HiveUtil extends SmartDataLakeLogger {

  /**
   * Deletes a Hive table
   *
   * @param table Hive table
   * @param tablePath Optional path of table to delete (can be None for managed tables...)
   * @param doPurge Flag to indicate if PURGE should be used when deleting (don't delete to HDFS trash). Default: true
   * @param existingOnly Flag if check "if exists" should be executed. Default: true
   */
  def dropTableOptionalPath(table: Table, tablePath: Option[Path], filesystem: Option[FileSystem] = None, doPurge: Boolean = true, existingOnly: Boolean = true)(implicit session: SparkSession): Unit = {
    val existsClause = if (existingOnly) "if exists " else ""
    val purgeClause = if (doPurge) " purge" else ""
    val stmt = s"drop table $existsClause${table.fullName}$purgeClause"
    execSqlStmt(stmt)
    tablePath.foreach { path =>
      implicit val fs: FileSystem = filesystem.getOrElse(HdfsUtil.getHadoopFsFromSpark(path))
      HdfsUtil.deletePath(path, doWarn = false)
    }
  }

  def dropTable(table: Table, tablePath: Path, filesystem: Option[FileSystem] = None, doPurge: Boolean = true, existingOnly: Boolean = true)(implicit session: SparkSession): Unit = {
    dropTableOptionalPath(table, Some(tablePath), filesystem, doPurge, existingOnly)
  }

    /**
   * Collects table-level statistics
   *
   * @param table Hive table
   */
  def analyzeTable(table: Table)(implicit session: SparkSession): Unit = {
    val stmt = s"ANALYZE TABLE ${table.fullName} COMPUTE STATISTICS"
    Try(measureTime(execSqlStmt(stmt))) match {
      case Success((_,t)) =>
        alterTableProperties(table, Map(TableStatsType.LastAnalyzedAt.toString -> Instant.now().toEpochMilli))
        logger.info(s"Gathered table-level statistics on table ${table.fullName} in $t seconds")
      case Failure(throwable) => logger.error(throwable.getMessage)
        throw new AnalyzeTableException(s"Error running: $stmt")
    }
  }

  /**
   * Collects column-level statistics
   *
   * @param table Hive table
   * @param columns Columns to collect statistics from
   */
  def analyzeTableColumns(table: Table, columns: Seq[String] = Seq(), partitionValue: Option[PartitionValues] = None )(implicit session: SparkSession): Unit = {
    val columnsClause = if (columns.nonEmpty) s"COLUMNS ${columns.mkString(",")}" else "ALL COLUMNS"
    val stmt = s"ANALYZE TABLE ${table.fullName} COMPUTE STATISTICS FOR $columnsClause"
    Try(measureTime(execSqlStmt(stmt))) match {
      case Success((_,t)) =>
        alterTableProperties(table, Map(TableStatsType.LastAnalyzedColumnsAt.toString -> Instant.now().toEpochMilli))
        logger.info(s"Gathered column-level statistics on table ${table.fullName} in $t seconds")
      case Failure(e) => logger.error(s"${e.getClass.getSimpleName}: ${e.getMessage}")
        throw new AnalyzeTableException(s"Error ${e.getClass.getSimpleName} ${e.getMessage} running: $stmt")
    }
  }

  /**
   * Collects column-level statistics for partitions
   *
   * @param table Hive table
   * @param partitionCols Partitioned columns
   * @param partitionValues Partition values
   */
  def analyzeTablePartitions(table: Table, partitionCols: Seq[String], partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {

    val preparedPartitionValues = if (partitionValues.nonEmpty) {
      partitionValues.map{
        partitionValue =>
          // extend PartitionValue with defaults for missing partition colums
          partitionValue.elements.view.mapValues(Some(_)) ++ partitionCols.diff(partitionValue.keys.toSeq).map( c => (c, None))
      }
    } else {
      // create a default entry for every partition column to compute statistics for all partition values existing on the storage
      Seq(partitionCols.map(c => (c, None)).toMap)
    }
    preparedPartitionValues.foreach{ p =>
      val partitionSpec = p.map{ case (col, value) => if(value.isDefined) s"$col='${value.get}'" else col}
        .mkString(",")
      val stmt = s"ANALYZE TABLE ${table.fullName} PARTITION($partitionSpec) COMPUTE STATISTICS"
      Try(execSqlStmt(stmt)) match {
        case Success(_) => logger.info(s"Gathered partition-level statistics for $partitionSpec on table ${table.fullName}")
        case Failure(e) => logger.error(s"${e.getClass.getSimpleName}: ${e.getMessage}")
          throw new AnalyzeTableException(s"Error ${e.getClass.getSimpleName} ${e.getMessage} running: $stmt")
      }
    }
  }

  // get Partitions for specified table from catalog
  def getTablePartitions(table: Table) (implicit session: SparkSession) : Seq[Map[String,String]] = {
    import session.implicits._

    // Parse HDFS partitionname into Map
    def parseHDFSPartitionString(partitions:String) : Map[String,String] = try {
      partitions.split(Path.SEPARATOR_CHAR).map(_.split("=")).map( e => (e(0), e(1))).toMap
    } catch {
      case ex : Throwable =>
        println(s"partition doesnt follow structure (<key1>=<value1>[/<key2>=<value2>]...): $partitions")
        throw ex
    }

    session.sql(s"show partitions ${table.fullName}").as[String].collect().map( parseHDFSPartitionString).toSeq
  }

  // get partition columns for specified table from DDL
  def getTablePartitionCols(table: Table) (implicit session: SparkSession) : Option[Seq[String]] = {
    import session.implicits._

    // get ddl and concat into one string without newlines
    val tableDDL = session.sql(s"show create table ${table.fullName}").as[String].collect().mkString(" ").replace("\n"," ")

    // extract partition by declaration
    val regexPartitionBy = raw"PARTITIONED BY\s+\(([^\)]+)\)".r.unanchored
    val partitionColsAndDatatypes = tableDDL match {
      case regexPartitionBy( partitionByDDL ) => {
        val columnNameAllowedChars = (('a' to 'z') ++ ('A' to 'Z') ++ ( '0' to '9' ) :+ '_' :+ ' ' :+ ',')
        // first split partition columns definition separated by comma, then split column name and type separated by whitespace
        Some(partitionByDDL.trim.split(',').map(_.trim.filter(columnNameAllowedChars.contains(_)).split(' ').filter(!_.isEmpty)))
      }
      case _ => None
    }

    // return seq of columns
    partitionColsAndDatatypes.map(_.map(_(0)))
  }

  private def movePartitionColsLast( cols:Seq[String], partitions:Seq[String] ): Seq[String] = {
    val (partitionCols, nonPartitionCols) = cols.partition( c => partitions.contains(c))
    nonPartitionCols ++ partitionCols
  }

  /**
   * Move partition columns at end of DataFrame as required when writing to Hive in Spark > 2.x
   */
  def movePartitionColsLast( df: DataFrame, partitions:Seq[String] ): DataFrame = {
    val newColOrder = movePartitionColsLast(df.columns, partitions)
    df.select(newColOrder.map(col):_*)
  }

  /**
   * Collects table statistics for table or table with partitions
   *
   * @param table Hive table
   * @param columns: Columns to analyse
   * @param partitionCols Partitioned columns
   * @param partitionValues Partition values
   */
  def analyze(table: Table, columns: Seq[String], partitionCols: Seq[String], partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): Unit = {
    if (partitionCols.isEmpty) {
      analyzeTable(table)
      val stats = getCatalogStats(table)
      val sizeInBytes = stats(TableStatsType.TableSizeInBytes.toString).asInstanceOf[BigInt]
      if (sizeInBytes <= Environment.analyzeTableColumnMaxBytesThreshold) {
        analyzeTableColumns(table, columns)
      } else {
        logger.warn(s"Column stats for table ${table.fullName} not calculated because table size ($sizeInBytes Bytes) is bigger than setting analyzeTableColumnMaxBytesThreshold (${Environment.analyzeTableColumnMaxBytesThreshold} Bytes)")
      }
    } else {
      analyzeTablePartitions(table, partitionCols, partitionValues)
      // sum size for all partitions
      val sizeInBytes = listPartitions(table, partitionCols)
        .map(pv => getCatalogPartitionStats(table, pv))
        .flatMap(s => s.get(TableStatsType.TableSizeInBytes.toString).map(_.asInstanceOf[BigInt])).sum
      // Note that computing column statistics for selected partitions only is *not* supported by Spark, it will always analyze the whole table
      // see also log "WARN SparkSqlAstBuilder - Partition specification is ignored when collecting column statistics" when calling ANALYZE TABLE with PARTITION and COLUMN clause.
      if (sizeInBytes <= Environment.analyzeTableColumnMaxBytesThreshold) {
        analyzeTableColumns(table, columns)
      } else {
        logger.warn(s"Column stats for table ${table.fullName} not calculated because table size ($sizeInBytes Bytes) is bigger than setting analyzeTableColumnMaxBytesThreshold (${Environment.analyzeTableColumnMaxBytesThreshold} Bytes)")
      }
    }
  }

  /**
   * Executes a Spark SQL statement
   *
   * @param session [[SparkSession]] to use
   * @param stmt statement to be executed
   * @return result DataFrame
   */
  def execSqlStmt(stmt: String)(implicit session: SparkSession): DataFrame = {
    try {
      logger.info(s"Executing SQL statement: $stmt")
      session.sql(stmt)
    } catch {
      case e: Exception =>
        handleSqlException(e, stmt)
        throw e
    }
  }

  /**
   * Logs an exception thrown by a Hive statement and re-throws it.
   *
   * @param e exception to be handled
   * @param stmt Hive statement that threw the exception
   * @return Unit
   */
  def handleSqlException(e: Exception, stmt: String) : Unit = {
    logger.warn(s"Error in SQL statement '$stmt':\n${e.getMessage}")
  }

  def listPartitions(table: Table, partitions: Seq[String])(implicit session: SparkSession): Seq[PartitionValues] = {
    import session.implicits._
    if (partitions.nonEmpty) {
      val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partitions)
      // list directories and extract partition values
      session.sql(s"show partitions ${table.fullName}").as[String].collect().toSeq
        .map( path => PartitionLayout.extractPartitionValues(partitionLayout, path + Path.SEPARATOR))
    } else Seq()
  }

  def createEmptyPartition(table: Table, partitionValues: PartitionValues)(implicit session: SparkSession): Unit = {
    val partitionDef = partitionValues.elements.map{ case (k,v) => s"$k='$v'"}.mkString(", ")
    execSqlStmt(s"ALTER TABLE ${table.fullName} ADD IF NOT EXISTS PARTITION ($partitionDef)")
  }

  def dropPartition(table: Table, tablePath: Path, partition: PartitionValues, filesystem: FileSystem)(implicit session: SparkSession): Unit = {
    val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partition.keys.toSeq)
    val partitionPath = new Path(tablePath, partition.getPartitionString(partitionLayout))
    val partitionDef = partition.elements.map{ case (k,v) => s"$k='$v'"}.mkString(", ")
    execSqlStmt(s"ALTER TABLE ${table.fullName} DROP IF EXISTS PARTITION ($partitionDef)")
    HdfsUtil.deletePath(partitionPath, false)(filesystem)
  }

  def movePartition(table: Table, tablePath: Path, existingPartition: PartitionValues, newPartition: PartitionValues, filenameWithGlobs: String, filesystem: FileSystem)(implicit session: SparkSession): Unit = {
    val partitionLayout = HdfsUtil.getHadoopPartitionLayout(existingPartition.keys.toSeq)
    val existingPartitionPath = new Path(tablePath, existingPartition.getPartitionString(partitionLayout))
    val existingPartitionPathWithFilenameGlobs = new Path(existingPartitionPath, filenameWithGlobs)
    val newPartitionPath = new Path(tablePath, newPartition.getPartitionString(partitionLayout))
    val newPartitionDef = newPartition.elements.map{ case (k,v) => s"$k='$v'"}.mkString(", ")
    HdfsUtil.moveFiles( existingPartitionPathWithFilenameGlobs, newPartitionPath, addPrefixIfExisting = true)(filesystem)
    dropPartition(table, tablePath, existingPartition, filesystem)
    execSqlStmt(s"ALTER TABLE ${table.fullName} ADD IF NOT EXISTS PARTITION ($newPartitionDef)")
  }

  /**
   * Note: this works only for tables in the Hive Metastore
   */
  def getCatalogStats(table: Table)(implicit session: SparkSession): Map[String,Any] = {
    val metadata = session.sessionState.catalog.getTableMetadata(table.tableIdentifier)
    val catalogStats = metadata.stats.map( stats =>
      Seq(stats.rowCount.map(v => TableStatsType.NumRows.toString -> v), Some(TableStatsType.TableSizeInBytes.toString -> stats.sizeInBytes)).flatten.toMap
    ).getOrElse(Map())
    val lastAnalyzedAt = metadata.properties.get(TableStatsType.LastAnalyzedAt.toString).map(v => v.toLong)
    val lastAnalyzedColumnsAt = metadata.properties.get(TableStatsType.LastAnalyzedColumnsAt.toString).map(v => v.toLong)
    val otherStats = Seq(Some(TableStatsType.CreatedAt.toString -> metadata.createTime), lastAnalyzedAt.map(v =>  TableStatsType.LastAnalyzedAt.toString -> v), lastAnalyzedColumnsAt.map(v =>  TableStatsType.LastAnalyzedColumnsAt.toString -> v)).flatten.toMap
    otherStats ++ catalogStats
  }

  /**
   * Note: this works only for tables in the Hive Metastore
   */
  def getCatalogPartitionStats(table: Table, partitionValues: PartitionValues)(implicit session: SparkSession): Map[String, Any] = {
    val metadata = session.sessionState.catalog.getPartition(table.tableIdentifier, partitionValues.getMapString)
    val catalogStats = metadata.stats.map(stats =>
      Seq(stats.rowCount.map(v => TableStatsType.NumRows.toString -> v), Some(TableStatsType.TableSizeInBytes.toString -> stats.sizeInBytes)).flatten.toMap
    ).getOrElse(Map())
    catalogStats + (TableStatsType.CreatedAt.toString -> metadata.createTime)
  }

  /**
   * Note: this works only for tables in the Hive Metastore
   */
  def getCatalogColumnStats(table: Table)(implicit session: SparkSession): Map[String, Map[String, Any]] = {
    session.sessionState.catalog.getTableMetadata(table.tableIdentifier).stats.toSeq.flatMap(_.colStats).toMap
      .mapValues (
        stats => Seq(
          stats.distinctCount.map(ColumnStatsType.DistinctCount.toString -> _),
          stats.nullCount.map(ColumnStatsType.NullCount.toString -> _),
          stats.avgLen.map(ColumnStatsType.AvgLen.toString -> _),
          stats.maxLen.map(ColumnStatsType.MaxLen.toString -> _),
          stats.min.map(ColumnStatsType.Min.toString -> _),
          stats.max.map(ColumnStatsType.Max.toString -> _)
        ).flatten.toMap
      ).toMap
  }

  /**
   * Note: this works only for tables in the Hive Metastore
   */
  def getCatalogPartitionColumnStats(table: Table, partitionValues: PartitionValues)(implicit session: SparkSession): Map[String, Map[String, Any]] = {
    session.sessionState.catalog.getPartition(table.tableIdentifier, partitionValues.getMapString).stats.toSeq.flatMap(_.colStats).toMap
      .mapValues(
        stats => Seq(
          stats.distinctCount.map(ColumnStatsType.DistinctCount.toString -> _),
          stats.nullCount.map(ColumnStatsType.NullCount.toString -> _),
          stats.avgLen.map(ColumnStatsType.AvgLen.toString -> _),
          stats.maxLen.map(ColumnStatsType.MaxLen.toString -> _),
          stats.min.map(ColumnStatsType.Min.toString -> _),
          stats.max.map(ColumnStatsType.Max.toString -> _)
        ).flatten.toMap
      ).toMap
  }

  /**
   * Query partitions from catalog
   *
   * Note that for Hive Metastore (HMD) this might not be the best solution, as it depends on up-to-date partition metadata in HMS!
   * We can do a directory listing for Hive tables. But for Delta Lake directory listing is not suitable, as there might be directories which contain only outdated records.
   * In this case using the catalog is more efficient than quering them using a Spark DataFrame.
   *
   * @return
   */
  def getPartitionValuesFromCatalog(table: Table)(implicit session: SparkSession): Seq[PartitionValues] = {
    val metadata = session.sessionState.catalog.listPartitions(table.tableIdentifier)
    metadata.map(p => PartitionValues(p.spec))
  }

  /**
   * Set table properties by execute and "alter table ... set tblproperties" statement.
   * Existing properties values will be overwritten.
   * If existing properties are not included in parameter 'properties', they will survive with their current value.
   */
  def alterTableProperties(table: Table, properties: Map[String,Any])(implicit session: SparkSession): Unit = {
    execSqlStmt(s"ALTER TABLE ${table.fullName} SET TBLPROPERTIES(${properties.map{case(k,v) => s"$k = '$v'"}.mkString(",")})")
  }

}
