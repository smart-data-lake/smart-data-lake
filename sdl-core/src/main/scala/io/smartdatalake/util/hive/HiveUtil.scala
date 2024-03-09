/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.hive

import io.smartdatalake.definitions._
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.PerformanceUtils.measureTime
import io.smartdatalake.util.misc.{EnvironmentUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataobject.Table
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.net.URI
import java.time.Instant
import scala.sys.process.{ProcessLogger, _}
import scala.util.{Failure, Success, Try}

/**
 * Provides utility functions for Hive.
 */
private[smartdatalake] object HiveUtil extends SmartDataLakeLogger {

  /**
   * Deletes a Hive table
   *
   * @param table Hive table
   * @param tablePath path of table to delete
   * @param doPurge Flag to indicate if PURGE should be used when deleting (don't delete to HDFS trash). Default: true
   * @param existingOnly Flag if check "if exists" should be executed. Default: true
   */
  def dropTable(table: Table, tablePath: Path, filesystem: Option[FileSystem] = None, doPurge: Boolean = true, existingOnly: Boolean = true)(implicit session: SparkSession): Unit = {
    val existsClause = if (existingOnly) "if exists " else ""
    val purgeClause = if (doPurge) " purge" else ""
    val stmt = s"drop table $existsClause${table.fullName}$purgeClause"
    execSqlStmt(stmt)
    implicit val fs: FileSystem = filesystem.getOrElse(HdfsUtil.getHadoopFsFromSpark(tablePath))
    HdfsUtil.deletePath(tablePath, false)
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
   * Calculate maximum number of records per file to reach the HDFS block size as closely as possible
   * Numbers are retrieved from catalog so if a table doesn't have statistics, we will return None here
   *
   * We will reduce the number by 2%: If the number is too low, the block is not filled optimally. On the other hand,
   * if the number is too high we end up with an additional (very small) block which is worse.
   *
   * @param table Hive Table
   * @return Desired number of records per file if it can be determined, None otherwise
   */
  def calculateMaxRecordsPerFileFromStatistics(table: Table)(implicit session: SparkSession): Option[BigInt] = {
    val desiredSizePerFile = HdfsUtil.desiredFileSize(session.sparkContext.hadoopConfiguration)
    logger.debug("Desired filesize for session is " +desiredSizePerFile +" bytes.")

    session.sharedState.externalCatalog.getTable(table.db.get, table.name).stats.flatMap(s =>
      s.rowCount.map(rCount => (desiredSizePerFile / (s.sizeInBytes / rCount))*98/100))
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
          partitionValue.elements.mapValues(Some(_)) ++ partitionCols.diff(partitionValue.keys.toSeq).map( c => (c, None))
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
    partitionColsAndDatatypes.map( _.map(_(0).toLowerCase))
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
   * Writes DataFrame to Hive table by using DataFrameWriter.
   * A missing table gets created. Dynamic partitioning is used to create partitions on the fly by Spark.
   * Existing data of partition is overwritten, if table has no partitions all table-data is overwritten.
   *
   * Note that you need to use writeDfToHiveWithTickTock to write a hive table in tick-tock mode.
   *
   * @param session SparkSession
   * @param dfNew DataFrame to write
   * @param outputPath Path to store files for Table
   * @param table Table
   * @param partitions Partition column names
   * @param hdfsOutputType tables underlying file format, default = parquet
   * @param numInitialHdfsPartitions the initial number of files created if table does not exist yet, default = -1. Note: the number of files created is controlled by the number of Spark partitions.
   */
  def writeDfToHive(dfNew: DataFrame, outputPath: Path, table: Table, partitions: Seq[String], saveMode: SaveMode, numInitialHdfsPartitions: Int = -1)(implicit session: SparkSession): Unit = {
    logger.info(s"(${table.fullName}) writeDfToHive: starting outputPath=$outputPath partitions=$partitions saveMode=${saveMode.name}")

    // check if all partition cols are present in DataFrame
    val missingPartitionCols = partitions.diff(dfNew.columns)
    require( missingPartitionCols.isEmpty, s"(${table.fullName}) Partition column(s) ${missingPartitionCols.mkString(",")} are missing in DataFrame columns (${dfNew.columns.mkString(",")})." )

    // check if table exists and location is correct
    val tableExists = isHiveTableExisting(table)
    if (!tableExists) logger.info(s"(${table.fullName}) writeDfToHive: table doesnt exist yet")

    // check if partitionsOpt match with existing table definition
    if (tableExists) {
      val configuredCols = partitions.toSet
      val existingCols = getTablePartitionCols(table).getOrElse(Seq()).toSet
      require( configuredCols==existingCols, s"(${table.fullName}) writeDfToHive: configured vs existing partition columns are different: configured=$configuredCols, existing=$existingCols" )
    }

    // check if this run is with SchemaEvolution and sort columns (partition columns last)
    val (df_newColsSorted, withSchemaEvolution) = if (tableExists) {
      // check if schema evolution
      val df_existing = session.table(table.fullName)
      val withSchemaEvolution = !SchemaEvolution.hasSameColNamesAndTypes(df_existing, dfNew)
      if (withSchemaEvolution) logger.info(s"(${table.fullName}) writeDfToHive: schema evolution detected\nexisting=${df_existing.schema.treeString}\nnew=${dfNew.schema.treeString}")

      // Schema evolution with Partitions can only be done with Tick-Tock
      require( !(withSchemaEvolution && partitions.nonEmpty), s"(${table.fullName}) Schema evolution with partitions only works with TickTock! Use writeDfToHiveWithTickTock instead." )

      // move partition cols last, retain current column ordering if not schema evolution
      // TODO: Do partitions-columns not only need to be at the end, but also in the right order if you have more than one?
      val colsSorted = movePartitionColsLast( if (withSchemaEvolution) dfNew.columns else df_existing.columns, partitions )
      logger.debug(s"(${table.fullName}) writeDfToHive: columns sorted to ${colsSorted.mkString(",")}")
      val df_newColsSorted = dfNew.select(colsSorted.map(col):_*)
      (df_newColsSorted, withSchemaEvolution)

    } else { // table does not exists
      // move partition cols last
      val colsSorted = movePartitionColsLast( dfNew.columns, partitions )
      logger.debug(s"(${table.fullName}) writeDfToHive: columns sorted to ${colsSorted.mkString(",")}")
      val df_newColsSorted = dfNew.select(colsSorted.map(col):_*)
      (df_newColsSorted, false)
    }

    // write to table
    val originalMaxRecordsPerFile = session.conf.get("spark.sql.files.maxRecordsPerFile")
    if (tableExists && !withSchemaEvolution) {
      // insert into existing table
      logger.info(s"(${table.fullName}) writeDfToHive: insert into ${table.fullName}")

      // Try to determine maximum number of records according to catalog statistics
      val df_partitioned = if (numInitialHdfsPartitions == -1) {
        // pass DataFrame straight through if numInitialHdfsPartitions == -1, in this case the file size in the responsibility of the framework and must be controlled in custom transformations
        df_newColsSorted
      } else if (EnvironmentUtil.isSparkAdaptiveQueryExecEnabled) {
        // pass DataFrame straight through if AQE is enabled
        logger.warn(s"(${table.fullName}) numInitialHdfsPartitions is ignored when Spark 3.0 Adaptive Query Execution (AQE) is enabled")
        df_newColsSorted
      } else {
        val maxRecordsPerFile: Option[BigInt] = calculateMaxRecordsPerFileFromStatistics(table)
        if (maxRecordsPerFile.isDefined) {
          // if exact number of records could be determined from Hive statistics, use it to split files
          logger.info(s"(${table.fullName}) writing with maxRecordsPerFile " + maxRecordsPerFile.get.toLong)
          // TODO: Check for side effects (df.write.option("maxRecordsPerFile", ... ) does only work for FileWriters, but spark config "spark.sql.files.maxRecordsPerFile" works also for writing tables,
          //       so we're setting it on the current runtime config used by all DFs / RDDs
          session.conf.set("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile.get.toLong)
          HdfsUtil.repartitionForHdfsFileSize(df_newColsSorted, outputPath, reducePartitions = true)
        } else {
          HdfsUtil.repartitionForHdfsFileSize(df_newColsSorted, outputPath)
        }
      }

      // Write file
      df_partitioned.write
        .mode(saveMode)
        .insertInto(table.fullName)

    } else {
      // for new tables:
      // use user defined numInitialHdfsPartitions or leave partitioning as is
      // it's assumed that i.e. a CustomDfCreator takes care of proper partitioning in this case
      val df_partitioned = if (numInitialHdfsPartitions == -1) {
        // pass DataFrame straight through if numInitialHdfsPartitions == -1, in this case the file size in the responsibility of the framework and must be controlled in custom transformations
        df_newColsSorted
      } else if (EnvironmentUtil.isSparkAdaptiveQueryExecEnabled) {
        // pass DataFrame straight through if AQE is enabled
        logger.warn(s"(${table.fullName}) numInitialHdfsPartitions is ignored when Spark 3.0 Adaptive Query Execution (AQE) is enabled")
        df_newColsSorted
      } else df_newColsSorted.repartition(numInitialHdfsPartitions)

      // create and write to table
      if (partitions.nonEmpty) { // with partitions
        logger.info(s"(${table.fullName}) writeDfToHive: creating external partitioned table at location $outputPath")
        implicit val fs: FileSystem = HdfsUtil.getHadoopFsFromSpark(outputPath)
        HdfsUtil.deletePath(outputPath, doWarn=false) // delete existing data, as all partitions need to be written when table is created.
        df_partitioned.write
          .partitionBy(partitions:_*)
          .format(OutputType.Parquet.toString)
          .option("path", outputPath.toString)
          .mode("overwrite")
          .saveAsTable(table.fullName)

      } else { // without partitions
        logger.info(s"(${table.fullName}) writeDfToHive: creating table at location $outputPath")
        df_partitioned.write
          .format(OutputType.Parquet.toString)
          .option("path", outputPath.toString)
          .mode("overwrite")
          .saveAsTable(table.fullName)
      }
    }
    session.conf.set("spark.sql.files.maxRecordsPerFile", originalMaxRecordsPerFile.toLong)
  }


  /**
   * Writes DataFrame to Hive table by using DataFrameWriter.
   * A missing table gets created. Dynamic partitioning is used to create partitions on the fly by Spark.
   * Existing data of partition is overwritten, if table has no partitions all table-data is overwritten.
   * This method always uses the TickTock method to write the data.
   *
   * @param session SparkSession
   * @param df_new DataFrame to write
   * @param outputPath Directory to store files for Table
   * @param table Table
   * @param partitions Partitions column name
   * @param forceTickTock set to true if you want to always to tick-tock, and avoid the optimization to cancel tick-tock for partitioned tables
   */
  def writeDfToHiveWithTickTock(df_new: DataFrame, outputPath: Path, table: Table, partitions: Seq[String], saveMode: SaveMode, forceTickTock: Boolean = false)
                               (implicit session: SparkSession): Unit = {
    logger.info(s"(${table.fullName}) writeDfToHiveWithTickTock: start writing outputPath=$outputPath partitions=$partitions saveMode=${saveMode.name} forceTickTock=$forceTickTock")

    // check if all partition cols are present in DataFrame
    val missingPartitionCols = partitions.diff(df_new.columns)
    require( missingPartitionCols.isEmpty, s"""(${table.fullName}) partition columns ${missingPartitionCols.mkString(",")} not present in DataFrame""" )

    // check if table exists and location is correct
    val tableExists = isHiveTableExisting(table)
    if (!tableExists) logger.info(s"(${table.fullName}) writeDfToHive: table doesn't exist yet")

    // check if partitionsOpt match with existing table definition
    if (tableExists) {
      val configuredCols = partitions.toSet
      val existingCols = getTablePartitionCols(table).getOrElse(Seq()).toSet
      require( configuredCols==existingCols, s"(${table.fullName}) writeDfToHive: configured vs existing partition columns are different: configured=$configuredCols, existing=$existingCols" )
    }

    // check if this run is with SchemaEvolution and sort columns (partition columns last)
    val (df_newColsSorted, withSchemaEvolution) = if (tableExists) {
      // check if schema evolution
      val df_existing = session.table(table.fullName)
      val withSchemaEvolution = !SchemaEvolution.hasSameColNamesAndTypes(df_existing, df_new)
      if (withSchemaEvolution) logger.info(s"(${table.fullName}) writeDfToHive: schema evolution detected\nexisting=${df_existing.schema.treeString}\nnew=${df_new.schema.treeString}")

      // if schema evolution with partitioning, make sure old partitions data is included within new dataframe
      if (withSchemaEvolution && partitions.nonEmpty) {
        val existingPartitions = df_existing.select(array(partitions.map(col): _*)).distinct().collect().map( _.getSeq[String](0))
        val newPartitions = df_new.select(array(partitions.map(col): _*)).distinct().collect().map( _.getSeq[String](0))
        assert(existingPartitions.diff(newPartitions).nonEmpty, s"(${table.fullName}) writeDfToHive: schema evolution with partitions needs all existing data in new dataframe, but partition data of existing dataframe is missing in new data frame!")
      }

      // move partition cols last, retain current column ordering if not schema evolution
      // TODO: Do partitions-columns not only need to be at the end, but also in the right order if you have more than one?
      val colsSorted = movePartitionColsLast( if (withSchemaEvolution) df_new.columns else df_existing.columns, partitions )
      logger.debug(s"(${table.fullName}) writeDfToHive: columns sorted to ${colsSorted.mkString(",")}")
      val df_newColsSorted = df_new.select(colsSorted.map(col):_*)
      (df_newColsSorted, withSchemaEvolution)

    } else { // table does not exists
      // move partition cols last
      val colsSorted = movePartitionColsLast( df_new.columns, partitions )
      logger.debug(s"(${table.fullName}) writeDfToHive: columns sorted to ${colsSorted.mkString(",")}")
      val df_newColsSorted = df_new.select(colsSorted.map(col):_*)
      (df_newColsSorted, false)
    }

    // cancel tick-tock if
    // - partitions without schema evolution to avoid partition migration
    // - table doesnt exists yet
    val doTickTock = forceTickTock || ((partitions.isEmpty || withSchemaEvolution) && tableExists)

    // define table: use tmp-table if we need to *do* a tick-tock
    val tableName = if (doTickTock) {
      logger.info(s"(${table.fullName}) writeDfToHive: tick-tock needed")
      s"${table.fullName}_tmp"
    } else table.fullName

    // write to table
    if (tableExists && !doTickTock && !withSchemaEvolution) {
      // insert into existing table
      logger.info(s"(${table.fullName}) writeDfToHive: insert into $tableName")
      df_newColsSorted.write.mode(saveMode).insertInto(tableName)

    } else {
      // define location: use tick-tock path
      val location: Path = alternatingTickTockLocation2(table, outputPath)

      // create and write to table
      if (partitions.nonEmpty) { // with partitions
        logger.info(s"(${table.fullName}) writeDfToHive: creating external partitioned table $tableName at location $location")
        implicit val fs: FileSystem = HdfsUtil.getHadoopFsFromSpark(location)
        HdfsUtil.deletePath(location, doWarn=false) // delete existing data, as all partitions need to be written when table is created.
        df_newColsSorted.write
          .partitionBy(partitions:_*)
          .format(OutputType.Parquet.toString)
          .option("path", location.toString)
          .mode("overwrite")
          .saveAsTable(tableName)

      } else { // without partitions
        logger.info(s"(${table.fullName}) writeDfToHive: creating table $tableName at location $location")
        df_newColsSorted.write
          .format(OutputType.Parquet.toString)
          .option("path", location.toString)
          .mode("overwrite")
          .saveAsTable(tableName)
      }
    }

    // point hiveTable to new data for Tick-Tock table
    if (doTickTock) {
      val existingTable = table.fullName
      // drop existing table (schema is outdated), rename tmp table (new schema)
      // Attention: this table is potentially missing for some milliseconds...
      // Note: we could also change location of existing table, but this get's complicated for partitioned tables as all partition locations need to be changed as well, maybe even with multiple partition cols.
      logger.info(s"(${table.fullName}) writeDfToHive: droping table $existingTable, renaming table $tableName to $existingTable" )
      session.sql(s"DROP TABLE IF EXISTS $existingTable")
      session.sql(s"ALTER TABLE $tableName RENAME TO $existingTable")
    }
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
   * Executes a Hive system command through [[ProcessBuilder]].
   * Execution s blocked until the external command is finished.
   *
   * @param stmt Hive command to be executed
   * @throws AnalyzeTableException If system command has a return code != 0
   * @return Command exit status == 0: true, otherwise false
   */
  def execHiveSystemCommand(stmt: String): Boolean = {
    val cmd = "kinit"
    val stdOut = new StringBuilder
    val stdErr = new StringBuilder
    val exitStatus = cmd ! ProcessLogger(stdOut append _, stdErr append _)
    if (exitStatus == 0) {
      logger.info(s"$cmd: stdOut: $stdOut")
      true
    } else {
      logger.error(s"Hive system command failed, cmd: $cmd, exit status: $exitStatus, stderr: $stdErr")
      false
    }
  }

  /**
   * Loggs an exception thrown by a Hive statement and re-throws it.
   *
   * @param e exception to be handled
   * @param stmt Hive statement that threw the exception
   * @return Unit
   */
  def handleSqlException(e: Exception, stmt: String) : Unit = {
    logger.warn(s"Error in SQL statement '$stmt':\n${e.getMessage}")
  }

  /**
   * Checks if a Hive table exists
   *
   * @return true if a table exists, otherwise false
   */
  def isHiveTableExisting(table: Table)(implicit session: SparkSession): Boolean = {
    if (table.db.isDefined) session.catalog.tableExists(table.db.get, table.name)
    else session.catalog.tableExists(table.name)
  }

  def hiveTableLocation(table: Table)(implicit session: SparkSession): String = {
    val extendedDescribe = session.sql(s"describe extended ${table.fullName}")
      .cache()

    // Spark 2.2, 2.3: Location is found as row with col_name == "Location",
    // Some tables can have a real column "Location", so the data_type column is also verified for a path with "/"
    //
    // +----------------------------+-----------------------------+-------+
    //|col_name                    |data_type                    |comment|
    //+----------------------------+-----------------------------+-------+
    // ...
    //|Location                    |string                       |null   |
    //
    //|                            |                             |       |
    //|# Detailed Table Information|                             |       |
    //...
    //|Location                    |hdfs://nameservice1/user/... |       |
    //+----------------------------+-----------------------------+-------+
    //
    val location22 = Try(extendedDescribe.where(col("col_name") === "Location" && col("data_type")
      .contains(Path.SEPARATOR_CHAR)).select("data_type").first().getString(0)).toOption

    // Spark 2.1: Location must be parsed from row with col_name == "Detailed Table Information"
    val tableDetails = extendedDescribe.where("col_name like '%Detailed Table Information%'").select("*").first()
      .toString.map( c => if( c < ' ') " " else c).mkString // translate linebreak and control characters to whitespace
    // look for location in table details with regexp
    val locationPattern = """.*Location: ([^,\)]*)[,\)].*""".r
    val location21 = tableDetails match {
      case locationPattern(location) => Some(location)
      case _ => None
    }

    location22.orElse(location21).getOrElse( throw new TableInformationException( s"Location for table ${table.fullName} not found"))
  }

  def existingTableLocation(table: Table)(implicit session: SparkSession): URI = {
    session.sharedState.externalCatalog.getTable(table.db.get,table.name).location
  }

  def existingTickTockLocation(table: Table)(implicit session: SparkSession): String = {
    hiveTableLocation(table)
  }

  def getCurrentTickTockLocationSuffix(table: Table)(implicit session: SparkSession): HiveTableLocationSuffix.Value = {
    val currentLocation = hiveTableLocation(table)
    logger.debug(s"currentLocation: $currentLocation")
    HiveTableLocationSuffix.withName(new Path(currentLocation).getName)
  }

  def removeTickTockFromLocation(location: Path): Path = {
    if (location.getName == HiveTableLocationSuffix.Tock.toString || location.getName == HiveTableLocationSuffix.Tick.toString) location.getParent
    else location
  }

  def alternateTickTockLocation(location: Path): Path = {
    val currentTickTock = location.getName
    val baseLocation = location.getParent
    currentTickTock match {
      // Tick -> Tock
      case tt if tt==HiveTableLocationSuffix.Tick.toString => new Path(baseLocation, HiveTableLocationSuffix.Tock.toString)
      // Tock -> Tick
      case tt if tt==HiveTableLocationSuffix.Tock.toString => new Path(baseLocation, HiveTableLocationSuffix.Tick.toString)
      case _ => throw new IllegalArgumentException(s"Table location $location doesn't use Tick-Tock")
    }
  }

  def alternatingTickTockLocation2(table: Table, basePath: Path)(implicit session: SparkSession): Path = {
    if (isHiveTableExisting(table)) {
      alternateTickTockLocation(new Path(hiveTableLocation(table)))
    } else {
      // If the table doesn't exist yet, start with tick
      new Path(basePath, HiveTableLocationSuffix.Tick.toString)
    }
  }

  /**
   * Normalizes a HDFS path so they can be better compared.
   * i.e. by replacing \ with / and always pointing to tick
   *
   * @param path
   * @return
   */
  def normalizePath(path: String) : String = {
    // TODO: we should convert to Hadoop Path() and back to String for normalizing
    path
      .replaceAll("\\\\", Path.SEPARATOR)
      .replaceAll("file:/", "")
      .replaceAll("/+$", "")
      .replaceAll("tock$", "tick")
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
