/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.definitions.OutputType.OutputType
import io.smartdatalake.definitions.{HiveTableLocationSuffix, OutputType}
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.HdfsUtil.desiredFileSize
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.sys.process.{ProcessLogger, _}
import scala.util.{Failure, Success, Try}

/**
 * Provides utility functions for Hive.
 */
private[smartdatalake] object HiveUtil extends SmartDataLakeLogger {

  /**
   * Creates a String by concatenating all column names of a table. 
   * Columns are seperated by ','.
   *
   * @param session [[SparkSession]] used
   * @param tableName Name of table
   * @return
   */
  def tableColumnsString(session: SparkSession, dbName: String, tableName: String): String = {
    import session.implicits._ // Workaround for
    val tableSchema = execSqlStmt(session, s"show columns in $dbName.$tableName")
    tableSchema.map(c => c(0).toString.replace(" ","").toLowerCase).collect.mkString(",")
  }

  /**
   * Deletes a Hive table
   *
   * @param session [[SparkSession]] used
   * @param hiveDb Hive DB to use
   * @param tableName Name of table
   * @param doPurge Flag to indicate if PURGE should be used when deleting (don't delete to HDFS trash). Default: true
   * @param existingOnly Flag if check "if exists" should be executed. Default: true
   */
  def dropTable(session: SparkSession, hiveDb: String, tableName: String, doPurge: Boolean = true,
                existingOnly: Boolean = true): Unit = {
    val existsClause = if (existingOnly) "if exists " else ""
    val purgeClause = if (doPurge) " purge" else ""
    val stmt = s"drop table $existsClause $hiveDb.$tableName$purgeClause"
    execSqlStmt(session, stmt)
  }

  /**
   * Collects table-level statistics
   *
   * @param session [[SparkSession]] to use
   * @param hiveDb Hive DB to use
   * @param hiveTable Hive table for which statistics should get collected
   */
  def analyzeTable(session: SparkSession, hiveDb: String, hiveTable: String): Unit = {
    val stmt = s"ANALYZE TABLE $hiveDb.$hiveTable COMPUTE STATISTICS"
    Try(execSqlStmt(session, stmt)) match {
      case Success(_) => logger.info(s"Gathered table-level statistics on table $hiveDb.$hiveTable")
      case Failure(throwable) => logger.error(throwable.getMessage)
        throw new AnalyzeTableException(s"Error running: $stmt")
    }
  }

  /**
   * Collects column-level statistics
   *
   * @param session [[SparkSession]] to use
   * @param hiveDb Hive DB to use
   * @param hiveTable  Hive table for which statistics should get collected
   * @param columns The column list of the Hive table
   */
  def analyzeTableColumns(session: SparkSession, hiveDb: String, hiveTable: String, columns: String): Unit = {
    val stmt = s"ANALYZE TABLE $hiveDb.$hiveTable COMPUTE STATISTICS FOR COLUMNS $columns"
    Try(execSqlStmt(session, stmt)) match {
      case Success(_) => logger.info(s"Gathered column-level statistics on table $hiveDb.$hiveTable")
      case Failure(throwable) => logger.error(throwable.getMessage)
        throw new AnalyzeTableException(s"Error running: $stmt")
    }
  }

  /**
   * Calculate maximum number of records per file to reach the HDFS block size as closely as possible
   * Numbers are retrieved from catalog so if a table doesn't have statistics, we will return None here
   *
   * We will reduce the number by 2%: If the number is too low, the block is not filled optimally. On the other hand,
   * if the number is too high we end up with an additional (very small) block which is worse.
   *
   * @param session
   * @param hiveDb
   * @param hiveTable
   * @return Desired number of records per file if it can be determined, None otherwise
   */
  def calculateMaxRecordsPerFileFromStatistics(session: SparkSession, hiveDb: String, hiveTable: String): Option[BigInt] = {
    val desiredSizePerFile = HdfsUtil.desiredFileSize(session)
    logger.debug("Desired filesize for session is " +desiredSizePerFile +" bytes.")

    session.sharedState.externalCatalog.getTable(hiveDb, hiveTable).stats.flatMap(s =>
      s.rowCount.map(rCount => (desiredSizePerFile / (s.sizeInBytes / rCount))*98/100))
  }

  /**
   * Collects column-level statistics for partitions
   *
   * @param session [[SparkSession]] to use
   * @param hiveDb  Hive DB to use
   * @param hiveTable  Hive table for which statistics should get collected
   * @param partitions Partitions to collect statistics for
   * @param partitionValues Seq of PartitionValues of hiveTable (mapping col->value)
   */
  def analyzeTablePartitions(session: SparkSession, hiveDb: String, hiveTable: String
                           , partitions: Seq[String], partitionValues: Seq[PartitionValues]): Unit = {

    val preparedPartitionValues = if (partitionValues.nonEmpty) {
      partitionValues.map{
        partitionValue =>
          // extend PartitionValue with defaults for missing partition colums
          partitionValue.elements.mapValues(Some(_)) ++ partitions.diff(partitionValue.keys.toSeq).map( c => (c, None))
      }
    } else {
      // create a default entry for every partition column to compute statistics for all partition values existing on the storage
      Seq(partitions.map(c => (c, None)).toMap)
    }
    preparedPartitionValues.foreach{ p =>
      val partitionSpec = p.map{ case (col, value) => if(value.isDefined) s"$col='${value.get}'" else col}
        .mkString(",")
      val stmt = s"ANALYZE TABLE $hiveDb.$hiveTable PARTITION($partitionSpec) COMPUTE STATISTICS"
      Try(execSqlStmt(session, stmt)) match {
        case Success(_) => logger.info(s"Gathered partition-level statistics for $partitionSpec on table $hiveDb.$hiveTable")
        case Failure(throwable) => logger.error(throwable.getMessage)
          throw new AnalyzeTableException(s"Error running: $stmt")
      }
    }
  }

  // get Partitions for specified table from catalog
  def getTablePartitions(hiveDb:String, tableName:String) (implicit session: SparkSession) : Seq[Map[String,String]] = {
    import session.implicits._

    // Parse HDFS partitionname into Map
    def parseHDFSPartitionString(partitions:String) : Map[String,String] = try {
      partitions.split("/").map(_.split("=")).map( e => (e(0), e(1))).toMap
    } catch {
      case ex : Throwable =>
        println(s"partition doesnt follow structure (<key1>=<value1>[/<key2>=<value2>]...): $partitions")
        throw ex
    }

    session.sql(s"show partitions $hiveDb.$tableName").as[String].collect.map( parseHDFSPartitionString).toSeq
  }

  // get partition columns for specified table from DDL
  def getTablePartitionCols(hiveDb:String, tableName:String) (implicit session: SparkSession) : Option[Seq[String]] = {
    import session.implicits._

    // get ddl and concat into one string without newlines
    val tableDDL = session.sql(s"show create table $hiveDb.$tableName").as[String].collect.mkString(" ").replace("\n"," ")

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
   * Writes DataFrame to Hive table by using DataFrameWriter.
   * A missing table gets created. Dynamic partitioning is used to create partitions on the fly by Spark.
   * Existing data of partition is overwritten, if table has no partitions all table-data is overwritten.
   *
   * Note that you need to use writeDfToHiveWithTickTock FIXME: write this not better so it can be better understood.
   *
   * @param session SparkSession
   * @param dfNew DataFrame to write
   * @param outputDir Directory to store files for Table
   * @param hiveTable Tablename
   * @param hiveDb Hive database name
   * @param partitions Partition column names
   * @param hdfsOutputType tables underlying file format, default = parquet
   * @param numInitialHdfsPartitions the initial number of files created if table does not exist yet, default = -1. Note: the number of files created is controlled by the number of Spark partitions.
   */
  def writeDfToHive(session: SparkSession, dfNew: DataFrame, outputDir: String, hiveTable: String,
                    hiveDb: String, partitions: Seq[String], saveMode: SaveMode,
                    hdfsOutputType: OutputType = OutputType.Parquet, numInitialHdfsPartitions: Int = -1): Unit = {
    implicit val sss = session
    val startTime = System.nanoTime
    logger.info(s"writeDfToHive: starting for table $hiveDb.$hiveTable, outputDir: $outputDir, partitions:$partitions")

    // check if all partition cols are present in DataFrame
    val missingPartitionCols = partitions.diff(dfNew.columns)
    require( missingPartitionCols.isEmpty, s"""Partition column(s) ${missingPartitionCols.mkString(",")} are missing in DataFrame columns (${dfNew.columns.mkString(",")}).""" )

    // check if table exists and location is correct
    val tableExists = isHiveTableExisting(hiveTable,session,hiveDb)
    if (!tableExists) logger.info(s"writeDfToHive: table $hiveDb.$hiveTable doesnt exist yet")

    // check if partitionsOpt match with existing table definition
    if (tableExists) {
      val configuredCols = partitions.toSet
      val existingCols = getTablePartitionCols(hiveDb,hiveTable).getOrElse(Seq()).toSet
      require( configuredCols==existingCols, s"writeDfToHive: configured are different from tables existing partition columns: configured=$configuredCols, existing=$existingCols" )
    }

    // check if this run is with SchemaEvolution and sort columns (partition columns last)
    val (df_newColsSorted, withSchemaEvolution) = if (tableExists) {
      // check if schema evolution
      val df_existing = session.table(s"$hiveDb.$hiveTable")
      val withSchemaEvolution = !SchemaEvolution.hasSameColNamesAndTypes(df_existing, dfNew)
      if (withSchemaEvolution) {
        logger.info("writeDfToHive: schema evolution detected")
        logger.info("writeDfToHive: existing schema")
        df_existing.printSchema
        logger.info("writeDfToHive: new schema")
        dfNew.printSchema
      }

      // if schema evolution with partitioning, make sure old partitions data is included within new dataframe
      if (withSchemaEvolution && partitions.nonEmpty) {
        val existingPartitions = df_existing.select(array(partitions.map(col): _*)).distinct.collect.map( _.getSeq[String](0))
        val newPartitions = dfNew.select(array(partitions.map(col): _*)).distinct.collect.map( _.getSeq[String](0))
        assert(existingPartitions.diff(newPartitions).nonEmpty, "Schema Evolution mit Partitionierung: Bisher vorhandene Partitionen in neuem DataFrame nicht vorhanden!")
      }

      // move partition cols last, retain current column ordering if not schema evolution
      // TODO: Do partitions-columns not only need to be at the end, but also in the right order if you have more than one?
      val colsSorted = movePartitionColsLast( if (withSchemaEvolution) dfNew.columns else df_existing.columns, partitions )
      logger.debug(s"""writeDfToHive: columns sorted to ${colsSorted.mkString(",")}""")
      val df_newColsSorted = dfNew.select(colsSorted.map(col):_*)
      (df_newColsSorted, withSchemaEvolution)

    } else { // table does not exists
      // move partition cols last
      val colsSorted = movePartitionColsLast( dfNew.columns, partitions )
      logger.debug(s"""writeDfToHive: columns sorted to ${colsSorted.mkString(",")}""")
      val df_newColsSorted = dfNew.select(colsSorted.map(col):_*)
      (df_newColsSorted, false)
    }

    // Schema evolution with Partitions can only be done with Tick-Tock
    require( !(withSchemaEvolution && partitions.nonEmpty), "Schema evolution with partitions only works with TickTock! Use writeDfToHiveWithTickTock instead." )

    // define table
    val table = s"$hiveDb.$hiveTable"

    val originalMaxRecordsPerFile = session.conf.get("spark.sql.files.maxRecordsPerFile")

    // write to table
    if (tableExists && !withSchemaEvolution) {
      // insert into existing table
      logger.info(s"writeDfToHive: insert into $table")

      // Try to determine maximum number of records according to catalog statistics
      val maxRecordsPerFile: Option[BigInt] = calculateMaxRecordsPerFileFromStatistics(session, hiveDb, hiveTable)
      val df_partitioned = if(maxRecordsPerFile.isDefined) {
        // if exact number of records could be determined from Hive statistics, use it to split files
        logger.info(s"writing with maxRecordsPerFile " +maxRecordsPerFile.get.toLong)
        // TODO: Check for side effects (df.write.option("maxRecordsPerFile", ... ) does only work for FileWriters, but spark config "spark.sql.files.maxRecordsPerFile" works also for writing tables,
        //       so we're setting it on the current runtime config used by all DFs / RDDs
        session.conf.set("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile.get.toLong)
        HdfsUtil.repartitionForHdfsFileSize(df_newColsSorted, outputDir, reducePartitions = true)
      }
      else {
        HdfsUtil.repartitionForHdfsFileSize(df_newColsSorted, outputDir)
      }

      // Write file
      df_partitioned.write
        .mode(saveMode)
        .insertInto(table)

    } else {
      // for new tables:
      // use user defined numInitialHdfsPartitions or leave partitioning as is
      // it's assumed that i.e. a CustomDfCreator takes care of proper partitioning in this case
      val df_partitioned =
        if(numInitialHdfsPartitions == -1)  df_newColsSorted else df_newColsSorted.repartition(numInitialHdfsPartitions)

      // create and write to table
      if (partitions.nonEmpty) { // with partitions
        logger.info(s"writeDfToHive: creating external partitioned table $table at location $outputDir")
        HdfsUtil.deletePath(outputDir, session.sparkContext, doWarn=false) // delete existing data, as all partitions need to be written when table is created.
        df_partitioned.write
          .partitionBy(partitions:_*)
          .format(hdfsOutputType.toString)
          .option("path", outputDir)
          .mode("overwrite")
          .saveAsTable(table)

      } else { // without partitions
        logger.info(s"writeDfToHive: creating table $table at location $outputDir")
        df_partitioned.write
          .format(hdfsOutputType.toString)
          .option("path", outputDir)
          .mode("overwrite")
          .saveAsTable(table)
      }
    }
    session.conf.set("spark.sql.files.maxRecordsPerFile", originalMaxRecordsPerFile.toLong)
    val elapsedTime = System.nanoTime - startTime
    logger.info( s"writeDfToHive: Time measurement $table ${elapsedTime/1000000000L}.${elapsedTime/1000000 % 1000}s" )
  }


  /**
   * Writes DataFrame to Hive table by using DataFrameWriter.
   * A missing table gets created. Dynamic partitioning is used to create partitions on the fly by Spark.
   * Existing data of partition is overwritten, if table has no partitions all table-data is overwritten.
   * This method always uses the TickTock method to write the data.
   *
   * @param session SparkSession
   * @param df_new DataFrame to write
   * @param outputDir Directory to store files for Table
   * @param hiveTable Tablename
   * @param hiveDb Hive database name
   * @param partitions Partitions column name
   * @param hdfsOutputType tables underlying file format, default = parquet
   */
  def writeDfToHiveWithTickTock(session: SparkSession, df_new: DataFrame, outputDir: String, hiveTable: String,
                    hiveDb: String, partitions: Seq[String], saveMode: SaveMode,
                    hdfsOutputType: OutputType = OutputType.Parquet): Unit = {
    implicit val sss = session
    val startTime = System.nanoTime
    logger.info(s"writeDfToHiveWithTickTock: starting for table $hiveDb.$hiveTable, outputDir: $outputDir, partitions:$partitions")

    // check if all partition cols are present in DataFrame
    val missingPartitionCols = partitions.diff(df_new.columns)
    require( missingPartitionCols.isEmpty, s"""partition columns ${missingPartitionCols.mkString(",")} not present in DataFrame""" )

    // check if table exists and location is correct
    val tableExists = isHiveTableExisting(hiveTable,session,hiveDb)
    if (!tableExists) logger.info(s"writeDfToHive: table $hiveDb.$hiveTable doesn't exist yet")

    // check if partitionsOpt match with existing table definition
    if (tableExists) {
      val configuredCols = partitions.toSet
      val existingCols = getTablePartitionCols(hiveDb,hiveTable).getOrElse(Seq()).toSet
      require( configuredCols==existingCols, s"writeDfToHive: configured are different from tables existing partition columns: configured=$configuredCols, existing=$existingCols" )
    }

    // check if this run is with SchemaEvolution and sort columns (partition columns last)
    val (df_newColsSorted, withSchemaEvolution) = if (tableExists) {
      // check if schema evolution
      val df_existing = session.table(s"$hiveDb.$hiveTable")
      val withSchemaEvolution = !SchemaEvolution.hasSameColNamesAndTypes(df_existing, df_new)
      if (withSchemaEvolution) {
        logger.info("writeDfToHive: schema evolution detected")
        logger.info("writeDfToHive: existing schema")
        df_existing.printSchema
        logger.info("writeDfToHive: new schema")
        df_new.printSchema
      }

      // if schema evolution with partitioning, make sure old partitions data is included within new dataframe
      if (withSchemaEvolution && partitions.nonEmpty) {
        val existingPartitions = df_existing.select(array(partitions.map(col): _*)).distinct.collect.map( _.getSeq[String](0))
        val newPartitions = df_new.select(array(partitions.map(col): _*)).distinct.collect.map( _.getSeq[String](0))
        assert(existingPartitions.diff(newPartitions).nonEmpty, "Schema Evolution mit Partitionierung: Bisher vorhandene Partitionen in neuem DataFrame nicht vorhanden!")
      }

      // move partition cols last, retain current column ordering if not schema evolution
      // TODO: Do partitions-columns not only need to be at the end, but also in the right order if you have more than one?
      val colsSorted = movePartitionColsLast( if (withSchemaEvolution) df_new.columns else df_existing.columns, partitions )
      logger.debug(s"""writeDfToHive: columns sorted to ${colsSorted.mkString(",")}""")
      val df_newColsSorted = df_new.select(colsSorted.map(col):_*)
      (df_newColsSorted, withSchemaEvolution)

    } else { // table does not exists
      // move partition cols last
      val colsSorted = movePartitionColsLast( df_new.columns, partitions )
      logger.debug(s"""writeDfToHive: columns sorted to ${colsSorted.mkString(",")}""")
      val df_newColsSorted = df_new.select(colsSorted.map(col):_*)
      (df_newColsSorted, false)
    }

    // cancel tick-tock if
    // - partitions without schema evolution to avoid partition migration
    // - table doesnt exists yet
    val doTickTock = (partitions.isEmpty || withSchemaEvolution) && tableExists

    // define location: use tick-tock path
    val location = alternatingTickTockLocation2(hiveDb, session, hiveTable, outputDir)

    // define table: use tmp-table if we need to *do* a tick-tock
    val table = if (doTickTock) {
      logger.info(s"writeDfToHive: tick-tock needed")
      s"$hiveDb.${hiveTable}_tmp"
    } else s"$hiveDb.$hiveTable"

    // write to table
    if (tableExists && !doTickTock && !withSchemaEvolution) {
      // insert into existing table
      logger.info(s"writeDfToHive: insert into $table")
      df_newColsSorted.write.mode(saveMode).insertInto(table)

    } else {
      // create and write to table
      if (partitions.nonEmpty) { // with partitions
        logger.info(s"writeDfToHive: creating external partitioned table $table at location $location")
        HdfsUtil.deletePath(location, session.sparkContext, doWarn=false) // delete existing data, as all partitions need to be written when table is created.
        df_newColsSorted.write
          .partitionBy(partitions:_*)
          .format(hdfsOutputType.toString)
          .option("path", location)
          .mode("overwrite")
          .saveAsTable(table)

      } else { // without partitions
        logger.info(s"writeDfToHive: creating table $table at location $location")
        df_newColsSorted.write
          .format(hdfsOutputType.toString)
          .option("path", location)
          .mode("overwrite")
          .saveAsTable(table)
      }
    }

    // point hiveTable to new data for Tick-Tock table
    if (doTickTock) {
      val existingTable = s"$hiveDb.$hiveTable"
      // drop existing table (schema is outdated), rename tmp table (new schema)
      // Attention: this table is potentially missing for some milliseconds...
      // Note: we could also change location of existing table, but this get's complicated for partitioned tables as all partition locations need to be changed as well, maybe even with multiple partition cols.
      logger.info(s"writeDfToHive: droping table $existingTable, renaming table $table to $existingTable" )
      session.sql(s"DROP TABLE IF EXISTS $existingTable")
      session.sql(s"ALTER TABLE $table RENAME TO $existingTable")
    }
    val elapsedTime = System.nanoTime - startTime
    logger.info( s"writeDfToHive: Zeitmessung $table ${elapsedTime/1000000000L}.${elapsedTime/1000000 % 1000}s" )
  }

  /**
   * Collects table statistics for table or table with partitions
   *
   * @param session [[SparkSession]] to use
   * @param hiveDb Target Hive DB
   * @param hiveTable Target Hive table
   * @param partitions Seq of partitions of hiveTable
   * @param partitionValues Seq of PartitionValues of hiveTable (mapping col->value)
   */
  def analyze(session: SparkSession, hiveDb: String, hiveTable: String, partitions: Seq[String], partitionValues: Seq[PartitionValues] = Seq()): Unit = {
    // If partitions are present, statistics can't be collected for the table itself
    // only for partitions or columns
    val columns = tableColumnsString(session, hiveDb, hiveTable)
    if (partitions.isEmpty){
      analyzeTableColumns(session, hiveDb, hiveTable, columns)
      analyzeTable(session, hiveDb, hiveTable)
    } else {
      analyzeTablePartitions(session, hiveDb, hiveTable, partitions, partitionValues)
      analyzeTableColumns(session, hiveDb, hiveTable, columns)
    }
  }

  /**
   * Executes a Spark SQL statement
   *
   * @param session [[SparkSession]] to use
   * @param stmt statement to be executed
   * @return result DataFrame
   */
  def execSqlStmt(session: SparkSession, stmt: String): DataFrame = {
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
   * @param tableName Name of table
   * @param session [[SparkSession]] to use
   * @param hiveDb Hive DB to use
   * @return true if a table exists, otherwise false
   */
  def isHiveTableExisting(tableName: String, session: SparkSession, hiveDb: String): Boolean = {
    session.catalog.tableExists(hiveDb, tableName)
  }

  def hiveTableLocation(dbName: String, session: SparkSession, tableName: String): String = {
    val extendedDescribe = session.sql(s"describe extended $dbName.$tableName")
      .cache

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
    val location22 = Try(extendedDescribe.where(col("col_name") === "Location" && col("data_type").contains("/")).select("data_type").first.getString(0)).toOption

    // Spark 2.1: Location must be parsed from row with col_name == "Detailed Table Information"
    val tableDetails = extendedDescribe.where("col_name like '%Detailed Table Information%'").select("*").first()
      .toString.map( c => if( c < ' ') " " else c).mkString // translate linebreak and control characters to whitespace
    // look for location in table details with regexp
    val locationPattern = """.*Location: ([^,\)]*)[,\)].*""".r
    val location21 = tableDetails match {
      case locationPattern(location) => Some(location)
      case _ => None
    }

    location22.orElse(location21).getOrElse( throw new TableInformationException( s"Location for table $dbName.$tableName not found"))
  }

  def existingTickTockLocation(dbName: String, session: SparkSession, tableName: String): String = {
    hiveTableLocation(dbName, session, tableName)
  }

  def getCurrentTickTockLocationSuffix(dbName: String, session: SparkSession, tableName: String): HiveTableLocationSuffix.Value = {
    val currentLocation = hiveTableLocation(dbName, session, tableName)
    logger.debug(s"currentLocation: $currentLocation")
    HiveTableLocationSuffix.withName(currentLocation.split('/').last)
  }

  def alternateTickTockLocation(currentLocation:String): String = {
    val currentTickTock = currentLocation.split('/').last
    val baseLocation = currentLocation.substring(0,currentLocation.lastIndexOf('/'))

    currentTickTock match {
      case tt if tt==HiveTableLocationSuffix.Tick.toString => // Tick -> Tock
        s"$baseLocation/${HiveTableLocationSuffix.Tock.toString}"
      case tt if tt==HiveTableLocationSuffix.Tock.toString => // Tock -> Tick
        s"$baseLocation/${HiveTableLocationSuffix.Tick.toString}"
      case _ =>
        throw new IllegalArgumentException(s"Table location $currentLocation doesn't use Tick-Tock")
    }
  }

  def alternatingTickTockLocation(dbName: String, session: SparkSession, tableName: String): String = {
    val currentLocation = hiveTableLocation(dbName, session, tableName)
    logger.debug(s"currentLocation: $currentLocation")
    val newLocation = alternateTickTockLocation(currentLocation)
    logger.debug(s"newLocation: $currentLocation")
    newLocation
  }

  def alternatingTickTockLocation2(dbName: String, session: SparkSession, tableName: String, outputDir:String): String = {
    if (isHiveTableExisting(tableName,session,dbName)) {
      alternateTickTockLocation(hiveTableLocation(dbName, session, tableName))
    } else {
      // If the table doesn't exist yet, start with tick
      s"$outputDir/${HiveTableLocationSuffix.Tick.toString}"
    }
  }
}
