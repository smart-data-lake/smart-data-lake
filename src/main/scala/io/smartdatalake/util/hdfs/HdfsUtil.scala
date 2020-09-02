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
package io.smartdatalake.util.hdfs

import java.net.URI

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Provides utility functions for HDFS.
 */
private[smartdatalake] object HdfsUtil extends SmartDataLakeLogger {

  /**
   * Returns size information about existing files in HDFS
   *
   * @param path Path to files in HDFS
   * @return Amount of files, total size of files in Bytes, average size of files in bytes
   */
  def sizeInfo(path: Path, fs: FileSystem): (Long, Long, Long) = {
    try {
      val recursive = false
      val ri = fs.listFiles(path, recursive)
      val it = new Iterator[org.apache.hadoop.fs.LocatedFileStatus]() {
        override def hasNext = ri.hasNext

        override def next() = ri.next()
      }
      val filesToIgnore = Seq("_metadata","_common_metadata","_SUCCESS")
      val files = it.toList.filter {
        f => !filesToIgnore.exists(f.getPath.getName.endsWith(_))
      }
      val numFiles = files.size
      val sumSize = files.map(_.getLen).sum
      val avgSize: Long =
      if (numFiles > 0) {
        sumSize / numFiles
      } else 0
      (numFiles, sumSize, avgSize)
    } catch {
      case e: java.io.FileNotFoundException => {
        (0, 0, 0)
      }
    }
  }

  /**
   * 128MB equals the default HDFS block size.
   */
  val DefaultBlocksize = 128 * 1024 * 1024

  /**
   * Try to lookup the dfs blocksize or use the default blocksize of currently 128 MB
   *
   * @param session
   * @return
   */
  def desiredFileSize(implicit session:SparkSession): Long = {
    session.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", DefaultBlocksize)
  }

  /**
   * Tries to find a reasonable amount of RDD partitions for a DataFrame.
   *
   * Only changes amount of partitions if files exist already.
   * Amount of records i.e. is not calculated as operations like df.count are expensive.
   * This method only makes sense on DataFrames that build on existing files,
   * i.e. when using historize or deduplication.
   *
   * Uses repartition if the resulting partition count is higher than the current and coalesce if it's lower
   *
   * @param df [[DataFrame]] whose partitions should be optimized
   * @param existingFilePath HDFS path of existing files
   * @param reducePartitions If you use maxRecordsPerFile to handle the file boundaries, set this to true.
   *                         It will effectively half the number of partitions so they are large enough for
   *                         Spark to handle the splitting of files. Otherwise the resulting partitions could be too
   *                         small and Spark can't use up the configured boundaries.
   * @return repartitioned [[DataFrame]] (or Input [[DataFrame]] if partitioning is untouched)
   */
  def repartitionForHdfsFileSize(df: DataFrame, existingFilePath: Path, reducePartitions: Boolean = false)(implicit session:SparkSession): DataFrame = {

    // Use the HDFS blocksize as target size or use the default if it can't be evaluated
    val desiredSize = desiredFileSize(df.sparkSession)

    val fs = getHadoopFsFromSpark(existingFilePath)
    val (numFiles, sumSize, avgSize) = HdfsUtil.sizeInfo(existingFilePath, fs)
    val reduceBy = if(reducePartitions) 2 else 1
    val numPartitionsRequired = Math.max(1,Math.ceil(sumSize.toDouble/desiredSize.toDouble).toInt) / reduceBy
    val currentPartitionNum = df.rdd.getNumPartitions

    logger.debug(s"Current Parquet files: ${numFiles} with a size of ${sumSize}. Requiring ${numPartitionsRequired} partitions now.")

    // Repartition is only done if files exist, otherwise you always end up with one partition
    val dfRepartitioned = if (sumSize > 0 && numPartitionsRequired > currentPartitionNum) {
      logger.debug(s"Executing repartition to ${numPartitionsRequired}")
      df.repartition(numPartitionsRequired)
    } else if(sumSize > 0 && numPartitionsRequired < currentPartitionNum) {
      logger.debug(s"Executing coalesce to ${numPartitionsRequired}")
      df.coalesce(numPartitionsRequired)
    }
    else df

    val adjustedPartitionNum = dfRepartitioned.rdd.getNumPartitions
    logger.debug(s"Repartitioning: Number of RDD partitions before=$currentPartitionNum after=$adjustedPartitionNum")
    dfRepartitioned
  }

  def deletePath( path: Path, fs:FileSystem, doWarn:Boolean ) : Unit = {
    try {
      fs.delete(path, true) // recursive=true
      logger.info(s"Hadoop path ${path} deleted.")
    } catch {
      case e: Exception => if (doWarn) logger.warn(s"Hadoop path ${path} couldn't be deleted (${e.getMessage})")
    }
  }

  /**
   * In contrast to deletePath this supports "globs"
   */
  def deleteFiles( path: Path, fs:FileSystem, doWarn:Boolean ) : Unit = {
    try {
      val deletePaths = fs.globStatus(path).map(_.getPath)
      deletePaths.foreach{ path => fs.delete(path, true) }
      logger.info(s"${deletePaths.size} files delete for hadoop path ${path}.")
    } catch {
      case e: Exception => if (doWarn) logger.warn(s"Hadoop path ${path} couldn't be deleted (${e.getMessage})")
    }
  }

  /**
   * Create default Hadoop Filesystem Authority
   *
   * @return
   */
  def getHadoopDefaultSchemeAuthority(): URI = {
    Environment.hadoopDefaultSchemeAuthority.getOrElse( FileSystem.get(new Configuration()).getUri)
  }

  /**
   * Add default authority to Hadoop Path if not specified
   *
   * @param path path to be extended with authority
   * @return Hadoop Path with authority
   */
  def addHadoopDefaultSchemaAuthority(path: Path): Path = {
    if (path.isAbsoluteAndSchemeAuthorityNull) path.makeQualified(HdfsUtil.getHadoopDefaultSchemeAuthority, null)
    else path
  }

  /**
   * Add scheme, authority and base path to path.
   * Prefix is added if
   * - path is absolute but doesn't have scheme and authority defined, or
   * - path is relativ
   * If after adding prefix path is absolute but scheme and authority is missing, default schema and authority is added.
   *
   * @param path path to be extended with prefix
   * @param prefix prefix to be added if path doesn't contain schema and authority
   * @return Hadoop Path with schema and authority
   */
  def prefixHadoopPath(path: String, prefix: Option[String]): Path = {
    val hadoopPath = new Path(path)
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull || !hadoopPath.isAbsolute) {
      val hadoopPathPrefixed = prefix.map( p => new Path(p + HdfsUtil.addLeadingSeparator(path, Environment.defaultPathSeparator)))
        .getOrElse(hadoopPath)
      HdfsUtil.addHadoopDefaultSchemaAuthority( hadoopPathPrefixed )
    }
    else hadoopPath
  }

  /**
   * Get Hadoop Filesystem from specified Path.
   * Note that use of this is not optimal as there might be additional configurations missing, which are defined in the SparkSession.
   * Use getHadoopFsFromSpark if there is already a SparkSession.
   *
   * @param path
   * @return
   */
  def getHadoopFs(path: Path): FileSystem = {
    path.getFileSystem(new Configuration())
  }

  /**
   * Get Hadoop Filesystem from specified Path with additional Configuration from the SparkSession
   *
   * @param path
   * @return
   */
  def getHadoopFsFromSpark(path: Path)(implicit session: SparkSession): FileSystem = {
    path.getFileSystem(session.sparkContext.hadoopConfiguration)
  }

  def getHadoopFsWithConf(path: Path, hadoopConf: Configuration)(implicit session: SparkSession): FileSystem = {
    path.getFileSystem(hadoopConf)
  }

  def addLeadingSeparator(path: String, separator: Char): String = {
    if (path.startsWith(separator.toString)) path else separator + path
  }

  def getHadoopPartitionLayout(partitionCols: Seq[String], separator: Char): String = {
    partitionCols.map(col => s"$col=%$col%$separator").mkString
  }
}
