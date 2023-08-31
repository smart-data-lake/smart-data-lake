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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{ResourceUtil, SmartDataLakeLogger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.IOException
import java.net.URI
import scala.collection.AbstractIterator
import scala.io.{Codec, Source}
import scala.util.{Try, Using}

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
  def sizeInfo(path: Path)(implicit fs: FileSystem): (Long, Long, Long) = {
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
  def desiredFileSize(implicit hadoopConf: Configuration): Long = {
    hadoopConf.getLong("dfs.blocksize", DefaultBlocksize)
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
  def repartitionForHdfsFileSize(df: DataFrame, existingFilePath: Path, reducePartitions: Boolean = false): DataFrame = {

    // Use the HDFS blocksize as target size or use the default if it can't be evaluated
    val desiredSize = desiredFileSize(df.sparkSession.sparkContext.hadoopConfiguration)

    implicit val fs: FileSystem = getHadoopFsFromSpark(existingFilePath)(df.sparkSession)
    val (numFiles, sumSize, avgSize) = HdfsUtil.sizeInfo(existingFilePath)
    val reduceBy = if(reducePartitions) 2 else 1
    val numPartitionsRequired = Math.max(1,Math.ceil(sumSize.toDouble / desiredSize / reduceBy).toInt)
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

  def deletePath( path: Path, doWarn:Boolean )(implicit fs: FileSystem) : Unit = {
    try {
      fs.delete(path, true) // recursive=true
      logger.info(s"Hadoop path ${path} deleted.")
    } catch {
      case e: Exception => if (doWarn) logger.warn(s"Hadoop path ${path} couldn't be deleted (${e.getMessage})")
    }
  }

  /**
   * Deletes parent directories of path if they are empty.
   * Handles all parent directories up-to base path.
   * Stops if a not empty directory is found.
   */
  def deleteEmptyParentPath( path: Path, basePath: Path )(implicit fs: FileSystem) : Unit = {
    assert(isSubdirectory(path, basePath), s"$path is not a subdirectory of $basePath")
    val parentPath = path.getParent
    if (parentPath.depth() > basePath.depth) {
      if (fs.exists(parentPath) && fs.listStatus(parentPath).isEmpty) {
        fs.delete(parentPath, false) // recursive=false
        logger.info(s"Hadoop path ${parentPath} deleted.")
      }
      deleteEmptyParentPath(parentPath, basePath)
    }
  }

  /**
   * Check if subPath is a subdirectory of path
   */
  def isSubdirectory( subPath: Path, path: Path): Boolean = {
    if (subPath.depth() <= path.depth()) false
    else {
      val subPathParent = subPath.getParent
      if (subPathParent.depth() == path.depth()) subPathParent == path
      else isSubdirectory(subPathParent, path)
    }
  }

  /**
   * In contrast to deletePath this supports "globs"
   */
  def deleteFiles(path: Path, doWarn:Boolean)(implicit fs: FileSystem) : Unit = {
    try {
      val pathsToDelete = fs.globStatus(path).map(_.getPath)
      pathsToDelete.foreach{ path => fs.delete(path, true) }
      logger.info(s"${pathsToDelete.length} files deleted for hadoop path $path.")
    } catch {
      case e: Exception => if (doWarn) logger.warn(s"Hadoop path $path couldn't be deleted: (${e.getMessage})")
    }
  }

  /**
   * Rename single path as one hadoop operation (note it depends on the implementation if this is atomic).
   */
  def renamePath(path: Path, newPath: Path)(implicit fs: FileSystem ): Unit = {
    // check if file already exists before rename, as error of fs.rename is not detailled enough (depending on hadoop fs implementation)
    val fileStat = Try(fs.getFileStatus(newPath))
    if (fileStat.isSuccess && fileStat.get.isFile) throw new FileAlreadyExistsException(s"Rename $path to $newPath failed. New file already exists.")
    if (fs.rename(path, newPath)) {
      logger.debug(s"Path $path renamed to $newPath")
    } else new IOException(s"Rename $path to $newPath failed. Reason unknown.")
  }

  /**
   * Move/rename path supporting "globs".
   * New path must be a directory. If not existing it will be created (also if there are no files to move).
   */
  def moveFiles(path: Path, newPath: Path, failOnError: Boolean = true, customFilter: (FileStatus => Boolean) = _ => true, addPrefixIfExisting: Boolean = false )(implicit fs: FileSystem): Unit = {
    try {
      val fileStat = Try(fs.getFileStatus(newPath))
      if (fileStat.isFailure) fs.mkdirs(newPath)
      else if (!fileStat.get.isDirectory) throw new RuntimeException(s"moveFile: new path $newPath must be a directory")
      val pathsToMove = fs.globStatus(path).toSeq.filter(_.isFile).filter(customFilter).map(_.getPath)
      def getParentHash(path: Path) = Integer.toHexString(path.getParent.hashCode())
      pathsToMove.foreach{ path =>
        try {
          renamePath(path, new Path(newPath, path.getName))
        } catch {
          // it's possible that files have the same name in different directories. Rename files adds hash of parent as prefix of filename in those cases.
          case _:FileAlreadyExistsException if (addPrefixIfExisting) =>
            renamePath(path, new  Path(newPath, s"${getParentHash(path)}-${path.getName}"))
        }
      }
      logger.info(s"${pathsToMove.size} files moved from $path to $newPath")
    } catch {
      case e: Exception if (!failOnError) => logger.warn(s"Hadoop path $path couldn't be moved to $newPath: (${e.getMessage})")
    }
  }

  /**
   * Create default Hadoop Filesystem Authority
   */
  def getHadoopDefaultSchemeAuthority: URI = {
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
      val hadoopPathPrefixed = prefix.map( p => new Path(p, path))
        .getOrElse(hadoopPath)
      HdfsUtil.addHadoopDefaultSchemaAuthority( hadoopPathPrefixed )
    }
    else hadoopPath
  }

  /**
   * If path is not absolute, prefix with working dir
   * @return Absolute hadoop path
   */
  def makeAbsolutePath(path: Path)(implicit filesystem: FileSystem): Path = {
    if (path.isAbsolute) path
    else new Path(filesystem.getWorkingDirectory, path)
  }

  /**
   * Get Hadoop Filesystem from specified Path with default Hadoop configuration.
   * Note that use of this is not optimal as there might be additional configurations missing, which are defined in the SparkSession.
   * Use getHadoopFsFromSpark if there is already a SparkSession.
   */
  def getHadoopFsWithDefaultConf(path: Path): FileSystem = {
    getHadoopFsWithConf(path)(new Configuration())
  }

  /**
   * Get Hadoop Filesystem from specified Path with additional Configuration from the SparkSession
   */
  def getHadoopFsFromSpark(path: Path)(implicit session: SparkSession): FileSystem = {
    getHadoopFsWithConf(path)(session.sparkContext.hadoopConfiguration)
  }

  /**
   * Get Hadoop Filesystem from specified Path with given Hadoop Configuration
   */
  def getHadoopFsWithConf(path: Path)(implicit hadoopConf: Configuration): FileSystem = {
    Environment.fileSystemFactory.getFileSystem(path, hadoopConf)
  }

  def getHadoopPartitionLayout(partitionCols: Seq[String]): String = {
    partitionCols.map(col => s"$col=%$col%${Path.SEPARATOR_CHAR}").mkString
  }

  def readHadoopFile(file: String)(implicit hadoopConf: Configuration): String = {
    val path = addHadoopDefaultSchemaAuthority(new Path(file))
    val filesystem = getHadoopFsWithConf(path)
    readHadoopFile(path)(filesystem)
  }

  def readHadoopFile(file: Path)(implicit filesystem: FileSystem): String = {
    Using.resource(filesystem.open(file)) { is =>
      Source.fromInputStream(is)(Codec.UTF8).getLines.mkString(sys.props("line.separator"))
    }
  }

  def writeHadoopFile(file: Path, content: String)(implicit filesystem: FileSystem): Unit = {
    Using.resource(filesystem.create(file, true)) { os =>
      os.write(content.getBytes(Codec.UTF8.name))
    }
  }

  def appendHadoopFile(file: Path, content: String)(implicit filesystem: FileSystem): Unit = {
    Using.resource(filesystem.append(file)) { os =>
      os.write(content.getBytes(Codec.UTF8.name))
    }
  }

  def movePartition(basePath: Path, existingPartition: PartitionValues, newPartition: PartitionValues, filenameWithGlobs: String)(implicit filesystem: FileSystem): Unit = {
    val partitionLayout = getHadoopPartitionLayout(existingPartition.keys.toSeq)
    val existingPartitionPath = new Path(basePath, existingPartition.getPartitionString(partitionLayout))
    val existingPartitionPathWithFilenameGlobs = new Path(existingPartitionPath, filenameWithGlobs)
    val newPartitionPath = new Path(basePath, newPartition.getPartitionString(partitionLayout))
    moveFiles( existingPartitionPathWithFilenameGlobs, newPartitionPath, addPrefixIfExisting = true)
    deletePath(existingPartitionPath, doWarn = true)
    deleteEmptyParentPath(existingPartitionPath, basePath)
  }

  def touchFile(path: Path)(implicit filesystem: FileSystem): Unit = {
    Using.resource(filesystem.create(path, /*overwrite*/ true))(_ => Unit)
  }

  /**
   * Check if a folder is writable by creating a test file in given path and deleting it again
   */
  def writeTest(path: Path, filename: String = System.currentTimeMillis.toString)(implicit filesystem: FileSystem): Unit = {
    val file = new Path(path, filename)
    touchFile(file)
    filesystem.delete(file, true) // recursive=true
  }

  /**
   * Wrapper for Hadoop RemoteIterator to use it with Scala style
   */
  case class RemoteIteratorWrapper[T](underlying: RemoteIterator[T]) extends AbstractIterator[T] with Iterator[T] {
    def hasNext: Boolean = underlying.hasNext
    def next(): T = underlying.next()
  }
}
