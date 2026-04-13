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
package io.smartdatalake.workflow.dataobject.file

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.{Environment, SDLSaveMode, TableStatsType}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.HadoopFileConnection
import org.apache.hadoop.fs._

import java.io.{FileNotFoundException, InputStream, OutputStream}
import scala.util.{Failure, Success, Try}

/**
 * A [[DataObject]] backed by a file in HDFS.
 *
 * Provides access to resources on local or distribute (remote) file systems supported by Apache Hadoop.
 * This includes normal disk access, FTP, and Hadoop Distributed File System (HDFS).
 *
 * @see [[FileSystem]]
 */
private[smartdatalake] trait HadoopFileDataObject extends FileRefDataObject with CanCreateInputStream with CanCreateOutputStream with HasHadoopStandardFilestore with SmartDataLakeLogger {

  /**
   * Return the [[InstanceRegistry]] parsed from the SDL configuration used for this run.
   *
   * @return the current [[InstanceRegistry]].
   */
  def instanceRegistry(): InstanceRegistry

  /**
   * Return the connection id.
   *
   * Connection defines path prefix (scheme, authority, base path) and ACL's in central location.
   */
  def connectionId(): Option[ConnectionId]

  protected val connection: Option[HadoopFileConnection] = connectionId().map {
    c => getConnectionReg[HadoopFileConnection](c, instanceRegistry())
  }

  // these variables are not serializable
  @transient private var hadoopPathHolder: Path = _

  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    if (hadoopPathHolder == null) { // avoid null-pointer on executors...
      hadoopPathHolder = HdfsUtil.prefixHadoopPath(path, connection.map(_.pathPrefix))
    }
    hadoopPathHolder
  }

  override def getPath(implicit context: ActionPipelineContext): String = hadoopPath.toUri.toString

  /**
   * Check if the input files exist.
   * Note that hadoopDir can be a specific file or a directory.
   */
  def checkFilesExisting(recursive: Boolean = false)(implicit context: ActionPipelineContext): Boolean = {
    val status = try {
      filesystem.getFileStatus(hadoopPath)
    } catch {
      case _: FileNotFoundException => return false
    }
    status.isFile || (status.isDirectory && listDataFiles(recursive = recursive).nonEmpty)
  }

  def listDataFiles(pv: PartitionValues = PartitionValues(Map()), recursive: Boolean = false)(implicit context: ActionPipelineContext): Iterator[Path] = {
    val partitionPattern = if (partitions.nonEmpty) Some(new GlobPattern(new Path(pv.getPartitionString(partitionLayout().get), fileName).toString)) else None
    val fileNamePattern = new GlobPattern(fileName)
    val filterFun = (status: FileStatus) => if (status.isDirectory) !status.getPath.toString.startsWith(".") else fileNamePattern.matches(status.getPath.getName)
    HdfsUtil.listFiles(hadoopPath, recursive || partitions.nonEmpty, filterFun)(filesystem)
      .map(_.getPath)
      .filter(p => partitionPattern.forall(_.matches(relativizePath(p.toString)))) // filter files matching partition pattern if defined
  }

  def listPartitionPathsStatus(pv: PartitionValues = PartitionValues(Map()), partitionLayoutParam: String = partitionLayout().get)(implicit context: ActionPipelineContext): Seq[FileStatus] = {
    assert(partitions.nonEmpty)
    val pathPattern = new Path(hadoopPath, pv.getPartitionString(partitionLayoutParam))
    filesystem.globStatus(pathPattern).filter(_.isDirectory).toSeq
  }

  def listPartitionPaths(pv: PartitionValues = PartitionValues(Map()), partitionLayoutParam: String = partitionLayout().get)(implicit context: ActionPipelineContext): Seq[Path] = {
    listPartitionPathsStatus(pv, partitionLayoutParam).map(_.getPath)
  }

  override def deleteFile(file: String)(implicit context: ActionPipelineContext): Unit = {
    deleteFile(new Path(file))
  }

  override def renameFile(file: String, newFile: String)(implicit context: ActionPipelineContext): Unit = {
    import java.nio.file.{FileAlreadyExistsException => JavaFileAlreadyExistsException}
    try {
      HdfsUtil.renamePath(new Path(file), new Path(newFile))(filesystem)
    } catch {
      case e: FileAlreadyExistsException => throw new JavaFileAlreadyExistsException(e.getMessage)
    }
  }

  def deleteFile(file: Path)(implicit context: ActionPipelineContext): Unit = {
    logger.debug(s"($id) deleteFile $file")
    if (!filesystem.delete(file, false)) { // recursive=false
      logger.warn(s"($id) deleting $file failed!")
    }
  }

  /**
   * Delete Hadoop Partitions.
   *
   * if there is no value for a partition column before the last partition column given, the partition path will be exploded
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    assert(partitions.nonEmpty, s"deletePartitions called but no partition columns are defined for $id")

    // delete given partitions on hdfs
    val pathsToDelete = partitionValues.flatMap(getConcreteInitPaths)
    pathsToDelete.foreach(filesystem.delete(_, /*recursive*/ true))
  }

  /**
   * Delete files inside Hadoop Partitions, but keep partition directory to preserve ACLs
   *
   * if there is no value for a partition column before the last partition column given, the partition path will be exploded
   */
  def deletePartitionsFiles(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    assert(partitions.nonEmpty, s"deletePartitions called but no partition columns are defined for $id")

    // delete files for given partitions on hdfs
    val pathsToDelete = partitionValues.flatMap(getConcreteInitPaths)
    pathsToDelete.foreach(deleteAllFiles)
  }

  /**
   * Generate all "init"-paths for given partition values exploding undefined partitions.
   * An "init"-path contains only the partitions before the last defined partition value.
   * Use case: Reading all files from a given path with Sparks DataFrameReader, the path can not contain wildcards.
   * If there are partitions without given partition value before the last partition value given, they must be searched with globs.
   */
  def getConcreteInitPaths(pv: PartitionValues)(implicit context: ActionPipelineContext): Seq[Path] = {
    assert(partitions.nonEmpty)
    // check if valid init of partitions -> then we can read all at once, otherwise we need to search with globs as DataFrameReader.load doesnt support wildcards
    if (pv.isInitOf(partitions)) {
      val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partitions.filter(pv.isDefinedAt))
      Seq(new Path(hadoopPath, pv.getPartitionString(partitionLayout)))
    } else {
      // prepare partitions to include in path search
      val partitionsToInclude = partitions.reverse.dropWhile(!pv.keys.contains(_)).reverse // get all partition columns until last given partition value
      val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partitionsToInclude)
      logger.info(s"($id) getConcretePaths with globs needed because ${pv.keys.mkString(",")} is not an init of partition columns ${partitions.mkString(",")}")
      listPartitionPaths(pv, partitionLayout)
    }
  }

  /**
   * Generate all paths for given partition values exploding undefined partitions.
   * In contrast to getConcreteInitPaths this method explodes the values for all partitions.
   * If returnFiles is set, it will return files matching partition directories + filename instead of the partition directories.
   */
  def getConcreteFullPaths(pv: PartitionValues, returnFiles: Boolean = false)(implicit context: ActionPipelineContext): Seq[Path] = {
    assert(partitions.nonEmpty)
    // check partitions completely defined -> then we can read all at once, otherwise we need to search with globs as DataFrameReader.load doesnt support wildcards
    if (pv.isComplete(partitions)) {
      if (returnFiles) listDataFiles(pv).toSeq
      else Seq(new Path(hadoopPath, pv.getPartitionString(partitionLayout().get)))
    } else {
      logger.info(s"($id) getConcretePaths with globs needed because ${pv.keys.mkString(",")} does not define all partition columns ${partitions.mkString(",")}")
      if (returnFiles) listDataFiles(pv).toSeq
      else listPartitionPaths(pv)
    }
  }

  /**
   * List partitions on data object's root path
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    getPartitionPathsStatus
      .map(s => extractPartitionValuesFromDirPath(s.getPath.toString))
  }

  def getPartitionPathsStatus(implicit context: ActionPipelineContext): Seq[FileStatus] = {
    if (partitions.nonEmpty) listPartitionPathsStatus()
    else Seq()
  }

  override def relativizePath(path: String)(implicit context: ActionPipelineContext): String = {
    val normalizedPath = new Path(path).toString
    val pathPrefix = (".*" + hadoopPath.toString).r // ignore any absolute path prefix up and including hadoop path
    pathPrefix.replaceFirstIn(normalizedPath, "").stripPrefix(Path.SEPARATOR)
  }

  override def concatPath(parent: String, child: String): String = {
    new Path(parent, child).toString
  }

  override def isAbsolutePath(path: String): Boolean = {
    new Path(path).isAbsolute
  }

  override def createEmptyPartition(partitionValues: PartitionValues)(implicit context: ActionPipelineContext): Unit = {
    // check if valid init of partitions -> otherwise we can not create empty partition as path is not fully defined
    if (partitionValues.isInitOf(partitions)) {
      val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partitions.filter(partitionValues.isDefinedAt))
      val partitionPath = new Path(hadoopPath, partitionValues.getPartitionString(partitionLayout))
      filesystem.mkdirs(partitionPath)
    } else {
      logger.info(s"($id) can not createEmptyPartition for $partitionValues as ${partitionValues.keys.mkString(",")} is not an init of partition columns ${partitions.mkString(",")}")
    }
  }

  override def movePartitions(partitionValuesMapping: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    partitionValuesMapping.foreach {
      case (pvExisting, pvNew) => HdfsUtil.movePartition(hadoopPath, pvExisting, pvNew, fileName)(filesystem)
    }
    logger.info(s"($id) Archived partitions ${partitionValuesMapping.map(m => s"${m._1}->${m._2}").mkString(", ")}")
  }

  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Seq[FileRef] = {
    val paths: Seq[(PartitionValues, String)] = getSearchPaths(partitionValues)
    // search paths and prepare FileRef's
    paths.flatMap { case (v, p) =>
      logger.debug(s"listing $p")
      filesystem.globStatus(new Path(p))
        .map { f =>
          // check if we have to extract partition values from file path
          val pVs = if (!v.isComplete(partitions)) extractPartitionValuesFromFilePath(f.getPath.toString)
          else v
          FileRef(f.getPath.toString, f.getPath.getName, pVs)
        }
    }
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    hadoopPath // initialize hadoopPath
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
  }

  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
  }

  override def createInputStreams(path: String)(implicit context: ActionPipelineContext): Iterator[InputStream] = {
    Try(filesystem.open(new Path(path))) match {
      case Success(r) => Iterator(r)
      case Failure(e) => throw new RuntimeException(s"Can't create InputStream for $id and $path: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  override def startWritingOutputStreams(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): Unit = {
    if (saveMode == SDLSaveMode.Overwrite) {
      if (partitions.nonEmpty)
        if (partitionValues.nonEmpty) deletePartitions(partitionValues)
        else logger.warn(s"($id) Cannot delete data from partitioned data object as no partition values are given but saveMode=overwrite")
      else deleteAll
    }
  }

  override def createOutputStream(path: String, overwrite: Boolean)(implicit context: ActionPipelineContext): OutputStream = {
    Try(filesystem.create(new Path(path), overwrite)) match {
      case Success(r) => r
      case Failure(e) => throw new RuntimeException(s"Can't create OutputStream for $id and $path: : ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  override def endWritingOutputStreams(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    // make sure empty partitions are created as well
    if (partitionValues.nonEmpty) createMissingPartitions(partitionValues)
  }

  override def deleteAll(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) deleteAll $hadoopPath")
    filesystem.delete(hadoopPath, true) // recursive=true
  }

  /**
   * delete all files inside given path recursively, but keep main directory
   */
  def deleteAllFiles(path: Path)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) deleteAllFiles $path")
    val dirEntries = HdfsUtil.listFiles(path, recursive = false)(filesystem)
    dirEntries.foreach { s =>
      if (s.isDirectory) filesystem.delete(hadoopPath, /*recursive*/ true)
      else filesystem.delete(s.getPath, false)
    }
  }

  def extractPartitionValuesFromDirPath(dirPath: String)(implicit context: ActionPipelineContext): PartitionValues = {
    PartitionLayout.extractPartitionValues(partitionLayout().get, relativizePath(dirPath) + separator)
  }

  override def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      HdfsUtil.getPathStats(hadoopPath)(filesystem) ++ getPartitionStats
    } catch {
      case e:Exception => Map(TableStatsType.Info.toString -> e.getMessage)
    }
  }
}
