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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.{Environment, SDLSaveMode}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.{AclDef, AclUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.HadoopFileConnection
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, FileSystem, Path}

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
   * Return the ACL definition for the Hadoop path of this DataObject
   *
   * @see [[org.apache.hadoop.fs.permission.AclEntry]]
   */
  def acl(): Option[AclDef]

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
  private[smartdatalake] def checkFilesExisting(implicit context: ActionPipelineContext): Boolean = {
    val status = try {
      filesystem.getFileStatus(hadoopPath)
    } catch {
      case _: FileNotFoundException => return false
    }
    status.isFile || {
      val globPath = if (partitions.nonEmpty) new Path(hadoopPath, PartitionValues(Map()).getPartitionString(partitionLayout().get)) else hadoopPath
      status.isDirectory && filesystem.globStatus(new Path(globPath, fileName)).exists(_.isFile)
    }
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
      // create path with wildcards
      val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partitionsToInclude)
      val globPartitionPath = new Path(hadoopPath, pv.getPartitionString(partitionLayout))
      logger.info(s"($id) getConcretePaths with globs needed because ${pv.keys.mkString(",")} is not an init of partition columns ${partitions.mkString(",")}")
      filesystem.globStatus(globPartitionPath).filter(_.isDirectory).map(_.getPath)
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
      val path = new Path(hadoopPath, pv.getPartitionString(partitionLayout().get))
      if (returnFiles) filesystem.globStatus(new Path(path, fileName)).filter(_.isFile).map(_.getPath).toSeq
      else Seq(path)
    } else {
      // create path with wildcards
      val globPartitionPath = new Path(hadoopPath, pv.getPartitionString(partitionLayout().get))
      logger.info(s"($id) getConcretePaths with globs needed because ${pv.keys.mkString(",")} does not define all partition columns ${partitions.mkString(",")}")
      if (returnFiles) filesystem.globStatus(new Path(globPartitionPath, fileName)).filter(_.isFile).map(_.getPath)
      else filesystem.globStatus(globPartitionPath).filter(_.isDirectory).map(_.getPath)
    }
  }

  /**
   * List partitions on data object's root path
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    getPartitionPathStatis
      .map(path => extractPartitionValuesFromDirPath(path.getPath.toString))
  }

  def getPartitionPathStatis(implicit context: ActionPipelineContext): Seq[FileStatus] = {
    partitionLayout().map {
      partitionLayout =>
        // get search pattern for root directory
        val pattern = PartitionLayout.replaceTokens(partitionLayout, PartitionValues(Map()))
        // list directories and extract partition values
        filesystem.globStatus(new Path(hadoopPath, pattern))
          .filter { fs => fs.isDirectory }
          .toSeq
    }.getOrElse(Seq())
  }

  override def relativizePath(path: String)(implicit context: ActionPipelineContext): String = {
    val normalizedPath = new Path(path).toString
    val pathPrefix = (".*" + hadoopPath.toString).r // ignore any absolute path prefix up and including hadoop path
    pathPrefix.replaceFirstIn(normalizedPath, "").stripPrefix(Path.SEPARATOR)
  }

  override def concatPath(parent: String, child: String): String = {
    new Path(parent, child).toString
  }

  override def isAbsolutePath(path: String) = {
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
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists(a => filesystem.getUri.toString.contains(a))) {
      require(acl().isDefined, s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
    applyAcls
  }

  override def createInputStream(path: String)(implicit context: ActionPipelineContext): InputStream = {
    Try(filesystem.open(new Path(path))) match {
      case Success(r) => r
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
   * delete all files inside given path recursively
   */
  def deleteAllFiles(path: Path)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) deleteAllFiles $path")
    val dirEntries = filesystem.globStatus(new Path(path, "*")).map(_.getPath)
    dirEntries.foreach { p =>
      if (filesystem.isDirectory(p)) deleteAllFiles(p)
      else filesystem.delete(p, /*recursive*/ false)
    }
  }

  protected[workflow] def applyAcls(implicit context: ActionPipelineContext): Unit = {
    val aclToApply = acl().orElse(connection.flatMap(_.acl))
    if (aclToApply.isDefined) AclUtil.addACLs(aclToApply.get, hadoopPath)(filesystem)
  }

  def extractPartitionValuesFromDirPath(dirPath: String)(implicit context: ActionPipelineContext): PartitionValues = {
    PartitionLayout.extractPartitionValues(partitionLayout().get, relativizePath(dirPath) + separator)
  }
}
