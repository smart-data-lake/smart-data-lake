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

import java.io.{InputStream, OutputStream}

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.{AclDef, AclUtil, SerializableHadoopConfiguration, SmartDataLakeLogger}
import io.smartdatalake.workflow.connection.HadoopFileConnection
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
 * A [[DataObject]] backed by a file in HDFS.
 *
 * Provides access to resources on local or distribute (remote) file systems supported by Apache Hadoop.
 * This includes normal disk access, FTP, and Hadoop Distributed File System (HDFS).
 *
 * @see [[FileSystem]]
 */
private[smartdatalake] trait HadoopFileDataObject extends FileRefDataObject with CanCreateInputStream with CanCreateOutputStream with SmartDataLakeLogger {

  /**
   * Return the [[InstanceRegistry]] parsed from the SDL configuration used for this run.
   *
   * @return  the current [[InstanceRegistry]].
   */
  def instanceRegistry(): InstanceRegistry

  /**
   * Return a [[String]] specifying the partition layout.
   *
   * For Hadoop the default partition layout is colname1=<value1>/colname2=<value2>/.../
   */
  final override def partitionLayout(): Option[String] = {
    if (partitions.nonEmpty) {
      Some(HdfsUtil.getHadoopPartitionLayout(partitions, separator))
    } else {
      None
    }
  }

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


  /**
   * Configure whether [[io.smartdatalake.workflow.action.Action]]s should fail if the input file(s) are missing
   * on the file system.
   *
   * Default is false.
   */
  def failIfFilesMissing: Boolean = false

  // these variables are not serializable
  @transient private[workflow] lazy val hadoopPath = HdfsUtil.prefixHadoopPath(path, connection.map(_.pathPrefix))
  @transient private var filesystemHolder: FileSystem = _
  private var serializableHadoopConf: SerializableHadoopConfiguration = _ // we must serialize hadoop config for CustomFileAction running transformation on executors

  /**
   * Create a hadoop [[FileSystem]] API handle for the provided [[SparkSession]].
   */
  def filesystem(implicit session: SparkSession): FileSystem = {
    if (serializableHadoopConf == null) {
      serializableHadoopConf = new SerializableHadoopConfiguration(session.sparkContext.hadoopConfiguration)
    }
    if (filesystemHolder == null) {
      filesystemHolder = HdfsUtil.getHadoopFsWithConf(hadoopPath, serializableHadoopConf.get)
    }
    filesystemHolder
  }

  /**
   * Check if the input files exist.
   *
   * @throws IllegalArgumentException if `failIfFilesMissing` = true and no files found at `path`.
   */
  protected def checkFilesExisting(implicit session:SparkSession): Boolean = {
    val files = if (filesystem.exists(hadoopPath.getParent)) {
      arrayToSeq(filesystem.globStatus(hadoopPath))
    } else {
      Seq.empty
    }

    if (files.isEmpty) {
      logger.warn(s"($id) No files found at $hadoopPath. Can not import any data.")
      require(!failIfFilesMissing, s"($id) failIfFilesMissing is enabled and no files to process have been found in $hadoopPath.")
    }

    files.nonEmpty
  }
  def arrayToSeq[T](arr: Array[T]): Seq[T] = if (arr!=null) arr.toSeq else Seq()

  override def deleteFileRefs(fileRefs: Seq[FileRef])(implicit session:SparkSession): Unit = {
    // delete given files on hdfs
    fileRefs.foreach { _ =>
      filesystem.delete(new Path(path), false) // recursive=false
    }
  }

  /**
   * Delete Hadoop Partitions.
   *
   * Note that this is only possible, if every set of column names in `partitionValues` are valid "inits"
   * of this [[DataObject]]'s `partitions`.
   *
   * Every valid "init" can be produced by repeatedly removing the last element of a collection.
   * Example:
   * - a,b of a,b,c -> OK
   * - a,c of a,b,c -> NOK
   *
   * @see [[scala.collection.TraversableLike.init]]
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    assert(partitions.nonEmpty, s"deletePartitions called but no partition columns are defined for $id")

    // delete given partitions on hdfs
    partitionValues.map { pv =>
      // check if valid init of partitions
      assert(partitions.inits.map(_.toSet).contains(pv.keys), s"${pv.keys.mkString(",")} is not a valid init of ${partitions.mkString(",")}")
      val partitionLayout = HdfsUtil.getHadoopPartitionLayout(partitions.filter(pv.isDefinedAt), separator)
      val partitionPath = new Path(hadoopPath, pv.getPartitionString(partitionLayout))
      filesystem.delete(partitionPath, true) // recursive=true
    }
  }

  /**
   * List partitions on data object's root path
   */
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    partitionLayout().map {
      partitionLayout =>
        // get search pattern for root directory
        val pattern = PartitionLayout.replaceTokens(partitionLayout, PartitionValues(Map()))
        // list directories and extract partition values
        filesystem.globStatus( new Path(hadoopPath, pattern))
          .filter{fs => fs.isDirectory}
          .map(_.getPath.toString)
          .map( path => PartitionLayout.extractPartitionValues(partitionLayout, "", path + separator))
          .toSeq
    }.getOrElse(Seq())
  }

  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[FileRef] = {
    val paths: Seq[(PartitionValues,String)] = getSearchPaths(partitionValues)
    // search paths and prepare FileRef's
    paths.flatMap{ case (v, p) =>
      logger.debug(s"listing $p")
      filesystem.globStatus( new Path(p))
        .map{ f =>
          // check if we have to extract partition values from file path
          val pVs = if (v.keys != partitions.toSet) extractPartitionValuesFromPath(f.getPath.toString)
          else v
          FileRef(f.getPath.toString, f.getPath.getName, pVs)
        }
    }
  }

  override def postWrite(implicit session: SparkSession): Unit = {
    super.postWrite
    applyAcls
  }

  override def createInputStream(path: String)(implicit session: SparkSession): InputStream = {
    Try(filesystem.open(new Path(path))) match {
      case Success(r) => r
      case Failure(e) => throw new RuntimeException(s"Can't create InputStream for $id and $path: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  override def createOutputStream(path: String, overwrite: Boolean)(implicit session: SparkSession): OutputStream = {
    Try(filesystem.create(new Path(path), overwrite)) match {
      case Success(r) => r
      case Failure(e) => throw new RuntimeException(s"Can't create OutputStream for $id and $path: : ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  override def deleteAll(implicit session: SparkSession): Unit = {
    filesystem.delete(hadoopPath, true) // recursive=true
  }

  protected[workflow] def applyAcls(implicit session: SparkSession): Unit = {
    val aclToApply = acl().orElse(connection.flatMap(_.acl))
    if (aclToApply.isDefined) AclUtil.addACLs(acl().get, hadoopPath)(filesystem)
  }
}
