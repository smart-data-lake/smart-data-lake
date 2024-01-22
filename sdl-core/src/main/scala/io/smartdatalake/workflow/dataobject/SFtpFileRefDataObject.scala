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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.filetransfer.SshUtil
import io.smartdatalake.util.hdfs.{PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SFtpFileRefConnection
import net.schmizz.sshj.sftp.{SFTPClient, SFTPException}

import java.io.{InputStream, OutputStream}
import java.nio.file.FileAlreadyExistsException
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * Connects to SFtp files
 * Needs java library "com.hieronymus % sshj % 0.21.1"
 * The following authentication mechanisms are supported
 * -> public/private-key: private key must be saved in ~/.ssh, public key must be registered on server.
 * -> user/pwd authentication: user and password is taken from two variables set as parameters.
 *                             These variables could come from clear text (CLEAR), a file (FILE) or an environment variable (ENV)
 *
 * @param partitionLayout partition layout defines how partition values can be extracted from the path.
 *                        Use "%<colname>%" as token to extract the value for a partition column.
 *                        As partition layout extracts partition from the path of individual files, it can also be used to extract partitions from the file name.
 *                        With "%<colname:regex>%" a regex can be given to limit search. This is especially useful
 *                        if there is no char to delimit the last token from the rest of the path or also between
 *                        two tokens.
 *                        Be careful that for directory based partition values extraction, the final path separator must be part
 *                        of the partition layout to extract the last token correctly, e.g. "%year%/" for partitioning with yearly directories.
 * @param saveMode Overwrite or Append new data.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 */
case class SFtpFileRefDataObject(override val id: DataObjectId,
                                 override val path: String,
                                 connectionId: ConnectionId,
                                 override val partitions: Seq[String] = Seq(),
                                 override val partitionLayout: Option[String] = None,
                                 override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                 override val expectedPartitionsCondition: Option[String] = None,
                                 override val metadata: Option[DataObjectMetadata] = None)
                                (@transient implicit val instanceRegistry: InstanceRegistry)
  extends FileRefDataObject with CanCreateInputStream with CanCreateOutputStream with SmartDataLakeLogger {

  /**
   * Connection defines host, port and credentials in central location
   */
  private val connection = getConnection[SFtpFileRefConnection](connectionId)

  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Seq[FileRef] = {
    connection.execWithSFtpClient {
      sftp =>
        val paths: Seq[(PartitionValues,String)] = getSearchPaths(partitionValues)
        // search paths and prepare FileRef's
        paths.flatMap{ case (v, p) =>
          logger.debug(s"listing $p")
          SshUtil.sftpListFiles(p)(sftp)
          .map{ f =>
            // check if we have to extract partition values from file path
            val pVs = if (v.keys != partitions.toSet) extractPartitionValuesFromFilePath(f)
            else v
            FileRef(f, f.reverse.takeWhile(_ != separator).reverse, pVs)
          }
        }
    }
  }

  override def deleteAll(implicit context: ActionPipelineContext): Unit = {
    connection.execWithSFtpClient { sftp =>
      def deleteDirectoryContent(path: String): Unit = {
        val (dirs, files) = sftp.ls(getPath).asScala.partition(_.isDirectory)
        files.foreach(f => sftp.rm(f.getPath))
        dirs.foreach { d =>
          deleteDirectoryContent(d.getPath)
          sftp.rmdir(d.getPath)
        }
      }
      deleteDirectoryContent(getPath)
    }
  }

  /**
   * Get parent directory of path
   */
  private def getParent(path: String) = {
    path.reverse.dropWhile(_ == separator).dropWhile(_ != separator).dropWhile(_ == separator).reverse
  }

  def deletePartitionsContent(partitionValues: Seq[PartitionValues], sftp: SFTPClient)(implicit context: ActionPipelineContext): Seq[String] = {
    val searchPaths = getSearchPaths(partitionValues).map(_._2)
    searchPaths.map { p =>
      val files = SshUtil.sftpListFiles(p)(sftp)
      files.foreach(sftp.rm)
      p
    }
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    connection.execWithSFtpClient { sftp =>
      @tailrec
      def deleteEmptyParents(partitionPath: String): Unit = {
        val parentPath = getParent(partitionPath)
        // check that it is subdirectory of base path
        if (parentPath.startsWith(getPath) && parentPath.length > getPath.length) {
          if (sftp.ls(parentPath).isEmpty) {
            sftp.rmdir(parentPath)
            deleteEmptyParents(parentPath)
          }
        }
      }
      val searchPaths = deletePartitionsContent(partitionValues, sftp)
      val parentDirectories = searchPaths.map(getParent).distinct
      parentDirectories.foreach(deleteEmptyParents)
    }
  }

  override def deleteFile(file: String)(implicit context: ActionPipelineContext): Unit = {
    connection.execWithSFtpClient {
      sftp => sftp.rm(file)
    }
  }

  override def renameFile(file: String, newFile: String)(implicit context: ActionPipelineContext): Unit = {
    connection.execWithSFtpClient {
      sftp => try {
        sftp.rename(file, newFile)
      } catch {
        case e: SFTPException if e.getMessage == "File/Directory already exists" => throw new FileAlreadyExistsException(e.getMessage)
      }
    }
  }

  override def mkDirs(path: String)(implicit context: ActionPipelineContext): Unit = {
    connection.execWithSFtpClient {
      sftp => sftp.mkdirs(path)
    }
  }

  override def createInputStream(path: String)(implicit context: ActionPipelineContext): InputStream = {
    Try {
      implicit val sftp = connection.pool.borrowObject
      SshUtil.getInputStream(path, () => Try(connection.pool.returnObject(sftp)))
    } match {
      case Success(r) => r
      case Failure(e) => throw new RuntimeException(s"Can't create InputStream for $id and $path: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  override def startWritingOutputStreams(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): Unit = {
    // ensure base and partition directories are existing
    mkDirs(path)
    val searchPaths = getSearchPaths(partitionValues)
    val partitionDirectories = searchPaths.map(p => getParent(p._2))
    partitionDirectories.foreach(mkDirs)
    // cleanup on overwrite
    if (saveMode == SDLSaveMode.Overwrite) {
      if (partitions.nonEmpty)
        if (partitionValues.nonEmpty) connection.execWithSFtpClient { sftp =>
          deletePartitionsContent(partitionValues, sftp)
        } else logger.warn(s"($id) Cannot delete data from partitioned data object as no partition values are given but saveMode=overwrite")
      else deleteAll
    }
  }

  override def endWritingOutputStreams(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    // make sure empty partitions are created as well
    if (partitionValues.nonEmpty) createMissingPartitions(partitionValues)
  }

  override def createOutputStream(path: String, overwrite: Boolean)(implicit context: ActionPipelineContext): OutputStream = {
    Try {
      implicit val sftp = connection.pool.borrowObject
      SshUtil.getOutputStream(path, overwrite, () => Try(connection.pool.returnObject(sftp)))
    } match {
      case Success(r) => r
      case Failure(e) => throw new RuntimeException(s"Can't create OutputStream for $id and $path: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  /**
   * extract partitions according to partition layout
   */
  /**
   * List partitions on data object's root path
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    partitionLayout.map {
      partitionLayout =>
        connection.execWithSFtpClient {
          sftp =>
            // get search pattern for root directory
            val pattern = PartitionLayout.replaceTokens(partitionLayout, PartitionValues(Map()))
            // list directories and extract partition values
            SshUtil.sftpListFiles(path + separator + pattern)(sftp)
              .map(extractPartitionValuesFromFilePath)
        }
    }.getOrElse(Seq())
  }

  override def relativizePath(filePath: String)(implicit context: ActionPipelineContext): String = {
    filePath.stripPrefix(path+separator)
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = try {
    connection.test()
  } catch {
    case ex: Throwable => throw ConnectionTestException(s"($id) Can not connect. Error: ${ex.getMessage}", ex)
  }

  override def recommendedParallelism: Option[Int] = Some(connection.maxParallelConnections)

  override def factory: FromConfigFactory[DataObject] = SFtpFileRefDataObject
}

object SFtpFileRefDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SFtpFileRefDataObject = {
    extract[SFtpFileRefDataObject](config)
  }
}