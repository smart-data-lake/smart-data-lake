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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.filetransfer.SshUtil
import io.smartdatalake.util.hdfs.{PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.SftpFileRefConnection
import net.schmizz.sshj.sftp.SFTPClient
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * Connects to SFtp files
  * Needs java library "com.hieronymus % sshj % 0.21.1"
  * The following authentication mechanisms are supported
  * -> public/private-key: private key must be saved in ~/.ssh, public key must be registered on server.
  * -> user/pwd authentication: user and password is taken from two variables set as parameters.
  *                             These variables could come from clear text (CLEAR), a file (FILE) or an environment variable (ENV)
  *
  *  @param partitionLayout partition layout defines how partition values can be extracted from the path.
  *                         Use "%<colname>%" as token to extract the value for a partition column.
  *                         With "%<colname:regex>%" a regex can be given to limit search. This is especially useful
  *                         if there is no char to delimit the last token from the rest of the path or also between
  *                         two tokens.
  */
case class SFtpFileRefDataObject(override val id: DataObjectId,
                                 override val path: String,
                                 connectionId: ConnectionId,
                                 override val partitions: Seq[String] = Seq(),
                                 override val partitionLayout: Option[String] = None,
                                 override val metadata: Option[DataObjectMetadata] = None)
                                (@transient implicit val instanceRegistry: InstanceRegistry)
  extends FileRefDataObject with CanCreateInputStream with CanCreateOutputStream with SmartDataLakeLogger {

  /**
   * Connection defines host, port and credentials in central location
   */
  private val connection = getConnection[SftpFileRefConnection](connectionId)

  def execWithSFtpClient[A]( func: SFTPClient => A ): A = connection.execWithSFtpClient(func)

  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[FileRef] = {
    connection.execWithSFtpClient {
      sftp =>
        val paths: Seq[(PartitionValues,String)] = getSearchPaths(partitionValues)
        // search paths and prepare FileRef's
        paths.flatMap{ case (v, p) =>
          logger.debug(s"listing $p")
          SshUtil.sftpListFiles(p)(sftp)
          .map{ f =>
            // check if we have to extract partition values from file path
            val pVs = if (v.keys != partitions.toSet) extractPartitionValuesFromPath(f)
            else v
            FileRef(f, f.reverse.takeWhile(_ != separator).reverse, pVs)
          }
        }
    }
  }

  override def deleteFileRefs(fileRefs: Seq[FileRef])(implicit session: SparkSession): Unit = {
    // delete given files on hdfs
    connection.execWithSFtpClient {
      sftp =>
        fileRefs.foreach { fileRef =>
          sftp.rm(fileRef.fullPath)
        }
    }
  }

  override def createInputStream(path: String)(implicit session: SparkSession): InputStream = {
    Try {
      implicit val sftp = connection.pool.borrowObject
      SshUtil.getInputStream(path, () => Try(connection.pool.returnObject(sftp)))
    } match {
      case Success(r) => r
      case Failure(e) => throw new RuntimeException(s"Can't create InputStream for $id and $path: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
    }
  }

  override def createOutputStream(path: String, overwrite: Boolean)(implicit session: SparkSession): OutputStream = {
    Try {
      implicit val sftp = connection.pool.borrowObject
      SshUtil.getOutputStream(path, () => Try(connection.pool.returnObject(sftp)))
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
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    partitionLayout.map {
      partitionLayout =>
        connection.execWithSFtpClient {
          sftp =>
            // get search pattern for root directory
            val pattern = PartitionLayout.replaceTokens(partitionLayout, PartitionValues(Map()))
            // list directories and extract partition values
            SshUtil.sftpListFiles(path + separator + pattern)(sftp)
              .map( f => PartitionLayout.extractPartitionValues(partitionLayout, "", f + separator))
        }
    }.getOrElse(Seq())
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = SFtpFileRefDataObject
}

object SFtpFileRefDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): SFtpFileRefDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[SFtpFileRefDataObject].value
  }
}