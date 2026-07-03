/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.config.exporter

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}


/**
 * Write documents unversioned to given Hadoop path, overwriting already existing files.
 *
 * @param path base path for writing
 */
case class HadoopExportWriter(path: HadoopPath, hadoopConfig: Configuration = new Configuration()) extends ExportWriter with SmartDataLakeLogger {
  private implicit val filesystem: FileSystem = path.getFileSystem(hadoopConfig)

  override def writeConfig(document: String, version: Option[String]): Unit = {
    writeFile(document, "exportedConfig.json")
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeFile(document, s"${dataObjectId}.schema.json")
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeFile(document, s"${dataObjectId}.stats.json")
  }

  override def readLatestSchema(dataObjectId: DataObjectId): Option[String] = {
    readFile(s"${dataObjectId}.schema.json")
  }

  private def readFile(filename: String): Option[String] = {
    val pathToRead = new HadoopPath(path, filename)
    if (filesystem.exists(pathToRead)) {
      Some(HdfsUtil.readHadoopFile(pathToRead))
    } else None
  }

  private def writeFile(document: String, filename: String): Unit = {
    filesystem.mkdirs(path)
    logger.info(s"Writing $filename")
    HdfsUtil.writeHadoopFile(new HadoopPath(path, filename), document)
    // delete unneeded crc File created by Hadoop local file system...
    if (filesystem.getUri.getScheme == "file") {
      HdfsUtil.deleteFiles(new HadoopPath(path, s".${filename}.crc"), doWarn = false)
    }
  }
}
