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
import io.smartdatalake.config.exporter.ExportType.ExportType
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
    writeFile(document, s"${dataObjectId.id}.schema.json")
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeFile(document, s"${dataObjectId.id}.stats.json")
  }

  override def writeLineage(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeFile(document, s"${dataObjectId.id}.lineage.json")
  }

  override def writeLineageDebug(document: String, dataObjectId: DataObjectId): Unit = {
    writeFile(document, s"${dataObjectId.id}.lineage-debug.txt")
  }

  override def readLatestSchema(dataObjectId: DataObjectId): Option[String] = {
    readLatest(ExportType.Schema, dataObjectId)
  }

  /**
   * Documents are written unversioned, the modification time of the file is used as its only version.
   */
  override def listVersions(tpe: ExportType, dataObjectId: DataObjectId): Seq[Long] = {
    findFile(tpe, dataObjectId).map(f => filesystem.getFileStatus(f).getModificationTime / 1000).toSeq
  }

  override def readVersion(tpe: ExportType, dataObjectId: DataObjectId, version: Long): Option[String] = {
    findFile(tpe, dataObjectId)
      .filter(f => filesystem.getFileStatus(f).getModificationTime / 1000 == version)
      .map(HdfsUtil.readHadoopFile)
  }

  override def readLatest(tpe: ExportType, dataObjectId: DataObjectId): Option[String] = {
    findFile(tpe, dataObjectId).map(HdfsUtil.readHadoopFile)
  }

  override def keepsHistory: Boolean = false

  private def findFile(tpe: ExportType, dataObjectId: DataObjectId): Option[HadoopPath] = {
    Seq(
      new HadoopPath(path, s"${dataObjectId.id}.$tpe.json"),
      // fallback to file names prefixed with "DataObject~", as written by HadoopExportWriter since version 2.9
      new HadoopPath(path, s"$dataObjectId.$tpe.json")
    ).find(filesystem.exists)
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
