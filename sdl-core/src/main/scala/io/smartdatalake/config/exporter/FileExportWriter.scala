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
import io.smartdatalake.util.misc.FileUtil.readFile
import io.smartdatalake.util.misc.SmartDataLakeLogger

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using


/**
 * Write documents versioned to given local file system path, appending filenames to index files for local UI.
 *
 * @param path base path for writing
 */
case class FileExportWriter(path: Path) extends ExportWriter with SmartDataLakeLogger {

  override def writeConfig(document: String, version: Option[String]): Unit = {
    writeFile(document, "config")
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeWithIndex(document, dataObjectId, "schema", version)
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeWithIndex(document, dataObjectId, "stats", version)
  }

  override def writeLineage(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeWithIndex(document, dataObjectId, "lineage", version)
  }

  /**
   * The debug output is written unversioned and is not added to an index, as it is read by a developer
   * extending the lineage extraction and not by the local UI.
   */
  override def writeLineageDebug(document: String, dataObjectId: DataObjectId): Unit = {
    if (path.getParent != null) Files.createDirectories(path)
    val file = path.resolve(s"${dataObjectId.id}.lineage-debug.txt")
    logger.info(s"Writing column lineage debug output for $dataObjectId to file $file")
    Files.write(file, document.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def writeWithIndex(document: String, dataObjectId: DataObjectId, tpe: String, version: Long): Unit = {
    if (path.getParent != null) Files.createDirectories(path)
    val indexFile = getIndexPath(dataObjectId, tpe)
    val (newFilename, newFile) = getDataPath(dataObjectId, tpe, version)
    val latestDocument = getLatestData(dataObjectId, tpe)
    if (!latestDocument.contains(document)) {
      logger.info(s"Writing $tpe for $dataObjectId to file $newFile and updating index")
      Files.write(newFile, document.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      Files.write(indexFile, (newFilename + System.lineSeparator).getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }
  }

  def writeFile(document: String, tpe: String): Unit = {
    if (path.getParent != null) Files.createDirectories(path.getParent)
    logger.info(s"Writing $tpe to file $path")
    Files.write(path, document.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def getLatestData(dataObjectId: DataObjectId, tpe: String): Option[String] = {
    // the index is sorted by version, see writeVersion
    val lastIndexEntry = readIndex(dataObjectId, tpe).lastOption
    val latestFile = lastIndexEntry.map(path.resolve).map(_.toFile)
    latestFile.map(readFile)
  }

  override def listVersions(tpe: ExportType, dataObjectId: DataObjectId): Seq[Long] = {
    readIndex(dataObjectId, tpe.toString).flatMap(parseVersion(dataObjectId, tpe.toString, _)).sorted
  }

  override def readVersion(tpe: ExportType, dataObjectId: DataObjectId, version: Long): Option[String] = {
    val (filename, file) = getDataPath(dataObjectId, tpe.toString, version)
    if (readIndex(dataObjectId, tpe.toString).contains(filename) && Files.exists(file)) Some(readFile(file.toFile))
    else None
  }

  /**
   * Write a given version idempotently: nothing is written if this version exists already.
   * The version might be older than the latest version, so the index is rewritten sorted by version.
   */
  override def writeVersion(tpe: ExportType, document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    val index = readIndex(dataObjectId, tpe.toString)
    val (newFilename, newFile) = getDataPath(dataObjectId, tpe.toString, version)
    if (!index.contains(newFilename)) {
      Files.createDirectories(path)
      logger.info(s"Writing $tpe for $dataObjectId version $version to file $newFile and updating index")
      Files.write(newFile, document.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      val newIndex = (index :+ newFilename).sortBy(f => parseVersion(dataObjectId, tpe.toString, f).getOrElse(Long.MinValue))
      Files.write(getIndexPath(dataObjectId, tpe.toString), newIndex.map(_ + System.lineSeparator).asJava, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }
  }

  private def parseVersion(dataObjectId: DataObjectId, tpe: String, filename: String): Option[Long] = {
    filename.stripPrefix(s"${dataObjectId.id}.$tpe.").stripSuffix(".json").toLongOption
  }

  def readIndex(dataObjectId: DataObjectId, tpe: String): Seq[String] = {
    val indexFile = getIndexPath(dataObjectId, tpe)
    Using(Source.fromFile(indexFile.toFile)) {
      _.getLines().filter(_.trim.nonEmpty).toVector
    }.getOrElse(Seq())
  }

  private def getIndexPath(dataObjectId: DataObjectId, tpe: String) = {
    path.resolve(s"${dataObjectId.id}.$tpe.index")
  }

  private def getDataPath(dataObjectId: DataObjectId, tpe: String, version: Long) = {
    val filename = s"${dataObjectId.id}.$tpe.$version.json"
    val file = path.resolve(filename)
    (filename, file)
  }

  override def readLatestSchema(dataObjectId: DataObjectId): Option[String] = {
    getLatestData(dataObjectId, "schema")
  }
}

