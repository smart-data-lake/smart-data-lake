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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.{PartitionLayout, PartitionValues}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession

private[smartdatalake] trait FileRefDataObject extends FileDataObject {

  /**
   * Definition of partition layout
   * use %<partitionColName>% as placeholder and * for globs in layout
   * Note: if you have globs in partition layout, it's not possible to write files to this DataObject
   * Note: if this is a directory, you must add a final backslash to the partition layout
   */
  def partitionLayout(): Option[String]

  /**
   * Definition of fileName. Default is an asterix to match everything.
   * This is concatenated with the partition layout to search for files.
   */
  val fileName = "*"

  // assert that if partitions are defined, also partition layout is defined and vice-versa
  require(partitions.nonEmpty || partitionLayout().isEmpty, s"if partitions are defined also partition layout must be defined (${this.id})")
  require(partitionLayout().isDefined || partitions.isEmpty, s"if partition layout is defined also partitions must be defined (${this.id})")
  // the partition layout must contain placeholders for all partition columns
  if (partitions.nonEmpty) {
    val partitionLayoutTokens = PartitionLayout.extractTokens(partitionLayout().get).toSet
    require(partitionLayoutTokens == partitions.toSet, s"specified partitions (${partitions.mkString(",")}) don't match with extracted partitions (${partitionLayoutTokens.mkString(",")}) specified partition layout (${partitionLayout()}) for ${this.id}")
  }

  /**
   * Method for subclasses to override the base path for this DataObject.
   * This is for instance needed if pathPrefix is defined in a connection.
   * @return
   */
  def getPath: String = path

  /**
   * List files for given partition values
   *
   * @param partitionValues List of partition values to be filtered. If empty all files in root path of DataObject will be listed.
   * @return List of [[FileRef]]s
   */
  def getFileRefs(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[FileRef]

  /**
   * Given some [[FileRef]]s for another [[DataObject]], translate the paths to the root path of this [[DataObject]]
   */
  def translateFileRefs(fileRefs: Seq[FileRef])(implicit session: SparkSession, context: ActionPipelineContext): Seq[FileRef] = {
    assert(!partitionLayout().exists(_.contains("*")), s"Cannot translate FileRef if partition layout contains * (${partitionLayout()})")
    fileRefs.map {
      f =>
        // make fileName match this DataObjects FileName pattern.
        val newFileName = if (f.fileName.matches(this.fileName.replace("*",".*"))) f.fileName
        else f.fileName + this.fileName.replace("*","")
        // prepend path and partition string before fileName
        val newPath = getPartitionString(f.partitionValues.addKey(Environment.runIdPartitionColumnName, context.runId.toString))
          .map(partitionString => getPath + separator + partitionString + newFileName)
          .getOrElse(getPath + separator + newFileName)
        f.copy(fullPath = newPath)
    }
  }

  /**
   * get partition values formatted by partition layout
   */
  def getPartitionString(partitionValues: PartitionValues)(implicit session: SparkSession): Option[String] = {
    if (partitionLayout().isDefined) Some(partitionValues.getPartitionString(partitionLayout().get))
    else if (partitions.isEmpty) None
    else throw new RuntimeException("Partition layout needed when working with PartitionValues")
  }

  /**
   * prepare paths to be searched
   */
  protected def getSearchPaths(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[(PartitionValues, String)] = {
    val partitionValuesWithDefault = if (partitionValues.isEmpty) Seq(PartitionValues(Map())) else partitionValues
    val partitionValuesPaths = partitionValuesWithDefault.map(v => (v, getPartitionString(v)))
    partitionValuesPaths.map {
      // through concatenating partition path and filename there might be two "*" one after another - we need to clean this after concatenation
      case (v, Some(partitionPath)) => (v, s"$getPath$separator$partitionPath$fileName".replace("**","*"))
      case (v, None) => (v, s"$getPath$separator$fileName".replace("**","*"))
    }
  }

  /**
   * Extract partition values from a given file path
   */
  protected def extractPartitionValuesFromPath(filePath: String): PartitionValues = {
    PartitionLayout.extractPartitionValues(partitionLayout().get, fileName, filePath.stripPrefix(path + separator))
  }

  /**
   * Delete given files. This is used to cleanup files after they are processed.
   */
  def deleteFileRefs(fileRefs: Seq[FileRef])(implicit session: SparkSession): Unit = throw new RuntimeException(s"($id) deleteFileRefs not implemented")

  /**
   * Delete all data. This is used to implement SaveMode.Overwrite.
   */
  def deleteAll(implicit session: SparkSession): Unit = throw new RuntimeException(s"($id) deleteAll not implemented")

  /**
   * Overwrite or Append new data.
   * When writing partitioned data, this applies only to partitions concerned.
   */
  def saveMode: SDLSaveMode
}

private[smartdatalake] case class FileRef( fullPath:String, fileName: String, partitionValues: PartitionValues) {
  def toStringShort: String = if (fullPath != "") fullPath else fileName
}
