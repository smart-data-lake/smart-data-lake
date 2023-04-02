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
package io.smartdatalake.util.filetransfer

import io.smartdatalake.workflow.{ActionPipelineContext, FileRefMapping}
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

trait FileTransfer {

  protected val srcDO: FileRefDataObject
  protected val tgtDO: FileRefDataObject

  /**
   * Establish mapping from input file references to output file references, translating directory and file name
   * @param fileRefs files to be transferred
   * @return target files which will be created when file transfer is executed
   */
  def getFileRefMapping(fileRefs: Seq[FileRef], filenameExtractorRegex: Option[Regex] = None)(implicit context: ActionPipelineContext): Seq[FileRefMapping] = {
    tgtDO.translateFileRefs(fileRefs, filenameExtractorRegex)
  }

  /**
   * Executes the file transfer
   * @param fileRefPairs: mapping from input to output file references.
   */
  def exec(fileRefPairs: Seq[FileRefMapping])(implicit context: ActionPipelineContext): Unit

}