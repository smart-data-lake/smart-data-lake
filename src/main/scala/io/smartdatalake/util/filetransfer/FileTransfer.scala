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

import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession

private[smartdatalake] trait FileTransfer {

  protected val srcDO: FileRefDataObject
  protected val tgtDO: FileRefDataObject

  /**
   * Initialize the file transfer with the files to copy
   *
   * @param fileRefs files to be transfered
   * @return target files which will be created when file transfer is executed
   */
  def init(fileRefs: Seq[FileRef])(implicit session: SparkSession): Seq[(FileRef,FileRef)] = {
    val tgtFileRefs = tgtDO.translateFileRefs(fileRefs)
    fileRefs.zip(tgtFileRefs)
  }

  /**
   * Executes the file transfer
   */
  def exec(fileRefPairs: Seq[(FileRef,FileRef)])(implicit session: SparkSession): Unit

}

/**
 * Factory for FileTransfer's.
 * For now we can do everything with the StreamFileTransfer.
 */
private[smartdatalake] object FileTransfer {
  def apply( srcDO: DataObject, tgtDO: DataObject, deleteSource: Boolean, overwrite: Boolean): FileTransfer = {
    (srcDO, tgtDO) match {
      case (inputDO: FileRefDataObject with CanCreateInputStream, outputDO: FileRefDataObject with CanCreateOutputStream) => new StreamFileTransfer(inputDO, outputDO, deleteSource, overwrite)
    }
  }
}
