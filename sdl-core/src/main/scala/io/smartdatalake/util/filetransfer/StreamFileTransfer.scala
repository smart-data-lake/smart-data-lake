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

import java.io.{InputStream, OutputStream}

import io.smartdatalake.util.misc.{SmartDataLakeLogger, TryWithRessource}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Copy data of each file from Input- to OutputStream of DataObject's
  */
private[smartdatalake] class StreamFileTransfer(override val srcDO: FileRefDataObject with CanCreateInputStream, override val tgtDO: FileRefDataObject with CanCreateOutputStream, deleteSource: Boolean = false, overwrite: Boolean = true)
  extends FileTransfer with SmartDataLakeLogger {

  override def exec(fileRefPairs: Seq[(FileRef,FileRef)])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    assert(fileRefPairs != null, "fileRefPairs is null - FileTransfer must be initialized first")
    fileRefPairs.foreach { case (srcFileRef, tgtFileRef) =>
      logger.info(s"Copy ${srcDO.id}:${srcFileRef.toStringShort} -> ${tgtDO.id}:${tgtFileRef.toStringShort}")
      // get streams
      TryWithRessource.exec(srcDO.createInputStream(srcFileRef.fullPath)) { is =>
        TryWithRessource.exec( tgtDO.createOutputStream(tgtFileRef.fullPath, overwrite)) { os =>
          // transfer data
          Try(copyStream(is, os)) match {
            case Success(r) => r
            case Failure(e) => throw new RuntimeException(s"Could not copy ${srcDO.toStringShort}:${srcFileRef.toStringShort} -> ${tgtDO.toStringShort}:${tgtFileRef.toStringShort}: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
          }
        }
      }
    }
  }

  private def copyStream( is: InputStream, os: OutputStream, bufferSize: Int = 4096 ): Unit = {
    val buffer = new Array[Byte](bufferSize)
    @tailrec def writeStep() {
      val cnt = is.read(buffer)
      if (cnt > 0) {
        os.write(buffer, 0, cnt)
        os.flush()
        writeStep()
      }
    }
    writeStep()
  }
}
