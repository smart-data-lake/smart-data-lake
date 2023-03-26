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
import io.smartdatalake.util.misc.{SmartDataLakeLogger, WithResource}
import io.smartdatalake.workflow.{ActionPipelineContext, FileRefMapping}
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.GenSeqLike
import scala.util.{Failure, Success, Try}

/**
  * Copy data of each file from Input- to OutputStream of DataObject's
  */
private[smartdatalake] class StreamFileTransfer(override val srcDO: FileRefDataObject with CanCreateInputStream, override val tgtDO: FileRefDataObject with CanCreateOutputStream, overwrite: Boolean = true, parallelism: Int = 1)
  extends FileTransfer with SmartDataLakeLogger {
  assert(parallelism>0, "parallelism must be greater than 0")

  override def exec(fileRefPairs: Seq[FileRefMapping])(implicit context: ActionPipelineContext): Unit = {
    assert(fileRefPairs != null, "fileRefPairs is null - FileTransfer must be initialized first")
    parallelize(fileRefPairs).foreach { m =>
      logger.info(s"Copy ${srcDO.id}:${m.src.toStringShort} -> ${tgtDO.id}:${m.tgt.toStringShort}")
      // get streams
      WithResource.exec(srcDO.createInputStream(m.src.fullPath)) { is =>
        WithResource.exec( tgtDO.createOutputStream(m.tgt.fullPath, overwrite)) { os =>
          // transfer data
          Try(copyStream(is, os)) match {
            case Success(r) => r
            case Failure(e) => throw new RuntimeException(s"Could not copy ${srcDO.toStringShort}:${m.src.toStringShort} -> ${tgtDO.toStringShort}:${m.tgt.toStringShort}: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
          }
        }
      }
    }
  }

  private def parallelize(fileRefPairs: Seq[FileRefMapping]) = {
    if (parallelism>1) {
      val parFileList = fileRefPairs.par
      parFileList.tasksupport = new scala.collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parallelism))
      parFileList
    } else fileRefPairs
  }

  private def copyStream( is: InputStream, os: OutputStream, bufferSize: Int = 4096 ): Unit = {
    val buffer = new Array[Byte](bufferSize)
    @tailrec def writeStep(): Unit = {
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
