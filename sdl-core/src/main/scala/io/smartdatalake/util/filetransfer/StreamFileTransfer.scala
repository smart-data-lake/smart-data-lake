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
package io.smartdatalake.util.filetransfer

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.generic.transformer.GenericFileTransformer
import io.smartdatalake.workflow.dataobject.file.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, FileRefMapping}

import java.io.{InputStream, OutputStream}
import java.util.concurrent.ForkJoinPool
import scala.annotation.tailrec
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success, Try, Using}

/**
  * Copy or transform data of each file from Input- to OutputStream of DataObject's
  */
private[smartdatalake] class StreamFileTransfer(override val srcDO: FileRefDataObject with CanCreateInputStream, override val tgtDO: FileRefDataObject with CanCreateOutputStream, overwrite: Boolean = true, parallelism: Int = 1)
  extends FileTransfer with SmartDataLakeLogger {
  assert(parallelism>0, "parallelism must be greater than 0")

  override def exec(fileRefPairs: Seq[FileRefMapping])(implicit context: ActionPipelineContext): Seq[FileRefMapping] = {
    execPerInputStream(fileRefPairs, "Copy") { (m, is, tgt) =>
      Using.resource(tgtDO.createOutputStream(tgt.fullPath, overwrite)) { os =>
        // transfer data
        Try(copyStream(is, os)) match {
          case Success(r) => r
          case Failure(e) => throw new RuntimeException(s"Could not copy ${srcDO.toStringShort}:${m.src.toStringShort} -> ${tgtDO.toStringShort}:${m.tgt.toStringShort}: ${e.getClass.getSimpleName} - ${e.getMessage}", e)
        }
      }
      m.copy(tgt = tgt)
    }
  }

  /**
   * Executes the file transfer, transforming every file with the given file transformer.
   * A file transformer might create multiple output files per input file in the directory of the target file reference.
   * If the transformation of a file fails, the remaining files are still processed, and an exception listing all failed files is thrown afterwards.
   * @return mapping from input to the output files created
   */
  def execWithTransformer(fileRefPairs: Seq[FileRefMapping], transformer: GenericFileTransformer, options: Map[String, String])
                         (implicit context: ActionPipelineContext): Seq[FileRefMapping] = {
    val results = execPerInputStream(fileRefPairs, "Transform") { (m, is, tgt) =>
      val tgtDir = tgt.fullPath.stripSuffix(tgt.fileName)
      val (fileNames, error) = GenericFileTransformer.transformToFiles(transformer, options, is, tgt.fileName,
        tgtDO.getTargetFileName, fileName => tgtDO.createOutputStream(tgtDir + fileName, overwrite))
      error.foreach(e => logger.error(s"Transformation of ${srcDO.id}:${m.src.toStringShort} failed: $e"))
      (fileNames.map(fileName => m.copy(tgt = tgt.copy(fullPath = tgtDir + fileName, fileName = fileName))), error.map(e => (m.src.toStringShort, e.toString)))
    }
    GenericFileTransformer.throwIfTransformationsFailed(results.flatMap(_._2), results.size)
    results.flatMap(_._1)
  }

  /**
   * Process every input stream of the given file references, in parallel if configured.
   * Note that one FileRef pair might create multiple input streams, e.g. for Webservice with paging. Then an index is added to the target file name.
   */
  private def execPerInputStream[R](fileRefPairs: Seq[FileRefMapping], operation: String)(fn: (FileRefMapping, InputStream, FileRef) => R)
                                   (implicit context: ActionPipelineContext): Seq[R] = {
    assert(fileRefPairs != null, "fileRefPairs is null - FileTransfer must be initialized first")
    parallelize(fileRefPairs).iterator.flatMap { m =>
      srcDO.createInputStreams(m.src.fullPath)
        .zipWithIndex.map {
          case (is,idx) =>
              Using.resource(is) { is =>
                val tgt = if (srcDO.createsMultiInputStreams) {
                  // add index to filename and adapt full path with new filename
                  val fileNameWithIdx = m.tgt.fileName.replaceFirst("([^.]*)\\.", "$1-"+idx+".")
                  m.tgt.copy(fileName = fileNameWithIdx, fullPath = m.tgt.fullPath.replaceFirst(m.tgt.fileName+"$", fileNameWithIdx))
                }
                else {
                  require(idx == 0, s"${srcDO.id} created multiple InputStreams, but createsMultiInputStreams=false")
                  m.tgt
                }
                logger.info(s"$operation ${srcDO.id}:${m.src.toStringShort} -> ${tgtDO.id}:${tgt.toStringShort}")
                fn(m, is, tgt)
              }
        }.toSeq
    }.toSeq
  }

  private def parallelize(fileRefPairs: Seq[FileRefMapping]) = {
    if (parallelism>1) {
      val parFileList = fileRefPairs.par
      parFileList.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
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
