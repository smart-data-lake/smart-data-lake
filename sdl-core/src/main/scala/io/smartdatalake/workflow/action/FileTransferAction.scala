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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{Condition, ExecutionMode}
import io.smartdatalake.util.filetransfer.FileTransfer
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileSubFeed}

/**
 * [[Action]] to transfer files between SFtp, Hadoop and local Fs.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param executionMode optional execution mode for this Action
 * @param executionCondition optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 * @param breakFileRefLineage If set to true, file references passed on from previous action are ignored by this action.
 *                            The action will detect on its own what files it is going to process.
 */
case class FileTransferAction(override val id: ActionId,
                              inputId: DataObjectId,
                              outputId: DataObjectId,
                              overwrite: Boolean = true,
                              override val breakFileRefLineage: Boolean = false,
                              override val executionMode: Option[ExecutionMode] = None,
                              override val executionCondition: Option[Condition] = None,
                              override val metricsFailCondition: Option[String] = None,
                              override val metadata: Option[ActionMetadata] = None)
                             ( implicit instanceRegistry: InstanceRegistry)
  extends FileOneToOneActionImpl {

  override val input: FileRefDataObject with CanCreateInputStream = getInputDataObject[FileRefDataObject with CanCreateInputStream](inputId)
  override val output: FileRefDataObject with CanCreateOutputStream = getOutputDataObject[FileRefDataObject with CanCreateOutputStream](outputId)
  override val inputs: Seq[FileRefDataObject] = Seq(input)
  override val outputs: Seq[FileRefDataObject] = Seq(output)

  // initialize FileTransfer
  private val fileTransfer = FileTransfer(input, output, overwrite)

  override def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed = {
    assert(inputSubFeed.fileRefs.nonEmpty, "inputSubFeed.fileRefs must be defined for FileTransferAction.doTransform")
    val inputFileRefs = inputSubFeed.fileRefs.get
    outputSubFeed.copy(fileRefMapping = Some(fileTransfer.getFileRefMapping(inputFileRefs)))
  }

  override def writeSubFeed(subFeed: FileSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): WriteSubFeedResult[FileSubFeed] = {
    val fileRefMapping = subFeed.fileRefMapping.getOrElse(throw new IllegalStateException(s"($id) file mapping is not defined"))
    output.startWritingOutputStreams(subFeed.partitionValues)
    if (fileRefMapping.nonEmpty) fileTransfer.exec(fileRefMapping)
    output.endWritingOutputStreams(subFeed.partitionValues)
    // return metric to action
    val filesWritten = fileRefMapping.size.toLong
    val metrics = Map("files_written"->filesWritten) ++ (if (filesWritten == 0) Map ("no_data" -> true) else Map())
    WriteSubFeedResult(subFeed, Some(fileRefMapping.isEmpty), Some(metrics))
  }

  override def postprocessOutputSubFeedCustomized(subFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed = {
    // create output sample file in init-phase
    if (context.phase == ExecutionPhase.Init) {
      subFeed.fileRefMapping.flatMap(_.headOption).foreach {
        sampleFileRefMapping =>
          val sampleFile = output.createSampleFile
          // exec only if output returned a sample file to create
          sampleFile.foreach {
            file =>
              val sampleFileTransfer = FileTransfer(input, output, overwrite = true)
              sampleFileTransfer.exec(Seq(sampleFileRefMapping.copy(tgt = FileRef(file, output.getFilenameFromPath(file), PartitionValues(Map())))))
          }
      }
    }
    subFeed
  }

  override def factory: FromConfigFactory[Action] = FileTransferAction
}

object FileTransferAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FileTransferAction = {
    extract[FileTransferAction](config)
  }
}
