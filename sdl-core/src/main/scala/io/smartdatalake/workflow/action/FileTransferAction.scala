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
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.filetransfer.StreamFileTransfer
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileSubFeed}

/**
 * [[Action]] to transfer files between SFtp, Hadoop, local Filesystem and a Webservice. Note that the Input DataObject and Output DataObject are not interpreted by this Action: the Data is just transferred as is.
 * As data is transferred as is, matching the data format between the Input DataObject (e.g. CSV from WebserviceFileDataObject) and Output DataObject (e.g. CsvFileDataObject) is in the responsibility of the developer/user.
 * If you want to convert or transform data formats between input and output, use the CopyAction instead. CopyAction will read the data from the Input DataObject into a DataFrame, and write that DataFrame to the Output DataObject. In this case the DataObjects are responsible to convert the data into a DataFrame and back.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param overwrite Allow existing output file to be overwritten. If false the action will fail if a file to be created already exists. Default is true.
 * @param maxParallelism Set maximum of files to be transferred in parallel.
 *                       Note that this information can also be set on DataObjects like SFtpFileRefDataObject, resp. its SFtpFileRefConnection.
 *                       The FileTransferAction will then take the minimum parallelism of input, output and this attribute.
 *                       If parallelism is not specified on input, output and this attribute, it is set to 1.
 * @param filenameExtractorRegex A regex to extract a part of the filename to keep in the translated FileRef.
 *                               If the regex contains group definitions, the first group is taken, otherwise the whole regex match.
 *                               Default is None which keeps the whole filename (without path).
 * @param breakFileRefLineage If set to true, file references passed on from previous action are ignored by this action.
 *                            The action will detect on its own what files it is going to process.
 */
case class FileTransferAction(override val id: ActionId,
                              inputId: DataObjectId,
                              outputId: DataObjectId,
                              overwrite: Boolean = true,
                              maxParallelism: Option[Int] = None,
                              filenameExtractorRegex: Option[String] = None,
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
  private val parallelism = Seq(maxParallelism,input.recommendedParallelism,output.recommendedParallelism).flatten.sorted.headOption.getOrElse(1) // take minimum value
  private val fileTransfer = new StreamFileTransfer(input, output, overwrite, parallelism)

  override def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed = {
    assert(inputSubFeed.fileRefs.nonEmpty, "inputSubFeed.fileRefs must be defined for FileTransferAction.doTransform")
    val inputFileRefs = inputSubFeed.fileRefs.get
    logger.info(s"($id) got ${inputFileRefs.size} files to copy")
    val fileRefMapping = fileTransfer.getFileRefMapping(inputFileRefs, filenameExtractorRegex.map(_.r))
    val partitionValues = if (outputSubFeed.partitionValues.nonEmpty || output.partitions.isEmpty) outputSubFeed.partitionValues
    else fileRefMapping.map(_.tgt.partitionValues).distinct
    outputSubFeed.copy(fileRefs = Some(fileRefMapping.map(_.tgt)), fileRefMapping = Some(fileRefMapping), partitionValues = partitionValues)
  }

  override def writeSubFeed(subFeed: FileSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): FileSubFeed = {
    var fileRefMapping = subFeed.fileRefMapping.getOrElse(throw new IllegalStateException(s"($id) file mapping is not defined"))
    output.startWritingOutputStreams(subFeed.partitionValues)
    if (fileRefMapping.nonEmpty) fileRefMapping = fileTransfer.exec(fileRefMapping)
    val outputSubFeed = subFeed.copy(fileRefMapping = Some(fileRefMapping))
    output.endWritingOutputStreams(outputSubFeed.partitionValues)
    // return metric to action
    val filesWritten = fileRefMapping.size.toLong
    val metrics = Map("files_written"->filesWritten) ++ (if (filesWritten == 0) Map ("no_data" -> true) else Map())
    subFeed.withMetrics(metrics).asInstanceOf[FileSubFeed]
  }

  override def postprocessOutputSubFeedCustomized(subFeed: FileSubFeed, inputSubFeeds: Seq[FileSubFeed])(implicit context: ActionPipelineContext): FileSubFeed = {
    // create output sample file in init-phase
    if (context.phase == ExecutionPhase.Init) {
      subFeed.fileRefMapping.flatMap(_.headOption).foreach {
        sampleFileRefMapping =>
          val sampleFile = output.createSampleFile
          // exec only if output returned a sample file to create
          sampleFile.foreach {
            file =>
              val sampleFileTransfer = new StreamFileTransfer(input, output, overwrite = true)
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
