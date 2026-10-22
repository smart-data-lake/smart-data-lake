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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.filetransfer.StreamFileTransfer
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericFileTransformer
import io.smartdatalake.workflow.dataobject.file.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileRefMapping, FileSubFeed}

import scala.util.Using

/**
 * [[Action]] to transfer files between SFtp, Hadoop, local Filesystem and a Webservice. Note that the Input DataObject and Output DataObject are not interpreted by this Action: the Data is just transferred as is.
 * As data is transferred as is, matching the data format between the Input DataObject (e.g. CSV from WebserviceFileDataObject) and Output DataObject (e.g. CsvFileDataObject) is in the responsibility of the developer/user.
 * If you want to convert or transform data formats between input and output, use the CopyAction instead. CopyAction will read the data from the Input DataObject into a DataFrame, and write that DataFrame to the Output DataObject. In this case the DataObjects are responsible to convert the data into a DataFrame and back.
 *
 * Optionally a file transformer can be configured to transform files as byte streams, create multiple output files per input
 * file, or transform partition values, e.g. to standardize partition values extracted from the file path of the input.
 * Note that the transformation is executed on the driver, in up to maxParallelism threads. Large files or CPU intensive
 * transformations can therefore cause memory and CPU resource problems on the driver.
 * Use CustomFileAction instead to distribute the transformation on Spark executors if the input is a HadoopFileDataObject.
 * Note that the file references created in init phase are a prediction with one output file per input file and the default
 * file name, as files are only written in exec phase. If the transformer creates multiple or differently named output files,
 * the file references passed on to the next Action therefore differ between init and exec phase. This is relevant
 * for runs only executing the init phase, e.g. simulation.
 *
 * Example:
 * {{{
 * actions = {
 *   download-airports {
 *     type = FileTransferAction
 *     inputId = ext-airports
 *     outputId = stg-airports
 *     overwrite = false
 *     maxParallelism = 4
 *   }
 * }
 * }}}
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
 * @param transformer optional file transformer, e.g. ScalaClassFileTransformer or ScalaCodeFileTransformer.
 *                    The transformation is executed on the driver, see above.
 *                    If a transformation fails for a file, the remaining files are still processed, but the Action fails afterwards.
 * @param createFileRefLineage   If set to false, this action does not propagate output FileRefs to further actions.
 *                               This helps to avoid performance and memory problems with too many FileRefs.
 *                               Default is true.
 */
case class FileTransferAction(override val id: ActionId,
                              inputId: DataObjectId,
                              outputId: DataObjectId,
                              overwrite: Boolean = true,
                              maxParallelism: Option[Int] = None,
                              filenameExtractorRegex: Option[String] = None,
                              transformer: Option[GenericFileTransformer] = None,
                              createFileRefLineage: Boolean = true,
                              override val breakFileRefLineage: Boolean = false,
                              override val executionMode: Option[ExecutionMode] = None,
                              override val executionCondition: Option[Condition] = None,
                              override val metricsFailCondition: Option[String] = None,
                              override val metadata: Option[ActionMetadata] = None)
                             ( implicit val instanceRegistry: InstanceRegistry)
  extends FileOneToOneActionImpl {

  override val input: FileRefDataObject with CanCreateInputStream = getInputDataObject[FileRefDataObject with CanCreateInputStream](inputId)
  override val output: FileRefDataObject with CanCreateOutputStream = getOutputDataObject[FileRefDataObject with CanCreateOutputStream](outputId)
  override val inputs: Seq[FileRefDataObject] = Seq(input)
  override val outputs: Seq[FileRefDataObject] = Seq(output)

  // initialize FileTransfer
  private val parallelism = Seq(maxParallelism, input.recommendedParallelism, output.recommendedParallelism).flatten.sorted.headOption.getOrElse(1) // take first value
  private val fileTransfer = new StreamFileTransfer(input, output, overwrite, parallelism)

  // a transformer might map input partitions to different output partitions, which is validated at runtime in transform
  override protected def transformsPartitionValues: Boolean = transformer.isDefined

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformer.foreach(_.prepare(id))
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[PartitionValues, PartitionValues] =
    applyTransformers(transformer.toSeq, partitionValues, executionModeResultOptions)

  private def execFileTransfer(fileTransfer: StreamFileTransfer, fileRefMapping: Seq[FileRefMapping], subFeed: FileSubFeed)(implicit context: ActionPipelineContext): Seq[FileRefMapping] = {
    transformer match {
      case Some(t) => fileTransfer.execWithTransformer(fileRefMapping, t, t.prepareOptions(id, subFeed.partitionValues, subFeed.executionModeResultOptions))
      case None => fileTransfer.exec(fileRefMapping)
    }
  }

  override def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed = {
    assert(inputSubFeed.fileRefs.nonEmpty, "inputSubFeed.fileRefs must be defined for FileTransferAction.doTransform")
    val inputFileRefs = inputSubFeed.fileRefs.get
    logger.info(s"($id) got ${inputFileRefs.size} files to copy")
    val fileRefMapping = translateFileRefs(inputFileRefs, inputSubFeed.executionModeResultOptions, filenameExtractorRegex.map(_.r))
    val partitionValues = if (outputSubFeed.partitionValues.nonEmpty || output.partitions.isEmpty) outputSubFeed.partitionValues
    else fileRefMapping.map(_.tgt.partitionValues).distinct
    outputSubFeed.copy(fileRefs = Some(fileRefMapping.map(_.tgt)), fileRefMapping = Some(fileRefMapping), partitionValues = partitionValues)
  }

  override def writeSubFeed(subFeed: FileSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): FileSubFeed = {
    var fileRefMapping = subFeed.fileRefMapping.getOrElse(throw new IllegalStateException(s"($id) file mapping is not defined"))
    output.startWritingOutputStreams(subFeed.partitionValues)
    if (fileRefMapping.nonEmpty) fileRefMapping = execFileTransfer(fileTransfer, fileRefMapping, subFeed)
    // update file references with the files actually written, as input streams or transformers might create multiple files
    var outputSubFeed = subFeed.copy(fileRefs = Some(fileRefMapping.map(_.tgt)), fileRefMapping = Some(fileRefMapping))
    output.endWritingOutputStreams(outputSubFeed.partitionValues)
    // return metric to action
    val filesWritten = fileRefMapping.size.toLong
    val metrics = Map("files_written"->filesWritten) ++ (if (filesWritten == 0) Map ("no_data" -> true) else Map())
    outputSubFeed = outputSubFeed.withMetrics(metrics).asInstanceOf[FileSubFeed]
    // remove fileRefMapping if createFileRefLineage is false
    if (!createFileRefLineage) outputSubFeed = outputSubFeed.copy(fileRefMapping = None, fileRefs = None)
    // return
    outputSubFeed
  }

  override def postprocessOutputSubFeedCustomized(subFeed: FileSubFeed, inputSubFeeds: Seq[FileSubFeed])(implicit context: ActionPipelineContext): FileSubFeed = {
    // create output sample file in init-phase
    if (context.phase == ExecutionPhase.Init) {
      subFeed.fileRefMapping.flatMap(_.headOption).filter(m => sampleInputFileExists(m.src)).foreach {
        sampleFileRefMapping =>
          val sampleFile = output.createSampleFile
          // exec only if output returned a sample file to create
          sampleFile.foreach {
            file =>
              transformer match {
                case Some(t) =>
                  val options = t.prepareOptions(id, subFeed.partitionValues, subFeed.executionModeResultOptions)
                  val sampleInput = input.createInputStreams(sampleFileRefMapping.src.fullPath).next()
                  Using.resource(sampleInput) { is =>
                    GenericFileTransformer.transformToSampleFile(t, options, is, sampleFileRefMapping.tgt.fileName, () => output.createOutputStream(file, overwrite = true))
                  }.foreach(ex => throw ex)
                case None =>
                  val sampleFileTransfer = new StreamFileTransfer(input, output, overwrite = true)
                  sampleFileTransfer.exec(Seq(sampleFileRefMapping.copy(tgt = FileRef(file, output.getFilenameFromPath(file), PartitionValues(Map())))))
              }
          }
      }
    }
    super.postprocessOutputSubFeedCustomized(subFeed, inputSubFeeds)
  }
}

object FileTransferAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FileTransferAction = {
    extract[FileTransferAction](config)
  }
}
