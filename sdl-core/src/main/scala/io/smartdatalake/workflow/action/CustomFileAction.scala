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
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.spark.customlogic.CustomFileTransformerConfig
import io.smartdatalake.workflow.dataobject.HadoopFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileSubFeed}
import org.apache.hadoop.fs.Path

import scala.util.Using

/**
 * [[Action]] to transform files between two Hadoop Data Objects.
 * The transformation is executed in distributed mode on the Spark executors.
 * A custom file transformer must be given, which reads a file from Hadoop and writes it back to Hadoop.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param transformer a custom file transformer, which reads a file from HadoopFileDataObject and writes it back to another HadoopFileDataObject
 * @param filesPerPartition number of files per Spark partition
 */
case class CustomFileAction(override val id: ActionId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            transformer: CustomFileTransformerConfig,
                            filesPerPartition: Int = 10,
                            override val breakFileRefLineage: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val executionCondition: Option[Condition] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends FileOneToOneActionImpl with SmartDataLakeLogger {

  assert(filesPerPartition>0, s"($id) filesPerPartition must be greater than 0. Current value: $filesPerPartition")

  override val input: HadoopFileDataObject = getInputDataObject[HadoopFileDataObject](inputId)
  override val output: HadoopFileDataObject = getOutputDataObject[HadoopFileDataObject](outputId)
  override val inputs: Seq[HadoopFileDataObject] = Seq(input)
  override val outputs: Seq[HadoopFileDataObject] = Seq(output)

  override def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed = {
    assert(inputSubFeed.fileRefs.nonEmpty, "inputSubFeed.fileRefs must be defined for FileTransferAction.doTransform")
    val inputFileRefs = inputSubFeed.fileRefs.get
    val fileRefMapping = output.translateFileRefs(inputFileRefs)
    val partitionValues = if (outputSubFeed.partitionValues.nonEmpty || output.partitions.isEmpty) outputSubFeed.partitionValues
    else fileRefMapping.map(_.tgt.partitionValues).distinct
    outputSubFeed.copy(fileRefs = Some(fileRefMapping.map(_.tgt)), fileRefMapping = Some(fileRefMapping), partitionValues = partitionValues)
  }

  override def writeSubFeed(subFeed: FileSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): FileSubFeed = {
    val fileRefMapping = subFeed.fileRefMapping.getOrElse(throw new IllegalStateException(s"($id) file mapping is not defined"))
    output.startWritingOutputStreams(subFeed.partitionValues)
    if (fileRefMapping.nonEmpty) {
      val session = context.sparkSession
      import session.implicits._

      // Create a Dataset of files to be processed
      val srcDO = input // avoid serialization of whole action by assigning input to local variable
      srcDO.filesystem // init filesystem to prepare serializable hadoop configuration
      val tgtDO = output // avoid serialization of whole action by assigning output to local variable
      tgtDO.filesystem // init filesystem to prepare serializable hadoop configuration
      val transformerVal = transformer // avoid serialization of whole action by assigning transformer to local variable
      val filePathPairs = fileRefMapping.map{ m => (m.src.fullPath, m.tgt.fullPath)}
      val nbOfPartitions = math.max(filePathPairs.size / filesPerPartition, 1)
      val transformedDs = filePathPairs.toDS().repartition(nbOfPartitions)
        .map { case (srcPath, tgtPath) =>
          val hadoopSrcPath = new Path(srcPath)
          val hadoopTgtPath = new Path(tgtPath)
          val result = Using.resource(srcDO.getFilesystem(hadoopSrcPath).open(hadoopSrcPath)) { is =>
            Using.resource(tgtDO.getFilesystem(hadoopTgtPath).create(hadoopTgtPath, true)) { os => // overwrite = true
              transformerVal.transform(is, os)
            }
          }
          (srcPath, tgtPath, result.map(_.getMessage))
        }

      // execute the data set and log results
      val results = transformedDs.collect()
      results.foreach { case (_, tgt, ex) =>
        if (ex.isEmpty) logger.info(s"transformed $tgt")
        else logger.error(s"transformed $tgt with error $ex")
      }
    }
    output.endWritingOutputStreams(subFeed.partitionValues)
    // return metric to action
    val filesWritten = fileRefMapping.size.toLong
    val metrics = Map("files_written"->fileRefMapping.size.toLong) ++ (if (filesWritten == 0) Map ("no_data" -> true) else Map())
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
              val hadoopSrcPath = new Path(sampleFileRefMapping.src.fullPath)
              val hadoopTgtPath = new Path(file)
              val result = Using.resource(input.filesystem.open(hadoopSrcPath)) { is =>
                Using.resource(output.filesystem.create(hadoopTgtPath, true)) { os => // overwrite = true
                  transformer.transform(is, os)
                }
              }
          }
      }
    }
    subFeed
  }

  override def factory: FromConfigFactory[Action] = CustomFileAction
}

object CustomFileAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomFileAction = {
    extract[CustomFileAction](config)
  }
}
