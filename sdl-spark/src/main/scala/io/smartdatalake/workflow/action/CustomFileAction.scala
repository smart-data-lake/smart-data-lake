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
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry, TypeMismatchException}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericFileTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.file.HadoopFileDataObject
import io.smartdatalake.workflow.dataobject.spark.SparkFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileSubFeed}
import org.apache.hadoop.fs.Path

import scala.util.Using

/**
 * [[Action]] to transform files between two Hadoop Data Objects.
 * The transformation is executed in distributed mode on Spark executors.
 * A custom file transformer must be given, which reads a file as input stream and writes one or more output files.
 * The transformer can also transform partition values, e.g. to standardize partition values extracted from the file path
 * of the input into different partition columns of the output.
 *
 * Use this Action if files must be processed as files (byte or line streams), e.g. to unzip, decrypt or repair a file
 * format Spark can not read. The list of files to transfer is created on the driver and then distributed to the
 * executors, where the transformer gets the input stream and the output stream of one file at a time. If the content
 * can be read as a DataFrame, prefer [[CopyAction]], if the files only need to be moved unchanged prefer
 * [[FileTransferAction]].
 *
 * Example:
 * {{{
 * actions = {
 *   transform-airports {
 *     type = CustomFileAction
 *     inputId = stg-airports
 *     outputId = int-airports
 *     transformer = {
 *       type = ScalaClassFileTransformer
 *       className = com.company.transformer.CutColumnsFileTransformer
 *       options = { delimiter = "," }
 *     }
 *     filesPerPartition = 5
 *   }
 * }
 * }}}
 *
 * If the transformer returns or throws an exception for a file, the remaining files are still processed, but the Action
 * fails afterwards, listing all failed files. Note that the output files already written stay in place.
 * Note that the file references created in init phase are a prediction with one output file per input file and the default
 * file name, as files are only written in exec phase. If the transformer creates multiple or differently named output files,
 * the file references passed on to the next Action therefore differ between init and exec phase. This is relevant
 * for runs only executing the init phase, e.g. simulation.
 *
 * @note inputId must be a HadoopFileDataObject and outputId a SparkFileDataObject, and the transformer code must be
 *       serializable as it is shipped to the Spark executors.
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param transformer file transformer to apply, e.g. ScalaClassFileTransformer or ScalaCodeFileTransformer.
 *                    It reads a file from HadoopFileDataObject and writes one or more files to another HadoopFileDataObject.
 * @param filesPerPartition number of files per Spark partition
 */
case class CustomFileAction(override val id: ActionId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            transformer: GenericFileTransformer,
                            filesPerPartition: Int = 10,
                            override val breakFileRefLineage: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val executionCondition: Option[Condition] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None,
                            override val engineConnectionId: Option[ConnectionId] = None
                           )(implicit val instanceRegistry: InstanceRegistry)
  extends FileOneToOneActionImpl with SmartDataLakeLogger {

  assert(filesPerPartition>0, s"($id) filesPerPartition must be greater than 0. Current value: $filesPerPartition")

  override val input: HadoopFileDataObject = getInputDataObject[HadoopFileDataObject](inputId)
  override val output: SparkFileDataObject = getOutputDataObject[SparkFileDataObject](outputId)
  override val inputs: Seq[HadoopFileDataObject] = Seq(input)
  override val outputs: Seq[SparkFileDataObject] = Seq(output)

  // the transformer might map input partitions to different output partitions, which is validated at runtime in transform
  override protected def transformsPartitionValues: Boolean = true

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformer.prepare(id)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[PartitionValues, PartitionValues] =
    applyTransformers(Seq(transformer), partitionValues, executionModeResultOptions)

  override def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed = {
    assert(inputSubFeed.fileRefs.nonEmpty, "inputSubFeed.fileRefs must be defined for CustomFileAction.doTransform")
    val inputFileRefs = inputSubFeed.fileRefs.get
    // create target file references with transformed partition values
    val fileRefMapping = translateFileRefs(inputFileRefs, inputSubFeed.executionModeResultOptions)
    val partitionValues = if (outputSubFeed.partitionValues.nonEmpty || output.partitions.isEmpty) outputSubFeed.partitionValues
    else fileRefMapping.map(_.tgt.partitionValues).distinct
    outputSubFeed.copy(fileRefs = Some(fileRefMapping.map(_.tgt)), fileRefMapping = Some(fileRefMapping), partitionValues = partitionValues)
  }

  override def writeSubFeed(subFeed: FileSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): FileSubFeed = {
    var fileRefMapping = subFeed.fileRefMapping.getOrElse(throw new IllegalStateException(s"($id) file mapping is not defined"))
    output.startWritingOutputStreams(subFeed.partitionValues)
    if (fileRefMapping.nonEmpty) {
      val session = SparkSubFeed.getSparkSession
      import session.implicits._

      // Create a Dataset of files to be processed
      val srcDO = input // avoid serialization of whole action by assigning input to local variable
      srcDO.filesystem // init filesystem to prepare serializable hadoop configuration
      val tgtDO = output // avoid serialization of whole action by assigning output to local variable
      tgtDO.filesystem // init filesystem to prepare serializable hadoop configuration
      val transformerVal = transformer // avoid serialization of whole action by assigning transformer to local variable
      // prepare options on the driver, as they might need the ActionPipelineContext to evaluate runtimeOptions
      val options = transformer.prepareOptions(id, subFeed.partitionValues, subFeed.executionModeResultOptions)
      val filePathPairs = fileRefMapping.map(m => (m.src.fullPath, m.tgt.fullPath, m.tgt.fileName))
      val nbOfPartitions = math.max(filePathPairs.size / filesPerPartition, 1)
      val transformedDs = filePathPairs.toDS().repartition(nbOfPartitions)
        .map { case (srcPath, tgtPath, tgtFileName) =>
          val hadoopSrcPath = new Path(srcPath)
          val tgtDir = tgtPath.stripSuffix(tgtFileName)
          val (fileNames, error) = Using.resource(srcDO.getFilesystem(hadoopSrcPath).open(hadoopSrcPath)) { is =>
            GenericFileTransformer.transformToFiles(transformerVal, options, is, tgtFileName, tgtDO.getTargetFileName, { fileName =>
              val hadoopTgtPath = new Path(tgtDir + fileName)
              tgtDO.getFilesystem(hadoopTgtPath).create(hadoopTgtPath, true) // overwrite = true
            })
          }
          (srcPath, tgtDir, fileNames, error.map(_.toString))
        }

      // execute the data set and log results
      val results = transformedDs.collect()
      results.foreach { case (src, tgtDir, fileNames, error) =>
        if (error.isEmpty) logger.info(s"transformed $src to ${fileNames.map(tgtDir + _).mkString(", ")}")
        else logger.error(s"transformed $src with error ${error.get}")
      }
      // fail after all files are processed if any transformation returned an error
      GenericFileTransformer.throwIfTransformationsFailed(results.collect { case (src, _, _, Some(error)) => (src, error) }.toSeq, results.length)
      // create mapping to the output files actually created
      val createdFiles = results.map { case (src, tgtDir, fileNames, _) => (src, (tgtDir, fileNames)) }.toMap
      fileRefMapping = fileRefMapping.flatMap { m =>
        val (tgtDir, fileNames) = createdFiles(m.src.fullPath)
        fileNames.map(fileName => m.copy(tgt = m.tgt.copy(fullPath = tgtDir + fileName, fileName = fileName)))
      }
    }
    output.endWritingOutputStreams(subFeed.partitionValues)
    // return metric to action
    val filesWritten = fileRefMapping.size.toLong
    val metrics = Map("files_written" -> filesWritten) ++ (if (filesWritten == 0) Map("no_data" -> true) else Map())
    subFeed.copy(fileRefs = Some(fileRefMapping.map(_.tgt)), fileRefMapping = Some(fileRefMapping)).withMetrics(metrics).asInstanceOf[FileSubFeed]
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
              val options = transformer.prepareOptions(id, subFeed.partitionValues, subFeed.executionModeResultOptions)
              val hadoopSrcPath = new Path(sampleFileRefMapping.src.fullPath)
              Using.resource(input.filesystem.open(hadoopSrcPath)) { is =>
                GenericFileTransformer.transformToSampleFile(transformer, options, is, sampleFileRefMapping.tgt.fileName,
                  () => output.filesystem.create(new Path(file), true)) // overwrite = true
              }.foreach(ex => throw ex)
          }
      }
    }
    super.postprocessOutputSubFeedCustomized(subFeed, inputSubFeeds)
  }
}

object CustomFileAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomFileAction = {
    extract[CustomFileAction](config)
  }
}
