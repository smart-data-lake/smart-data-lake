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
import io.smartdatalake.util.misc.{SmartDataLakeLogger, TryWithRessource}
import io.smartdatalake.workflow.action.customlogic.CustomFileTransformerConfig
import io.smartdatalake.workflow.dataobject.HadoopFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * [[Action]] to transform files between two Hadoop Data Objects.
 * The transformation is executed in distributed mode on the Spark executors.
 * A custom file transformer must be given, which reads a file from Hadoop and writes it back to Hadoop.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param transformer a custom file transformer, which reads a file from HadoopFileDataObject and writes it back to another HadoopFileDataObject
 * @param deleteDataAfterRead if the input files should be deleted after processing successfully
 * @param filesPerPartition number of files per Spark partition
 * @param executionMode optional execution mode for this Action
 * @param executionCondition     optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
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

  override def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed = {
    assert(inputSubFeed.fileRefs.nonEmpty, "inputSubFeed.fileRefs must be defined for FileTransferAction.doTransform")
    val inputFileRefs = inputSubFeed.fileRefs.get
    outputSubFeed.copy(fileRefMapping = Some(output.translateFileRefs(inputFileRefs)))
  }


  override def writeSubFeed(subFeed: FileSubFeed, isRecursive: Boolean)(implicit session: SparkSession, context: ActionPipelineContext): WriteSubFeedResult = {
    val fileRefMapping = subFeed.fileRefMapping.getOrElse(throw new IllegalStateException(s"($id) file mapping is not defined"))
    output.startWritingOutputStreams(subFeed.partitionValues)
    if (fileRefMapping.nonEmpty) {
      import session.implicits._

      // Create a Dataset of files to be processed
      val srcDO = input // avoid serialization of whole action by assigning input to local variable
      srcDO.filesystem // init filesystem to prepare hadoop conf serialization
      val tgtDO = output // avoid serialization of whole action by assigning output to local variable
      tgtDO.filesystem // init filesystem to prepare hadoop conf serialization
      val transformerVal = transformer // avoid serialization of whole action by assigning transformer to local variable
      val filePathPairs = fileRefMapping.map{ m => (m.src.fullPath, m.tgt.fullPath)}
      val nbOfPartitions = math.max(filePathPairs.size / filesPerPartition, 1)
      val transformedDs = filePathPairs.toDS.repartition(nbOfPartitions)
        .map { case (srcPath, tgtPath) =>
          val result = TryWithRessource.exec(srcDO.filesystem.open(new Path(srcPath))) { is =>
            TryWithRessource.exec(tgtDO.filesystem.create(new Path(tgtPath), true)) { os => // overwrite = true
              transformerVal.transform(is, os)
            }
          }
          (srcPath, tgtPath, result.map(_.getMessage))
        }

      // execute the data set and log results
      val results = transformedDs.collect
      results.foreach { case (_, tgt, ex) =>
        if (ex.isEmpty) logger.info(s"transformed $tgt")
        else logger.error(s"transformed $tgt with error $ex")
      }
    }
    output.endWritingOutputStreams(subFeed.partitionValues)
    // return metric to action
    val metrics = Map("files_written"->fileRefMapping.size.toLong)
    WriteSubFeedResult(Some(fileRefMapping.isEmpty), Some(metrics))
  }

  override def factory: FromConfigFactory[Action] = CustomFileAction
}

object CustomFileAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomFileAction = {
    extract[CustomFileAction](config)
  }
}
