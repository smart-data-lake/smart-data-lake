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
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.ExecutionMode
import io.smartdatalake.util.filetransfer.FileTransfer
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed}
import org.apache.spark.sql.SparkSession

/**
 * [[Action]] to transfer files between SFtp, Hadoop and local Fs.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param deleteDataAfterRead if the input files should be deleted after processing successfully
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class FileTransferAction(override val id: ActionObjectId,
                              inputId: DataObjectId,
                              outputId: DataObjectId,
                              override val deleteDataAfterRead: Boolean = false,
                              overwrite: Boolean = true,
                              override val breakFileRefLineage: Boolean = false,
                              override val executionMode: Option[ExecutionMode] = None,
                              override val metricsFailCondition: Option[String] = None,
                              override val metadata: Option[ActionMetadata] = None)
                             ( implicit instanceRegistry: InstanceRegistry)
  extends FileSubFeedAction {

  override val input: FileRefDataObject with CanCreateInputStream = getInputDataObject[FileRefDataObject with CanCreateInputStream](inputId)
  override val output: FileRefDataObject with CanCreateOutputStream = getOutputDataObject[FileRefDataObject with CanCreateOutputStream](outputId)
  override val inputs: Seq[FileRefDataObject] = Seq(input)
  override val outputs: Seq[FileRefDataObject] = Seq(output)

  // initialize FileTransfer
  private val fileTransfer = FileTransfer(input, output, deleteDataAfterRead, overwrite)

  override def doTransform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed, doExec: Boolean)(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed = {
    // recreate FileRefs if needed
    val inputFileRefs = inputSubFeed.fileRefs.getOrElse(input.getFileRefs(inputSubFeed.partitionValues))
    val fileRefPairs = fileTransfer.init(inputFileRefs)
    if (doExec) fileTransfer.exec(fileRefPairs)
    outputSubFeed.copy(fileRefs = Some(fileRefPairs.map(_._2)), processedInputFileRefs = Some(inputFileRefs))
  }

  override def factory: FromConfigFactory[Action] = FileTransferAction
}

object FileTransferAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FileTransferAction = {
    extract[FileTransferAction](config)
  }
}
