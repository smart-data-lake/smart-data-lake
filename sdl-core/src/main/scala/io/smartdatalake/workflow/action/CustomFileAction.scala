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
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class CustomFileAction(override val id: ActionObjectId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            transformer: CustomFileTransformerConfig,
                            override val deleteDataAfterRead: Boolean = false,
                            filesPerPartition: Int = 10,
                            override val breakFileRefLineage: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends FileSubFeedAction with SmartDataLakeLogger {

  assert(filesPerPartition>0, s"($id) filesPerPartition must be greater than 0. Current value: $filesPerPartition")

  override val input: HadoopFileDataObject = getInputDataObject[HadoopFileDataObject](inputId)
  override val output: HadoopFileDataObject = getOutputDataObject[HadoopFileDataObject](outputId)
  override val inputs: Seq[HadoopFileDataObject] = Seq(input)
  override val outputs: Seq[HadoopFileDataObject] = Seq(output)

  override def doTransform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed, doExec: Boolean)(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed = {
    import session.implicits._

    // recreate FileRefs is desired
    val inputFileRefs = inputSubFeed.fileRefs.getOrElse( input.getFileRefs(inputSubFeed.partitionValues))
    val tgtFileRefs = output.translateFileRefs(inputFileRefs)

    // transform files in distributed mode with Spark
    if (doExec) {
      // Create a Dataset of files to be processed
      val srcDO = input // avoid serialization of whole action by assigning input to local variable
      srcDO.filesystem // init filesystem to prepare hadoop conf serialization
      val tgtDO = output // avoid serialization of whole action by assigning output to local variable
      tgtDO.filesystem // init filesystem to prepare hadoop conf serialization
      val transformerVal = transformer // avoid serialization of whole action by assigning transformer to local variable
      val filePathPairs = inputFileRefs.map(_.fullPath).zip(tgtFileRefs.map(_.fullPath))
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

    // return
    outputSubFeed.copy(fileRefs = Some(tgtFileRefs), processedInputFileRefs = Some(inputFileRefs))
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Action] = CustomFileAction
}

object CustomFileAction extends FromConfigFactory[Action] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): CustomFileAction = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._
    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[CustomFileAction].value
  }
}
