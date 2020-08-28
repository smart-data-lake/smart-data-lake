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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.ExecutionMode
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanHandlePartitions, CanWriteDataFrame, DataObject, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import scala.util.{Failure, Success, Try}

/**
 * [[Action]] to copy files (i.e. from stage to integration)
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param deleteDataAfterRead a flag to enable deletion of input partitions after copying.
 * @param transformer optional custom transformation to apply
 * @param columnBlacklist Remove all columns on blacklist from dataframe
 * @param columnWhitelist Keep only columns on whitelist in dataframe
 * @param additionalColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe.
 *                          The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 * @param executionMode optional execution mode for this Action
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class CopyAction(override val id: ActionObjectId,
                      inputId: DataObjectId,
                      outputId: DataObjectId,
                      deleteDataAfterRead: Boolean = false,
                      transformer: Option[CustomDfTransformerConfig] = None,
                      columnBlacklist: Option[Seq[String]] = None,
                      columnWhitelist: Option[Seq[String]] = None,
                      additionalColumns: Option[Map[String,String]] = None,
                      filterClause: Option[String] = None,
                      standardizeDatatypes: Boolean = false,
                      override val breakDataFrameLineage: Boolean = false,
                      override val persist: Boolean = false,
                      override val executionMode: Option[ExecutionMode] = None,
                      override val metricsFailCondition: Option[String] = None,
                      override val metadata: Option[ActionMetadata] = None
                     )(implicit instanceRegistry: InstanceRegistry) extends SparkSubFeedAction {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: DataObject with CanWriteDataFrame = getOutputDataObject[DataObject with CanWriteDataFrame](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[DataObject with CanWriteDataFrame] = Seq(output)

  // parse filter clause
  private val filterClauseExpr = Try(filterClause.map(expr)) match {
    case Success(result) => result
    case Failure(e) => throw new ConfigurationException(s"(${id}) Error parsing filterClause parameter as Spark expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
  }

  override def transform(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    applyTransformations(subFeed, transformer, columnBlacklist, columnWhitelist, additionalColumns, standardizeDatatypes, Seq(), filterClauseExpr)
  }

  override def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    if (deleteDataAfterRead) input match {
      // delete input partitions if applicable
      case (partitionInput: CanHandlePartitions) if partitionInput.partitions.nonEmpty && inputSubFeed.partitionValues.nonEmpty =>
        partitionInput.deletePartitions(inputSubFeed.partitionValues)
      // otherwise delete all
      case (fileInput: FileRefDataObject) =>
        fileInput.deleteAll
      case x => throw new IllegalStateException(s"($id) input ${input.id} doesn't support deleting data")
    }
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Action] = CopyAction
}

object CopyAction extends FromConfigFactory[Action] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): CopyAction = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._
    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[CopyAction].value
  }
}
