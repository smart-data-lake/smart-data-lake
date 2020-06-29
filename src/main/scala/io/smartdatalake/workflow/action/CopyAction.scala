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
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanHandlePartitions, CanWriteDataFrame, DataObject}
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
 * @param transformer a custom transformation that is applied to each SubFeed separately
 * @param initExecutionMode optional execution mode if this Action is a start node of a DAG run
 * @param executionMode optional execution mode for this Action
 */
case class CopyAction(override val id: ActionObjectId,
                      inputId: DataObjectId,
                      outputId: DataObjectId,
                      deleteDataAfterRead: Boolean = false,
                      transformer: Option[CustomDfTransformerConfig] = None,
                      columnBlacklist: Option[Seq[String]] = None,
                      columnWhitelist: Option[Seq[String]] = None,
                      filterClause: Option[String] = None,
                      standardizeDatatypes: Boolean = false,
                      override val breakDataFrameLineage: Boolean = false,
                      override val persist: Boolean = false,
                      override val initExecutionMode: Option[ExecutionMode] = None,
                      override val executionMode: Option[ExecutionMode] = None,
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

    // enrich DataFrames if not yet existing
    var transformedSubFeed = enrichSubFeedDataFrame(input, subFeed, runtimeExecutionMode(subFeed), context.phase)

    // apply transformations
    transformedSubFeed = applyTransformations(
      transformedSubFeed, transformer, columnBlacklist, columnWhitelist, standardizeDatatypes, output, None, filterClauseExpr)

    // return transformed subfeed
    transformedSubFeed
  }

  override def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    // delete input partitions if desired
    if (deleteDataAfterRead) (input, inputSubFeed) match {
      case (partitionInput: CanHandlePartitions, sparkSubFeed: SparkSubFeed) =>
        if (sparkSubFeed.partitionValues.nonEmpty) partitionInput.deletePartitions(sparkSubFeed.partitionValues)
      case x => throw new IllegalStateException(s"Unmatched case $x")
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
