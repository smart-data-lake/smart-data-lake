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

import java.time.LocalDateTime

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{Condition, ExecutionMode, TechnicalTableColumn}
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, DataObject, TransactionalSparkTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * [[Action]] to historize a subfeed.
 * Historization creates a technical history of data by creating valid-from/to columns.
 * It needs a transactional table as output with defined primary keys.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param filterClause filter of data to be processed by historization. It can be used to exclude historical data not needed to create new history, for performance reasons.
 * @param historizeBlacklist optional list of columns to ignore when comparing two records in historization. Can not be used together with [[historizeWhitelist]].
 * @param historizeWhitelist optional final list of columns to use when comparing two records in historization. Can not be used together with [[historizeBlacklist]].
 * @param ignoreOldDeletedColumns if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns from nested data types in Schema Evolution.
 *                                      Keeping deleted columns in complex data types has performance impact as all new data
 *                                      in the future has to be converted by a complex function.
 * @param transformer optional custom transformation to apply
 * @param columnBlacklist Remove all columns on blacklist from dataframe
 * @param columnWhitelist Keep only columns on whitelist in dataframe
 * @param additionalColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe.
 *                          The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 * @param executionMode optional execution mode for this Action
 * @param executionCondition optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class HistorizeAction(
                            override val id: ActionId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            transformer: Option[CustomDfTransformerConfig] = None,
                            columnBlacklist: Option[Seq[String]] = None,
                            columnWhitelist: Option[Seq[String]] = None,
                            additionalColumns: Option[Map[String,String]] = None,
                            standardizeDatatypes: Boolean = false,
                            filterClause: Option[String] = None,
                            historizeBlacklist: Option[Seq[String]] = None,
                            historizeWhitelist: Option[Seq[String]] = None,
                            ignoreOldDeletedColumns: Boolean = false,
                            ignoreOldDeletedNestedColumns: Boolean = true,
                            override val breakDataFrameLineage: Boolean = false,
                            override val persist: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val executionCondition: Option[Condition] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None
                          )(implicit instanceRegistry: InstanceRegistry) extends SparkSubFeedAction {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: TransactionalSparkTableDataObject = getOutputDataObject[TransactionalSparkTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalSparkTableDataObject] = Seq(output)

  // Output is used as recursive input in DeduplicateAction to get existing data. This override is needed to force tick-tock write operation.
  override val recursiveInputs: Seq[TransactionalSparkTableDataObject] = Seq(output)

  // historize black/white list
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty, s"(${id}) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // primary key
  require(output.table.primaryKey.isDefined, s"(${id}) Primary key must be defined for output DataObject")

  // parse filter clause
  private val filterClauseExpr = Try(filterClause.map(expr)) match {
    case Success(result) => result
    case Failure(e) => throw new ConfigurationException(s"(${id}) Error parsing filterClause parameter as Spark expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
  }

  override def transform(inputSubFeed: SparkSubFeed, outputSubFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    val timestamp = context.referenceTimestamp.getOrElse(LocalDateTime.now)
    val pks = output.table.primaryKey.get // existance is validated earlier
    // get existing data
    // Note that HistorizeAction needs to read/write all existing data for tick-tock operation, even if only specific partitions have changed
    val existingDf = if (output.isTableExisting) Some(output.getDataFrame())
    else None
    val historizeTransformer = historizeDataFrame(existingDf, pks, timestamp) _
    val transformedDf = applyTransformations(inputSubFeed, transformer, columnBlacklist, columnWhitelist, additionalColumns, standardizeDatatypes, Seq(historizeTransformer), filterClauseExpr)
    outputSubFeed.copy(dataFrame = Some(transformedDf))
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    if (transformer.isDefined) transformer.get.transformPartitionValues(id, partitionValues)
    else PartitionValues.oneToOneMapping(partitionValues)
  }

  protected def historizeDataFrame(existingDf: Option[DataFrame], pks: Seq[String], refTimestamp: LocalDateTime)(newDf: DataFrame)(implicit session: SparkSession): DataFrame = {

    val newFeedDf = newDf.dropDuplicates(pks)

    // if output does not yet exist, we just transform the new data into historized form
    if (existingDf.isDefined) {
      ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get.where(filterClauseExpr.getOrElse(lit(true))), TechnicalTableColumn.captured.toString)
      // apply schema evolution
      val (modifiedExistingDf, modifiedNewFeedDf) = SchemaEvolution.process(existingDf.get, newFeedDf, ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns
        , colsToIgnore = Seq(TechnicalTableColumn.captured.toString, TechnicalTableColumn.delimited.toString))
      // filter existing data to be excluded from historize operation
      val (filteredExistingDf, filteredExistingRemainingDf) =
        filterClauseExpr match {
          case Some(expr) => (modifiedExistingDf.where(expr), Some(modifiedExistingDf.where(not(expr))))
          case None => (modifiedExistingDf, None)
        }
      // historize
      val historizedDf = Historization.getHistorized(filteredExistingDf, modifiedNewFeedDf, pks, refTimestamp, historizeWhitelist, historizeBlacklist)
      // union with filter remaining df and return
      if (filteredExistingRemainingDf.isDefined) historizedDf.union(filteredExistingRemainingDf.get)
      else historizedDf
    } else  Historization.getInitialHistory(newFeedDf, refTimestamp)
  }

  override def factory: FromConfigFactory[Action] = HistorizeAction
}

object HistorizeAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HistorizeAction = {
    extract[HistorizeAction](config)
  }
}