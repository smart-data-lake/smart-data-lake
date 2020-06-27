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
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{ExecutionMode, TechnicalTableColumn}
import io.smartdatalake.util.evolution.SchemaEvolution
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
 * @param initExecutionMode optional execution mode if this Action is a start node of a DAG run
 */
case class HistorizeAction(
                            override val id: ActionObjectId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            transformer: Option[CustomDfTransformerConfig] = None,
                            columnBlacklist: Option[Seq[String]] = None,
                            columnWhitelist: Option[Seq[String]] = None,
                            standardizeDatatypes: Boolean = false,
                            filterClause: Option[String] = None,
                            historizeBlacklist: Option[Seq[String]] = None,
                            historizeWhitelist: Option[Seq[String]] = None,
                            ignoreOldDeletedColumns: Boolean = false,
                            ignoreOldDeletedNestedColumns: Boolean = true,
                            override val breakDataFrameLineage: Boolean = false,
                            override val persist: Boolean = false,
                            override val initExecutionMode: Option[ExecutionMode] = None,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val metadata: Option[ActionMetadata] = None
                          )(implicit instanceRegistry: InstanceRegistry) extends SparkSubFeedAction {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: TransactionalSparkTableDataObject = getOutputDataObject[TransactionalSparkTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalSparkTableDataObject] = Seq(output)

  // historize black/white list
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty, s"(${id}) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // primary key
  require(output.table.primaryKey.isDefined, s"(${id}) Primary key must be defined for output DataObject")

  // parse filter clause
  private val filterClauseExpr = Try(filterClause.map(expr)) match {
    case Success(result) => result
    case Failure(e) => throw new ConfigurationException(s"(${id}) Error parsing filterClause parameter as Spark expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
  }

  def transform(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // create input subfeeds if not yet existing
    var transformedSubFeed = enrichSubFeedDataFrame(input, subFeed, runtimeExecutionMode(subFeed.isDAGStart), context.phase)

    // apply transformations
    transformedSubFeed = applyTransformations(
      transformedSubFeed, transformer, columnBlacklist, columnWhitelist, standardizeDatatypes, output, Some(historizeDataFrame(_: SparkSubFeed,_: Option[DataFrame],_:Seq[String],_: LocalDateTime)), filterClauseExpr)

    // return
    transformedSubFeed
  }

  protected def historizeDataFrame( subFeed: SparkSubFeed,
                                    existingDf: Option[DataFrame],
                                    pks: Seq[String],
                                    refTimestamp: LocalDateTime)
                                  (implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {

    val newFeedDf = subFeed.dataFrame.get.dropDuplicates(pks)

    // if output does not yet exist, we just transform the new data into historized form
    val historyDf = if (existingDf.isDefined) {
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

    subFeed.copy(dataFrame = Some(historyDf))
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Action] = HistorizeAction
}

object HistorizeAction extends FromConfigFactory[Action] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): HistorizeAction = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._
    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[HistorizeAction].value
  }
}