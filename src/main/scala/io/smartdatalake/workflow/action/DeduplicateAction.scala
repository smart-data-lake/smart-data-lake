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

import java.sql.Timestamp
import java.time.LocalDateTime

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{ExecutionMode, TechnicalTableColumn}
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, DataObject, TransactionalSparkTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * [[Action]] to deduplicate a subfeed.
 * Deduplication keeps the last record for every key, also after it has been deleted in the source.
 * It needs a transactional table as output with defined primary keys.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param ignoreOldDeletedColumns if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns from nested data types in Schema Evolution.
 *                                      Keeping deleted columns in complex data types has performance impact as all new data
 *                                      in the future has to be converted by a complex function.
 * @param initExecutionMode optional execution mode if this Action is a start node of a DAG run
 */
case class DeduplicateAction(override val id: ActionObjectId,
                             inputId: DataObjectId,
                             outputId: DataObjectId,
                             transformer: Option[CustomDfTransformerConfig] = None,
                             columnBlacklist: Option[Seq[String]] = None,
                             columnWhitelist: Option[Seq[String]] = None,
                             filterClause: Option[String] = None,
                             standardizeDatatypes: Boolean = false,
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

  require(output.table.primaryKey.isDefined, s"(${id}) Primary key must be defined for output DataObject")

  // parse filter clause
  private val filterClauseExpr = Try(filterClause.map(expr)) match {
    case Success(result) => result
    case Failure(e) => throw new ConfigurationException(s"(${id}) Error parsing filterClause parameter as Spark expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
  }

  override def transform(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // create input subfeeds if not yet existing
    var transformedSubFeed = ActionHelper.enrichSubFeedDataFrame(input, subFeed, runtimeExecutionMode(subFeed.isDAGStart))

    // apply transformations
    transformedSubFeed = ActionHelper.applyTransformations(
      transformedSubFeed, transformer, columnBlacklist, columnWhitelist, standardizeDatatypes, output, Some(deduplicateDataFrame(_: SparkSubFeed,_: Option[DataFrame],_:Seq[String],_: LocalDateTime)), filterClauseExpr)

    // return
    transformedSubFeed
  }

  /**
   * deduplicates a SubFeed.
   */
  private def deduplicateDataFrame(subFeed: SparkSubFeed,
                                   existingDf: Option[DataFrame],
                                   pks: Seq[String],
                                   refTimestamp: LocalDateTime)
                                  (implicit session: SparkSession): SparkSubFeed = {

    val enhancedDf = subFeed.dataFrame.get.withColumn(TechnicalTableColumn.captured.toString, ActionHelper.ts1(refTimestamp))

    // deduplicate
    val deduplicatedDf = if (existingDf.isDefined) {
      // apply schema evolution
      val (baseDf, newDf) = SchemaEvolution.process(existingDf.get, enhancedDf, ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns)
      deduplicate(baseDf, newDf, pks)
    } else enhancedDf

    subFeed.copy(dataFrame = Some(deduplicatedDf))
  }

  /**
   * deduplicate -> keep latest record per key
   *
   * @param baseDf existing data
   * @param newDf new data
   * @return deduplicated data
   */
  def deduplicate(baseDf: DataFrame, newDf:DataFrame, keyColumns: Seq[String])(implicit session: SparkSession) : DataFrame = {
    import session.implicits._
    import udfs._

    baseDf.union(newDf)
      .groupBy(keyColumns.map(col):_*)
      .agg(collect_list(struct("*")).as("rows"))
      .withColumn("latestRowIndex", udf_getLatestRowIndex($"rows"))
      .withColumn("latestRow", $"rows"($"latestRowIndex"))
      .select($"latestRow.*")
  }

  // keep udf's in separate object to avoid Spark serialization error for DeduplicateAction
  object udfs extends Serializable {
    private def getLatestRowIndex(rows: Seq[Row]): Int = {
      rows.map(_.getAs[Timestamp](TechnicalTableColumn.captured.toString)).zipWithIndex.maxBy(_._1.getTime)._2
    }
    val udf_getLatestRowIndex: UserDefinedFunction = udf(getLatestRowIndex _)
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Action] = DeduplicateAction
}

object DeduplicateAction extends FromConfigFactory[Action] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): DeduplicateAction = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._
    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[DeduplicateAction].value
  }
}