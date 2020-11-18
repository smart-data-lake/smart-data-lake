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
 * @param transformer optional custom transformation to apply
 * @param columnBlacklist Remove all columns on blacklist from dataframe
 * @param columnWhitelist Keep only columns on whitelist in dataframe
 * @param additionalColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe.
 *                          The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 * @param ignoreOldDeletedColumns if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns from nested data types in Schema Evolution.
 *                                      Keeping deleted columns in complex data types has performance impact as all new data
 *                                      in the future has to be converted by a complex function.
 * @param executionMode optional execution mode for this Action
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class DeduplicateAction(override val id: ActionObjectId,
                             inputId: DataObjectId,
                             outputId: DataObjectId,
                             transformer: Option[CustomDfTransformerConfig] = None,
                             columnBlacklist: Option[Seq[String]] = None,
                             columnWhitelist: Option[Seq[String]] = None,
                             additionalColumns: Option[Map[String,String]] = None,
                             filterClause: Option[String] = None,
                             standardizeDatatypes: Boolean = false,
                             ignoreOldDeletedColumns: Boolean = false,
                             ignoreOldDeletedNestedColumns: Boolean = true,
                             override val breakDataFrameLineage: Boolean = false,
                             override val persist: Boolean = false,
                             override val executionMode: Option[ExecutionMode] = None,
                             override val metricsFailCondition: Option[String] = None,
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
    val timestamp = context.referenceTimestamp.getOrElse(LocalDateTime.now)
    val pks = output.table.primaryKey
      .getOrElse( throw new ConfigurationException(s"There is no <primary-keys> defined for table ${output.table.name}."))
    val existingDf = if (output.isTableExisting) {
      Some(output.getDataFrame())
    } else None
    val deduplicateTransformer = DeduplicateAction.deduplicateDataFrame(existingDf, pks, timestamp, ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns) _
    applyTransformations(subFeed, transformer, columnBlacklist, columnWhitelist, additionalColumns, standardizeDatatypes, Seq(deduplicateTransformer), filterClauseExpr)
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

  /**
   * deduplicates a SubFeed.
   */
  def deduplicateDataFrame(existingDf: Option[DataFrame], pks: Seq[String], refTimestamp: LocalDateTime, ignoreOldDeletedColumns: Boolean, ignoreOldDeletedNestedColumns: Boolean)(df: DataFrame)(implicit session: SparkSession): DataFrame = {
    // enhance
    val enhancedDf = df.withColumn(TechnicalTableColumn.captured.toString, ActionHelper.ts1(refTimestamp))

    // deduplicate
    if (existingDf.isDefined) {
      // apply schema evolution
      val (baseDf, newDf) = SchemaEvolution.process(existingDf.get, enhancedDf, ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns)
      // Schema evolution puts new columns at the end of the column list, so we need to move the technical captured column back to the end
      deduplicate(baseDf, newDf, pks)
    } else enhancedDf
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

    baseDf.unionByName(newDf)
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
}