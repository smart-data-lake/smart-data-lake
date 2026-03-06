/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.testutils

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.ProductUtil
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark._
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.jdk.CollectionConverters._

/**
 * Partitioned transactional mock data object.
 * Set dataFrame and partitionValues to be served by using writeSparkDataFrame.
 * PartitionValues are inferred if parameter of writeSparkDataFrame is empty.
 */
case class MockSparkDataObject(override val id: DataObjectId,
                               override val partitions: Seq[String] = Seq(),
                               override val schemaMin: Option[GenericSchema] = None,
                               primaryKey: Option[Seq[String]] = None,
                               tableName: String = "mock",
                               override val constraints: Seq[Constraint] = Seq(),
                               override val expectations: Seq[Expectation] = Seq(),
                               saveMode: SDLSaveMode = SDLSaveMode.Overwrite
                              )
  extends TransactionalTableDataObject with CanCreateSparkDataFrame with CanWriteSparkDataFrame
    with CanHandlePartitions with ExpectationValidation with CanMergeDataFrame {
  assert(partitions.isEmpty || saveMode == SDLSaveMode.Overwrite, s"($id) Only saveMode=Overwrite implemented for partitioned MockDataObjects")
  assert(saveMode == SDLSaveMode.Overwrite || saveMode == SDLSaveMode.Append, s"($id) Only saveMode=Overwrite or saveMode=Append implemented for MockDataObjects")

  // variables to store mock values. They are filled using writeSparkDataFrame
  private var dataFrameMock: Option[DataFrame] = None
  private var partitionedDataFrameMock: Option[Map[PartitionValues, DataFrame]] = None
  private var partitionValuesMock: Set[PartitionValues] = Set()

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = partitionValuesMock.toSeq

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    if (partitions.nonEmpty) {
      partitionedDataFrameMock
        .map(_.filterKeys(pv => partitionValues.isEmpty || partitionValues.exists(pv.isIncludedIn)).values.reduce(_ unionAll _))
        .orElse(schemaMin.map(subFeedCompanion.getEmptyDataFrame(_, id).asInstanceOf[SparkDataFrame].inner))
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) partitionedDataFrameMock not initialized"))
    } else {
      dataFrameMock
        .orElse(schemaMin.map(subFeedCompanion.getEmptyDataFrame(_, id).asInstanceOf[SparkDataFrame].inner))
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) dataFrameMock not initialized"))
    }
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    val genericDf = SparkDataFrame(df)
    validateSchemaMin(genericDf.schema, "write")
    validateSchemaHasPartitionCols(df, "write")
    validateSchemaHasPrimaryKeyCols(df, "write")
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(genericDf)).getOrElse(genericDf).inner
    if (!isTableExisting) {
      if (partitions.nonEmpty) partitionedDataFrameMock = Some(Map(partitionValues.head -> saveModeTargetDf.where(lit(false))))
      else dataFrameMock = Some(saveModeTargetDf.where(lit(false)))
    }
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitionValues.flatMap(_.keys).distinct.diff(partitions).isEmpty, s"($id) partitionValues keys dont match partition columns") // assert partition keys match
    assert(partitions.diff(df.columns).isEmpty, s"($id) partition columns are missing in DataFrame")
    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // recreate DataFrame to truncate logical plan to avoid side-effects in tests
    // this also force evaluates constraints and triggers RuntimeFailTransformer
    val (newDf, insertCnt) = materialize(df)

    if (partitions.nonEmpty) {
      finalSaveMode match {
        case SDLSaveMode.Overwrite =>
          // mimick partition overwrite
          val inferredPartitionValues = if (partitionValues.isEmpty && partitions.nonEmpty) PartitionValues.fromDataFrame(SparkDataFrame(df.select(partitions.map(col): _*)))
          else partitionValues
          val newDataFrames = inferredPartitionValues.map(pv => (pv, newDf.where(getPartitionValueFilter(pv)))).toMap
          if (newDataFrames.nonEmpty) {
            partitionedDataFrameMock = Some(
              partitionedDataFrameMock.getOrElse(Map()) ++ newDataFrames
            )
            partitionValuesMock = partitionValuesMock ++ inferredPartitionValues
            dataFrameMock = None
          }
          Map("records_written" -> insertCnt)
      }
    } else {
      val metrics = finalSaveMode match {
        case SDLSaveMode.Overwrite =>
          dataFrameMock = Some(newDf)
          Map("records_written" -> insertCnt)
        case SDLSaveMode.Append =>
          dataFrameMock = Some(Seq(dataFrameMock, Some(newDf)).flatten.reduceLeft(_.unionByName(_)))
          Map("records_written" -> insertCnt)
        case SDLSaveMode.Merge  =>
          val (dfMerged, metrics) = mergeDataFrameByPrimaryKey(newDf, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions)
            .getOrElse(SaveModeMergeOptions()))
          dataFrameMock = Some(dfMerged)
          metrics
      }
      partitionValuesMock = Set()
      partitionedDataFrameMock = None
      metrics
    }
  }

  private def materialize(df: DataFrame): (DataFrame, Int) = {
    val records = df.collect().toSeq
    val dfMaterialized = df.sparkSession.createDataFrame(records.asJava, df.schema)
    (dfMaterialized, records.length)
  }

  def mergeDataFrameByPrimaryKey(dfNew: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): (DataFrame, MetricsMap) = {
    val session = context.sparkSession
    import session.implicits._
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")
    val saveModeExpr = saveModeOptions.getExpressions(SparkSubFeed.subFeedType)

    val dfExisting = dataFrameMock
      .getOrElse(SparkSubFeed.getEmptyDataFrame(SparkSchema(dfNew.drop(saveModeOptions.insertColumnsToIgnore:_*).schema), id).inner)
      .as("existing")
    val existingColumns = dfExisting.columns
    val targetColumns = (existingColumns ++ dfNew.columns.diff(saveModeOptions.insertColumnsToIgnore)).distinct

    // prepare join condition
    val pkCols = table.primaryKey.get
    val joinCondition = pkCols.map(colName => col(s"new.$colName") === col(s"existing.$colName")).reduce(_ and _)
      .and(saveModeExpr.additionalMergePredicateExpr.map(_.asInstanceOf[SparkColumn].inner).getOrElse(lit(true)))
    val dfJoined = dfExisting.join(dfNew.as("new"), joinCondition, "full")
    var dfMatched = dfJoined.where($"new.${pkCols.head}".isNotNull and $"existing.${table.primaryKey.get.head}".isNotNull)
      .observe("merge1", count("*").as("rows_matched"))
    val dfNewNotMatched = dfJoined.where($"new.${pkCols.head}".isNotNull and $"existing.${table.primaryKey.get.head}".isNull)
      .select($"new.*")
    val dfExistingNotMatched = dfJoined.where($"new.${pkCols.head}".isNull and $"existing.${table.primaryKey.get.head}".isNotNull)
      .select($"existing.*")

    // remove records from dfMatched if deleteCondition is defined
    saveModeExpr.deleteConditionExpr.foreach{
      c => dfMatched = dfMatched.where(not(c.asInstanceOf[SparkColumn].inner))
        .observe("merge2", count("*").as("rows_matched_without_deleted"))
    }

    // update records
    val updateCols = saveModeOptions.updateColumnsOpt.getOrElse(dfNew.columns.toSeq.diff(table.primaryKey.get))
    val updateSelectCols = targetColumns
      .map(c => (if (updateCols.contains(c)) col(s"new.$c") else if (existingColumns.contains(c)) col(s"existing.$c") else lit(null)).as(c))
    val updateCondition = saveModeExpr.updateConditionExpr.map(_.asInstanceOf[SparkColumn].inner).getOrElse(lit(true))
    val dfUpdated = dfMatched.where(updateCondition).select(updateSelectCols: _*)
      .observe("merge3", count("*").as("rows_updated"))
    val dfNotUpdated = dfMatched.where(not(updateCondition)).select($"existing.*")

    // update existing records
    val (dfUpdated2, dfNotUpdated2) = if (saveModeOptions.updateExistingCondition.isDefined) {
      val updateCols = dfNew.columns.toSeq.diff(Seq(Historization.historizeOperationColName))
      val updateSelectCols = targetColumns
        .map(c => (if (updateCols.contains(c)) col(s"new.$c") else if (existingColumns.contains(c)) col(s"existing.$c") else lit(null)).as(c))
      val updateExistingCondition = saveModeExpr.updateExistingConditionExpr.get.asInstanceOf[SparkColumn].inner
      val dfUpdatedExisting = dfMatched.where(updateExistingCondition and not(updateCondition)).select(updateSelectCols: _*)
        .observe(s"merge4", count("*").as("rows_updated_existing"))
      val dfNotUpdated = dfMatched.where(not(updateExistingCondition) and not(updateCondition)).select($"existing.*")
      (dfUpdated.unionByName(dfUpdatedExisting), dfNotUpdated)
    } else (dfUpdated, dfNotUpdated)
    var dfMerged = dfUpdated2.unionByName(dfNotUpdated2, allowMissingColumns = true)

    // add insert clause - insertExpr does not support referring new columns in existing table on schema evolution, that's why we use it only when needed, and insertAll otherwise
    val insertCols = dfNew.columns.diff(saveModeOptions.insertColumnsToIgnore)
    val insertSelectCols = targetColumns
      .map(c => saveModeOptions.insertValuesOverride.get(c).map(expr).getOrElse(if (insertCols.contains(c)) col(s"new.$c").as(c) else lit(null)).as(c))
    val insertCondition = saveModeExpr.insertConditionExpr.map(_.asInstanceOf[SparkColumn].inner).getOrElse(lit(true))
    val dfInsert = dfNewNotMatched.where(insertCondition).select(insertSelectCols: _*)
      .observe("merge5", count("*").as("rows_inserted"))
    dfMerged = dfMerged
      .unionByName(dfInsert)
      .unionByName(dfExistingNotMatched, allowMissingColumns = true)
    logger.info(s"($id) created merged DataFrame with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1 + "=" + e._2).mkString(" ")}")

    // collect metrics and materialize
    val observation = new SparkObservation("final")
      .setOtherObservationsPrefix("merge")
    val dfFinal = observation.on(dfMerged, registerListener = true, count("*").as("count"))
    val (dfMaterialized, _) = materialize(dfFinal)
    val metrics = observation.waitFor()
      .map{ case (k, v) => k.split('#').head -> v } // remove postfix from metrics keys
    (dfMaterialized, metrics)
  }

  def register(implicit instanceRegistry: InstanceRegistry): MockSparkDataObject = {
    instanceRegistry.register(this)
    this
  }

  override private[smartdatalake] def expectedPartitionsCondition: Option[String] = None

  override val metadata: Option[DataObjectMetadata] = None
  override val options: Map[String, String] = Map()

  override var table: Table = Table(Some("mock"), tableName, primaryKey = primaryKey)

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = true

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    if (partitions.nonEmpty) partitionValuesMock.nonEmpty
    else dataFrameMock.isDefined
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    partitionValuesMock = Set()
    dataFrameMock = None
    partitionedDataFrameMock = None
  }

  private implicit val subFeedCompanion: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(SparkSubFeed.subFeedType)

  private def getPartitionValueFilter(pv: PartitionValues) = pv.getFilterExpr.asInstanceOf[SparkColumn].inner

  override def factory: FromConfigFactory[DataObject] = MockSparkDataObject

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {}


}

object MockSparkDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MockSparkDataObject = {
    extract[MockSparkDataObject](config)
  }
}

