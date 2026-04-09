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
import io.smartdatalake.workflow.dataframe.plainScala.{ScalaAbstractColumn, ScalaDataFrame, ScalaSchema, ScalaSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanHandlePartitions, CanMergeDataFrame, CanWriteDataFrame, Constraint, ExpectationValidation, Table, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Partitioned transactional mock data object.
 * Set dataFrame and partitionValues to be served by using writeScalaDataFrame.
 * PartitionValues are inferred if parameter of writeScalaDataFrame is empty.
 *
 * TODO: implement CanCreateIncrementalOutput, so that CopyActionTest can be migrated
 */
case class MockScalaDataObject(override val id: DataObjectId, override val partitions: Seq[String] = Seq(),
                          override val schemaMin: Option[GenericSchema] = None, primaryKey: Option[Seq[String]] = None, tableName: String = "mock",
                          override val constraints: Seq[Constraint] = Seq(),
                          override val expectations: Seq[Expectation] = Seq(),
                          saveMode: SDLSaveMode = SDLSaveMode.Overwrite
                              )
  extends DataObject with TransactionalTableDataObject with CanCreateDataFrame with CanWriteDataFrame
    with CanHandlePartitions with ExpectationValidation with CanMergeDataFrame {
  assert(partitions.isEmpty || saveMode==SDLSaveMode.Overwrite, s"($id) Only saveMode=Overwrite implemented for partitioned MockDataObjects")
  assert(saveMode==SDLSaveMode.Overwrite || saveMode==SDLSaveMode.Append, s"($id) Only saveMode=Overwrite or saveMode=Append implemented for MockDataObjects")

  // variables to store mock values. They are filled using writeSparkDataFrame
  private var dataFrameMock: Option[ScalaDataFrame] = None
  private var partitionedDataFrameMock: Option[Map[PartitionValues, ScalaDataFrame]] = None
  private var partitionValuesMock: Set[PartitionValues] = Set()

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = partitionValuesMock.toSeq

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = ScalaSubFeed.subFeedType)(implicit context: ActionPipelineContext): GenericDataFrame = {
    if (subFeedType =:= typeOf[ScalaSubFeed]) getScalaDataFrame(partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    if (subFeedType =:= typeOf[ScalaSubFeed]) ScalaSubFeed(Some(getScalaDataFrame(partitionValues)), id, partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[ScalaSubFeed])

  def getScalaDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): ScalaDataFrame = {
    if (partitions.nonEmpty) {
      partitionedDataFrameMock
        .map(_.filterKeys(pv => partitionValues.isEmpty || partitionValues.exists(pv.isIncludedIn)).values.reduce(_ unionByName _))
        .orElse(dataFrameMock) // dataFrameMock can be initialized with an empty DataFrame for partitioned MockDataObject if no partitionValues are provided in initSparkDataFrame
        .orElse(schemaMin.map(subFeedCompanion.getEmptyDataFrame(_, id).asInstanceOf[ScalaDataFrame]))
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) partitionedDataFrameMock not initialized"))
    } else {
      dataFrameMock
        .orElse(schemaMin.map(subFeedCompanion.getEmptyDataFrame(_, id).asInstanceOf[ScalaDataFrame]))
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) dataFrameMock not initialized"))
    }
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    df match {
      case scalaDf: ScalaDataFrame => initScalaDataFrame(scalaDf, partitionValues, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method init")
    }
  }

  def initScalaDataFrame(df: ScalaDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    import functions._
    validateSchemaMin(df.schema, "write")
    //validateSchemaHasPartitionCols(df, "write")
    //validateSchemaHasPrimaryKeyCols(df, "write")
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df)
    if (!isTableExisting) {
      // Note: it's possible that dataFrameMock is initialized with an empty DataFrame, if no partitionValues are provided for a partitioned MockDataObject
      if (partitions.nonEmpty && partitionValues.nonEmpty) partitionedDataFrameMock = Some(Map(partitionValues.head -> saveModeTargetDf.where(lit(false)).asInstanceOf[ScalaDataFrame]))
      else dataFrameMock = Some(saveModeTargetDf.where(lit(false)).asInstanceOf[ScalaDataFrame])
    }
  }

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    df match {
      case scalaDf: ScalaDataFrame => writeScalaDataFrame(scalaDf, partitionValues, isRecursiveInput, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeDataFrame")
    }
  }

  override private[smartdatalake] def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[ScalaSubFeed])

  def writeScalaDataFrame(df: ScalaDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitionValues.flatMap(_.keys).distinct.diff(partitions).isEmpty, s"($id) partitionValues keys dont match partition columns") // assert partition keys match
    assert(partitions.diff(df.columns).isEmpty, s"($id) partition columns are missing in DataFrame")
    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    import functions._

    val insertCnt = df.count

    if (partitions.nonEmpty) {
      finalSaveMode match {
        case SDLSaveMode.Overwrite =>
          // mimick partition overwrite
          val inferredPartitionValues = if (partitionValues.isEmpty && partitions.nonEmpty) PartitionValues.fromDataFrame(df.select(partitions.map(col)))
          else partitionValues
          val newDataFrames = inferredPartitionValues.map(pv => (pv, df.filter(getPartitionValueFilter(pv)))).toMap
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
          dataFrameMock = Some(df)
          Map("records_written" -> insertCnt)
        case SDLSaveMode.Append =>
          dataFrameMock = Some(Seq(dataFrameMock, Some(df)).flatten.reduceLeft(_.unionByName(_)))
          Map("records_written" -> insertCnt)
        case SDLSaveMode.Merge  =>
          val (dfMerged, metrics) = mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions)
            .getOrElse(SaveModeMergeOptions()))
          dataFrameMock = Some(dfMerged)
          metrics
      }
      partitionValuesMock = Set()
      partitionedDataFrameMock = None
      metrics
    }
  }

  def mergeDataFrameByPrimaryKey(dfNew: ScalaDataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): (ScalaDataFrame, MetricsMap) = {
    import functions._
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")
    val saveModeExpr = saveModeOptions.getExpressions(ScalaSubFeed.subFeedType)

    val dfExisting = dataFrameMock
      .getOrElse(ScalaSubFeed.getEmptyDataFrame(dfNew.drop(saveModeOptions.insertColumnsToIgnore).schema, id))
      .as("existing")
    val existingColumns = dfExisting.columns
    val targetColumns = (existingColumns ++ dfNew.columns.diff(saveModeOptions.insertColumnsToIgnore)).distinct

    // prepare join condition
    val pkCols = table.primaryKey.get
    val joinCondition = pkCols.map(colName => col(s"new.$colName") === col(s"existing.$colName")).reduce(_ and _)
      .and(saveModeExpr.additionalMergePredicateExpr.getOrElse(lit(true)))
    val dfJoined = dfExisting.join(dfNew.as("new"), joinCondition, "full")
    var dfMatched = dfJoined.where(col(s"new.${pkCols.head}").isNotNull and col(s"existing.${table.primaryKey.get.head}").isNotNull)
    val dfNewNotMatched = dfJoined.where(col(s"new.${pkCols.head}").isNotNull and col(s"existing.${table.primaryKey.get.head}").isNull)
      .select(col("new.*"))
    val dfExistingNotMatched = dfJoined.where(col(s"new.${pkCols.head}").isNull and col(s"existing.${table.primaryKey.get.head}").isNotNull)
      .select(col("existing.*"))

    // remove records from dfMatched if deleteCondition is defined
    saveModeExpr.deleteConditionExpr.foreach{
      c => dfMatched = dfMatched.where(not(c))
    }

    // update records
    val updateCols = saveModeOptions.updateColumnsOpt.getOrElse(dfNew.columns.toSeq.diff(table.primaryKey.get))
    val updateSelectCols = targetColumns
      .map(c => (if (updateCols.contains(c)) col(s"new.$c") else if (existingColumns.contains(c)) col(s"existing.$c") else lit(null)).as(c))
    val updateCondition = saveModeExpr.updateConditionExpr.getOrElse(lit(true))
    val dfUpdated = dfMatched.where(updateCondition).select(updateSelectCols)
    val dfNotUpdated = dfMatched.where(not(updateCondition)).select(col("existing.*"))

    // update existing records
    val (dfUpdated2, dfNotUpdated2) = if (saveModeOptions.updateExistingCondition.isDefined) {
      val updateCols = dfNew.columns.toSeq.diff(Seq(Historization.historizeOperationColName))
      val updateSelectCols = targetColumns
        .map(c => (if (updateCols.contains(c)) col(s"new.$c") else if (existingColumns.contains(c)) col(s"existing.$c") else lit(null)).as(c))
      val updateExistingCondition = saveModeExpr.updateExistingConditionExpr.get
      val dfUpdatedExisting = dfMatched.where(updateExistingCondition and not(updateCondition)).select(updateSelectCols)
      val dfNotUpdated = dfMatched.where(not(updateExistingCondition) and not(updateCondition)).select(col("existing.*"))
      (dfUpdated.unionByName(dfUpdatedExisting), dfNotUpdated)
    } else (dfUpdated, dfNotUpdated)
    var dfMerged = dfUpdated2.unionByName(dfNotUpdated2, allowMissingColumns = true)

    // add insert clause - insertExpr does not support referring new columns in existing table on schema evolution, that's why we use it only when needed, and insertAll otherwise
    val insertCols = dfNew.columns.diff(saveModeOptions.insertColumnsToIgnore)
    val insertSelectCols = targetColumns
      .map(c => saveModeOptions.insertValuesOverride.get(c).map(expr).getOrElse(if (insertCols.contains(c)) col(s"new.$c").as(c) else lit(null)).as(c))
    val dfInsert = dfNewNotMatched.where(saveModeExpr.insertConditionExpr.getOrElse(lit(true))).select(insertSelectCols)
    val dfMerged2 = dfMerged
      .unionByName(dfInsert)
      .unionByName(dfExistingNotMatched, allowMissingColumns = true)
    logger.info(s"($id) created merged DataFrame with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1 + "=" + e._2).mkString(" ")}")

    // collect metrics and materialize
    val metrics = Map(
      "records_updated" -> dfUpdated.count,
      "records_not_updated" -> dfNotUpdated.count,
      // TODO
      //"records_updated_existing" -> (if (saveModeOptions.updateExistingCondition.isDefined) dfUpdated2.except(dfUpdated).count else 0L),
      "records_inserted" -> dfInsert.count,
      // TODO
      //"records_deleted" -> (if (saveModeOptions.deleteConditionExpr.isDefined) dfMatched.where(saveModeOptions.deleteConditionExpr.get).count else 0L)
    )
    (dfMerged2.asInstanceOf[ScalaDataFrame], metrics)
  }

  def register(implicit instanceRegistry: InstanceRegistry): MockScalaDataObject = {
    instanceRegistry.register(this)
    this
  }

  override private[smartdatalake] def expectedPartitionsCondition: Option[String] = None
  override val metadata: Option[DataObjectMetadata] = None

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

  private lazy val functions = DataFrameSubFeed.getFunctions(ScalaSubFeed.subFeedType)
  private implicit val subFeedCompanion: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(ScalaSubFeed.subFeedType)

  private def getPartitionValueFilter(pv: PartitionValues) = pv.getFilterExpr.asInstanceOf[ScalaAbstractColumn]

  override def factory: FromConfigFactory[DataObject] = MockSparkDataObject

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {}

}

object MockScalaDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MockSparkDataObject = {
    extract[MockSparkDataObject](config)
  }
}

