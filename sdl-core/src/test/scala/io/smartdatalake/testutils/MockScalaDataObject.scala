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
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.plainScala.{ScalaAbstractColumn, ScalaDataFrame, ScalaSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.Expectation
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
    with CanHandlePartitions with ExpectationValidation {
  assert(partitions.isEmpty || saveMode==SDLSaveMode.Overwrite, s"($id) Only saveMode=Overwrite implemented for partitioned MockDataObjects")
  assert(saveMode==SDLSaveMode.Overwrite || saveMode==SDLSaveMode.Append, s"($id) Only saveMode=Overwrite or saveMode=Append implemented for MockDataObjects")

  // variables to store mock values. They are filled using writeSparkDataFrame
  private var dataFrameMock: Option[ScalaDataFrame] = None
  private var partitionedDataFrameMock: Option[Map[PartitionValues, ScalaDataFrame]] = None
  private var partitionValuesMock: Set[PartitionValues] = Set()

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = partitionValuesMock.toSeq

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
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
        .orElse(schemaMin.map(subFeedCompanion.getEmptyDataFrame(_, id).asInstanceOf[ScalaDataFrame]))
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) partitionedDataFrameMock not initialized"))
    } else {
      dataFrameMock
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) dataFrameMock not initialized"))
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
    import functions._

    if (partitions.nonEmpty) {
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
    } else {
      saveMode match {
        case SDLSaveMode.Overwrite => dataFrameMock = Some(df)
        case SDLSaveMode.Append => dataFrameMock = Some(Seq(dataFrameMock, Some(df)).flatten.reduceLeft(_ unionByName _))
      }
      partitionValuesMock = Set()
      partitionedDataFrameMock = None
    }
    Map("records_written" -> df.collect.length) // enforce evaluate all columns by '.collect', so that constraints or RuntimeFailTransformer work as expected
  }

  def register(implicit instanceRegistry: InstanceRegistry): MockScalaDataObject = {
    instanceRegistry.register(this)
    this
  }

  override private[smartdatalake] def expectedPartitionsCondition: Option[String] = None
  override val metadata: Option[DataObjectMetadata] = None

  override var table: Table = Table(Some("mock"), tableName, primaryKey = primaryKey)

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = true

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = true

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

