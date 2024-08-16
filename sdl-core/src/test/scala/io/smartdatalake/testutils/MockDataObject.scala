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
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.jdk.CollectionConverters._

/**
 * Partitioned transactional mock data object.
 * Set dataFrame and partitionValues to be served by using writeSparkDataFrame.
 * PartitionValues are inferred if parameter of writeSparkDataFrame is empty.
 */
case class MockDataObject(override val id: DataObjectId, override val partitions: Seq[String] = Seq(),
                          override val schemaMin: Option[GenericSchema] = None, primaryKey: Option[Seq[String]] = None, tableName: String = "mock",
                          override val constraints: Seq[Constraint] = Seq(),
                          override val expectations: Seq[Expectation] = Seq(),
                          saveMode: SDLSaveMode = SDLSaveMode.Overwrite
                         ) extends DataObject with CanHandlePartitions with TransactionalTableDataObject with ExpectationValidation {
  assert(partitions.isEmpty || saveMode==SDLSaveMode.Overwrite, s"($id) Only saveMode=Overwrite implemented for partitioned MockDataObjects")
  assert(saveMode==SDLSaveMode.Overwrite || saveMode==SDLSaveMode.Append, s"($id) Only saveMode=Overwrite or saveMode=Append implemented for MockDataObjects")

  // variables to store mock values. They are filled using writeSparkDataFrame
  private var dataFrameMock: Option[DataFrame] = None
  private var partitionedDataFrameMock: Option[Map[PartitionValues,DataFrame]] = None
  private var partitionValuesMock: Set[PartitionValues] = Set()

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = partitionValuesMock.toSeq

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    if (partitions.nonEmpty) {
      partitionedDataFrameMock
        .map(_.filterKeys(pv => partitionValues.isEmpty || partitionValues.exists(pv.isIncludedIn)).values.reduce(_ unionAll _))
        .orElse(schemaMin.map(subFeedCompanion.getEmptyDataFrame(_, id).asInstanceOf[SparkDataFrame].inner))
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) partitionedDataFrameMock not initialized"))
    } else {
      dataFrameMock
        .getOrElse(throw NoDataToProcessWarning("mock", s"($id) dataFrameMock not initialized"))
    }
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitionValues.flatMap(_.keys).distinct.diff(partitions).isEmpty, s"($id) partitionValues keys dont match partition columns") // assert partition keys match
    assert(partitions.diff(df.columns).isEmpty, s"($id) partition columns are missing in DataFrame")

    // recreate DataFrame to truncate logical plan to avoid side-effects in tests
    val newDf = context.sparkSession.createDataFrame(df.collect.toSeq.asJava, df.schema)

    if (partitions.nonEmpty) {
      // mimick partition overwrite
      val inferredPartitionValues = if (partitionValues.isEmpty && partitions.nonEmpty) PartitionValues.fromDataFrame(SparkDataFrame(df.select(partitions.map(col):_*)))
      else partitionValues
      val newDataFrames = inferredPartitionValues.map(pv => (pv, newDf.where(getPartitionValueFilter(pv)))).toMap
      if (newDataFrames.nonEmpty) {
        partitionedDataFrameMock = Some(
          partitionedDataFrameMock.getOrElse(Map()) ++ newDataFrames
        )
        partitionValuesMock = partitionValuesMock ++ inferredPartitionValues
        dataFrameMock = None
      }
    } else {
      saveMode match {
        case SDLSaveMode.Overwrite => dataFrameMock = Some(newDf)
        case SDLSaveMode.Append => dataFrameMock = Some(Seq(dataFrameMock, Some(newDf)).flatten.reduceLeft(_.unionAll(_)))
      }
      partitionValuesMock = Set()
      partitionedDataFrameMock = None
    }
    Map("records_written" -> newDf.collect().length) // enforce evaluate all columns by '.collect', so that constraints or RuntimeFailTransformer work as expected
  }

  def register(implicit instanceRegistry: InstanceRegistry): MockDataObject = {
    instanceRegistry.register(this)
    this
  }

  override private[smartdatalake] def expectedPartitionsCondition: Option[String] = None
  override val metadata: Option[DataObjectMetadata] = None
  override val options: Map[String,String] = Map()

  override var table: Table = Table(Some("mock"), tableName, primaryKey = primaryKey)

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = true

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = true

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    partitionValuesMock = Set()
    dataFrameMock = None
    partitionedDataFrameMock = None
  }

  private implicit val subFeedCompanion: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(SparkSubFeed.subFeedType)
  private def getPartitionValueFilter(pv: PartitionValues) = pv.getFilterExpr.asInstanceOf[SparkColumn].inner

  override def factory: FromConfigFactory[DataObject] = MockDataObject

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {}


}

object MockDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MockDataObject = {
    extract[MockDataObject](config)
  }
}

