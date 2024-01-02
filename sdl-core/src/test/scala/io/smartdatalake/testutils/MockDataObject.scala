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
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Partitioned transactional mock data object.
 * Set dataFrame and partitionValues to be served by using writeSparkDataFrame.
 * PartitionValues are inferred if parameter of writeSparkDataFrame is empty.
 * writeSparkDataFrame mimicks overwrite mode, also for partitions.
 */
case class MockDataObject(override val id: DataObjectId, override val partitions: Seq[String] = Seq(), override val schemaMin: Option[GenericSchema] = None, primaryKey: Option[Seq[String]] = None) extends DataObject with CanHandlePartitions with TransactionalTableDataObject {

  // variables to store mock values. They are filled using writeSparkDataFrame
  private var dataFrameMock: Option[DataFrame] = None
  private var partitionValuesMock: Seq[PartitionValues] = Seq()

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = partitionValuesMock

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    dataFrameMock.getOrElse(throw NoDataToProcessWarning("mock", s"($id) dataFrameMock not initialized"))
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitionValues.flatMap(_.keys).distinct.diff(partitions).isEmpty, s"($id) partitionValues keys dont match partition columns") // assert partition keys match
    assert(partitions.diff(df.columns).isEmpty, s"($id) partition columns are missing in DataFrame")
    val inferredPartitionValues = if (partitionValues.isEmpty && partitions.nonEmpty) {
      // infer partition values
      df.select(partitions.map(col):_*).collect().map(row => PartitionValues(partitions.map(p => (p,row.getAs[Any](p))).toMap)).toSeq
    } else partitionValues

    if (partitions.nonEmpty && dataFrameMock.isDefined) {
      // mimick partition overwrite
      dataFrameMock = Some(
        dataFrameMock.get
        .where(subFeedCompanion.not(PartitionValues.createFilterExpr(partitionValues)).asInstanceOf[SparkColumn].inner)
        .unionAll(df)
      )
      partitionValuesMock = partitionValuesMock ++ inferredPartitionValues
    } else {
      // overwrite all
      dataFrameMock = Some(df)
      partitionValuesMock = inferredPartitionValues
    }
    Map("records_written" -> df.count())
  }

  def register(implicit instanceRegistry: InstanceRegistry): MockDataObject = {
    instanceRegistry.register(this)
    this
  }

  override private[smartdatalake] def expectedPartitionsCondition: Option[String] = None
  override val metadata: Option[DataObjectMetadata] = None
  override val options: Map[String,String] = Map()

  override var table: Table = Table(Some("mock"), id.id, primaryKey = primaryKey)

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = true

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = true

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    partitionValuesMock = Seq()
    dataFrameMock = None
  }

  private implicit val subFeedCompanion: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(SparkSubFeed.subFeedType)

  override def factory: FromConfigFactory[DataObject] = MockDataObject

}

object MockDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MockDataObject = {
    extract[MockDataObject](config)
  }
}

