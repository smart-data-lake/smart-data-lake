/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.dataobject.spark

import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe.typeOf

/**
 * Extension methods to read/write classic Spark DataFrames on DataObjects implementing the generic
 * CanCreateDataFrame/CanWriteDataFrame interface, e.g. engine-agnostic DataObjects like DeltaLakeTableDataObject.
 *
 * Note that for DataObjects implementing [[CanCreateSparkDataFrame]]/[[CanWriteSparkDataFrame]] the
 * methods defined by those traits take precedence over these extension methods.
 */
object SparkDataObjectOps {

  implicit class SparkCanCreateDataFrameOps(dataObject: CanCreateDataFrame) {
    def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
      dataObject.getDataFrame(partitionValues, typeOf[SparkSubFeed]) match {
        case df: SparkDataFrame => df.inner
        case df => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
      }
    }
  }

  implicit class SparkCanWriteDataFrameOps(dataObject: CanWriteDataFrame) {
    def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                           (implicit context: ActionPipelineContext): MetricsMap = {
      dataObject.writeDataFrame(SparkDataFrame(df), partitionValues, isRecursiveInput, saveModeOptions)
    }
    def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)
                          (implicit context: ActionPipelineContext): Unit = {
      dataObject.init(SparkDataFrame(df), partitionValues, saveModeOptions)
    }
  }
}
