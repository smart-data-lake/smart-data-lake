/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.BigQueryTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import org.apache.spark.sql.DataFrame

case class BigQueryTableDataObject(override val id: DataObjectId,
                                   override var table: Table,
                                   override val schemaMin: Option[GenericSchema] = None,
                                   override val constraints: Seq[Constraint] = Seq(),
                                   override val expectations: Seq[Expectation] = Seq(),
                                   saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                   connectionId: ConnectionId,

                                   override val metadata: Option[DataObjectMetadata] = None)
                                  (@transient implicit val instanceRegistry: InstanceRegistry) extends TransactionalTableDataObject with ExpectationValidation {

  private val connection = getConnection[BigQueryTableConnection](connectionId)

  private val hasQuery: Boolean = table.query.isDefined
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = ???

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = ???

  override def dropTable(implicit context: ActionPipelineContext): Unit = ???

  /**
   * Returns the factory that can parse this type (that is, type `CO`).
   *
   * Typically, implementations of this method should return the companion object of the implementing class.
   * The companion object in turn should implement [[FromConfigFactory]].
   *
   * @return the factory (object) for this class.
   */
  override def factory: FromConfigFactory[DataObject] = ???

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = ???

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = ???
}
