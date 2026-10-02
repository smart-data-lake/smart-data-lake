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
package io.smartdatalake.testutils.sql

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.JdbcTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table

/**
 * Source DataObject for tests of the SQL engine with the behaviour traits, the counterpart of MockScalaDataObject and
 * MockSparkDataObject. All inputs of an Action using the SQL engine must be tables on its engine connection, so it
 * is a JdbcTableDataObject, but writing replaces the table. Like that, the schema of the source data can change
 * between writes, and removed columns disappear.
 */
class MockSQLTableDataObject(id: DataObjectId, connectionId: ConnectionId, primaryKey: Option[Seq[String]] = None)(implicit instanceRegistry: InstanceRegistry)
  extends JdbcTableDataObject(id, table = Table(db = None, name = id.id.replace("-", "_"), primaryKey = primaryKey), connectionId = connectionId)(instanceRegistry) {

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    dropTable
    resetCachedIsTableExisting()
    resetCachedSchema()
    init(df, partitionValues, saveModeOptions)
    super.writeDataFrame(df, partitionValues, isRecursiveInput, saveModeOptions)
  }
}
