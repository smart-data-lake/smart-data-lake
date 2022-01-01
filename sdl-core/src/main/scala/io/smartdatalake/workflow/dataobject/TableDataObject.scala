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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

private[smartdatalake] trait TableDataObject extends DataObject with CanCreateDataFrame with SchemaValidation {

  var table: Table

  var tableSchema: StructType = null

  def isDbExisting(implicit context: ActionPipelineContext): Boolean

  def isTableExisting(implicit context: ActionPipelineContext): Boolean

  def dropTable(implicit context: ActionPipelineContext): Unit

  def getPKduplicates(implicit context: ActionPipelineContext): DataFrame = if (table.primaryKey.isEmpty) {
    getDataFrame().where(lit(false))
  } else {
    getDataFrame().getNonuniqueRows(table.primaryKey.get.toArray)
  }

  def getPKnulls(implicit context: ActionPipelineContext): DataFrame = {
    getDataFrame().getNulls(table.primaryKey.get.toArray)
  }

  def getPKviolators(implicit context: ActionPipelineContext): DataFrame = {
    getPKduplicates.union(getPKnulls)
  }

  def isPKcandidateKey(implicit context: ActionPipelineContext): Boolean =  {
    table.primaryKey.isEmpty || getDataFrame().isCandidateKey(table.primaryKey.get.toArray)
  }

  override def atlasQualifiedName(prefix: String): String = s"${table.db.getOrElse("default")}.${table.name}"

  override def atlasName: String = id.id
}
