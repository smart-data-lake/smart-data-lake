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
import io.smartdatalake.smartdatalake.{SnowparkDataFrame, SnowparkStructType}

private[smartdatalake] trait SnowparkTableDataObject extends DataObject with CanCreateSnowparkDataFrame with SchemaValidation {

  var table: Table

  var tableSchema: SnowparkStructType = null

  def isDbExisting(implicit context: ActionPipelineContext): Boolean

  def isTableExisting(implicit context: ActionPipelineContext): Boolean

  def dropTable(implicit context: ActionPipelineContext): Unit

//  def getPKduplicates(implicit context: ActionPipelineContext): SnowparkDataFrame = if (table.primaryKey.isEmpty) {
//    getSnowparkDataFrame().where(lit(false))
//  } else {
//    getSnowparkDataFrame().getNonuniqueRows(table.primaryKey.get.toArray)
//  }
//
//  def getPKnulls(implicit context: ActionPipelineContext): SnowparkDataFrame = {
//    getSnowparkDataFrame().getNulls(table.primaryKey.get.toArray)
//  }
//
//  def getPKviolators(implicit context: ActionPipelineContext): SnowparkDataFrame = {
//    getPKduplicates.union(getPKnulls)
//  }
//
//  def isPKcandidateKey(implicit context: ActionPipelineContext): Boolean =  {
//    table.primaryKey.isEmpty || getSnowparkDataFrame().isCandidateKey(table.primaryKey.get.toArray)
//  }

  override def atlasQualifiedName(prefix: String): String = s"${table.db.getOrElse("default")}.${table.name}"

  override def atlasName: String = id.id
}
