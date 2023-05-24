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

import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.Type

trait TableDataObject extends DataObject with CanCreateDataFrame with SchemaValidation {

  var table: Table

  def isDbExisting(implicit context: ActionPipelineContext): Boolean

  def isTableExisting(implicit context: ActionPipelineContext): Boolean

  def dropTable(implicit context: ActionPipelineContext): Unit

  def getPKduplicates(subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._
    if (table.primaryKey.isEmpty) {
      getDataFrame(Seq(), subFeedType).filter(lit(false)) // get empty dataframe
    } else {
      getDataFrame(Seq(), subFeedType).getNonuniqueRows(table.primaryKey.get)
    }
  }

  def getPKnulls(subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._
    if (table.primaryKey.isEmpty) {
      getDataFrame(Seq(), subFeedType).filter(lit(false)) // get empty dataframe
    } else {
      getDataFrame(Seq(), subFeedType).getNulls(table.primaryKey.get)
    }
  }

  def getPKviolators(subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    getPKduplicates(subFeedType).unionByName(getPKnulls(subFeedType))
  }

  override def atlasQualifiedName(prefix: String): String = s"${table.db.getOrElse("default")}.${table.name}"

  override def atlasName: String = id.id
}
