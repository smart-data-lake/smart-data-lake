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

package io.smartdatalake.workflow.dataframe
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkSchema

import scala.reflect.runtime.universe

/**
 * Lazy Generic Schema parses the schema config string only when it is first used.
 * Like that files referenced are not required to exist until they are really used.
 * @param schemaConfig configuration string for defining schema
 */
case class LazyGenericSchema(schemaConfig: String) extends GenericSchema {

  private lazy val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig, lazyFileReading = false)

  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = schema.diffSchema(schema)
  override def columns: Seq[String] = schema.columns
  override def fields: Seq[GenericField] = schema.fields
  override def sql: String = schema.sql
  override def add(colName: String, dataType: GenericDataType): GenericSchema = schema.add(colName, dataType)
  override def add(field: GenericField): GenericSchema = schema.add(field)
  override def remove(colName: String): GenericSchema = schema.remove(colName)
  override def filter(func: GenericField => Boolean): GenericSchema = schema.filter(func)
  override def getEmptyDataFrame(dataObjectId: SdlConfigObject.DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = schema.getEmptyDataFrame(dataObjectId)
  override def getDataType(colName: String): GenericDataType = schema.getDataType(colName)
  override def makeNullable: GenericSchema = schema.makeNullable
  override def toLowerCase: GenericSchema = schema.toLowerCase
  override def removeMetadata: GenericSchema = schema.removeMetadata
  override def subFeedType: universe.Type = schema.subFeedType
}
