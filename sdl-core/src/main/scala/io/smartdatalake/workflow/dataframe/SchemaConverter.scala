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
package io.smartdatalake.workflow.dataframe

import scala.reflect.runtime.universe.Type

/**
 * Converts schemas and data types from one SubFeedType to another, e.g. to pass a schema on to an Action with another
 * engine, or to validate a DataFrame against a schema parsed by another engine.
 *
 * Schemas are converted through their engine-neutral Json representation, see [[GenericSchema.toJson]] and
 * [[GenericSchema.fromJson]]. It uses the Spark type names for simple data types, so every engine must write these
 * names in `toJson` and parse them in `DataFrameSubFeedCompanion.createSimpleDataType`.
 */
private[smartdatalake] object SchemaConverter {

  private def conversionException(fromSubFeedType: Type, toSubFeedType: Type, e: Exception) =
    new IllegalStateException(s"Can not convert schema from ${fromSubFeedType.typeSymbol.name} to ${toSubFeedType.typeSymbol.name}: ${e.getMessage}", e)

  /**
   * Convert a given schema with SubFeedType A to SubFeedType B.
   */
  def convert(schema: GenericSchema, toSubFeedType: Type): GenericSchema = {
    // convert if needed
    if (schema.subFeedType != toSubFeedType) {
      try GenericSchema.fromJson(schema.toJson, toSubFeedType)
      catch { case e: Exception => throw conversionException(schema.subFeedType, toSubFeedType, e) }
    } else schema match {
      // resolve lazy schema
      case x:LazyGenericSchema => x.get
      // otherwise return as is
      case x => x
    }
  }

  /**
   * Convert a given data type with SubFeedType A to SubFeedType B.
   */
  def convertDatatype(dataType: GenericDataType, toSubFeedType: Type): GenericDataType = {
    if (dataType.subFeedType != toSubFeedType) {
      try GenericSchema.dataTypeFromJson(dataType.toJson, toSubFeedType)
      catch { case e: Exception => throw conversionException(dataType.subFeedType, toSubFeedType, e) }
    } else dataType
  }
}
