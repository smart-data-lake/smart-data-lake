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

import io.smartdatalake.util.misc.{ReflectionUtil, ScalaUtil}
import io.smartdatalake.workflow.DataFrameSubFeed
import org.reflections.Reflections

import scala.reflect.runtime.universe.Type

/**
 * An interface for to be implementated by schema converters.
 * A schema converter can convert schemas from a specific SubFeedType A to a specific SubFeedType B.
 * As SDLB doesnt define an own intermediary schema langauge, a schema conversion must be implemented for every pair of SubFeedType SDLB should be able to convert.
 */
private[smartdatalake] trait SchemaConverter {
  def fromSubFeedType: Type
  def toSubFeedType: Type
  final def convert(schema: GenericSchema): GenericSchema = {
    val helper = DataFrameSubFeed.getCompanion(toSubFeedType)
    helper.createSchema(schema.fields.map(convertField))
  }
  def convertField(field: GenericField): GenericField
  def convertDataType(dataType: GenericDataType): GenericDataType
}

private[smartdatalake] object SchemaConverter {

  // search for converters by using reflection
  implicit private lazy val workflowReflections: Reflections = ReflectionUtil.getReflections("io.smartdatalake.workflow")
  private lazy val converters = ReflectionUtil.getTraitImplClasses[SchemaConverter]
    .map(c => ScalaUtil.companionOf[SchemaConverter](c.getName))
    .map(o => ((o.fromSubFeedType,o.toSubFeedType), o)).toMap

  /**
   * Get the converter to convert schema from SubFeedType A to SubFeedType B.
   * If there is no SchemaConverter implementation in the classpath for this combination, an exception is thrown.
   */
  private def getConverter(fromSubFeedType: Type, toSubFeedType:Type): SchemaConverter = {
    converters.get(fromSubFeedType, toSubFeedType)
      .getOrElse(throw new IllegalStateException(s"No schema converter found from ${fromSubFeedType.typeSymbol.name} to ${toSubFeedType.typeSymbol.name}"))
  }

  /**
   * Convert a given schema with SubFeedType A to SubFeedType B.
   */
  def convert(schema: GenericSchema, toSubFeedType: Type): GenericSchema = {
    // convert if needed
    if (schema.subFeedType != toSubFeedType) getConverter(schema.subFeedType, toSubFeedType).convert(schema)
    else schema match {
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
    if (dataType.subFeedType != toSubFeedType) getConverter(dataType.subFeedType, toSubFeedType).convertDataType(dataType)
    else dataType
  }
}