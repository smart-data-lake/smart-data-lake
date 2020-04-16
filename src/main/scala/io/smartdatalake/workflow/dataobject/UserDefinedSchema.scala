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

import org.apache.spark.sql.types.StructType

/**
 * A [[DataObject]] that allows optional user-defined schema definition.
 */
private[smartdatalake] trait UserDefinedSchema {

  /**
   * An optional [[DataObject]] user-defined schema definition.
   *
   * Some [[DataObject]]s support optional schema inference.
   * Specifying this attribute disables automatic schema inference. When the wrapped data source contains a source
   * schema, this `schema` attribute is ignored.
   *
   * Note: This is only used by the functionality defined in [[CanCreateDataFrame]], that is,
   * when reading Spark data frames from the underlying data container.
   * [[io.smartdatalake.workflow.action.Action]]s that bypass Spark data frames ignore the `schema` attribute
   * if it is defined.
   */
  def schema: Option[StructType]
}
