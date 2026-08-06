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
package org.apache.spark.sql.custom

import org.apache.spark.sql.Column
import org.apache.spark.sql.internal.{Alias, UnresolvedAttribute}

/**
 * Helper to access the package-private plan node of a Spark Connect [[Column]].
 * This needs to be located in a subpackage of org.apache.spark.sql.
 */
object ColumnUtil {

  /**
   * Extract the name of a column if it is a named expression (attribute or alias),
   * analogous to classic Sparks NamedExpression.name.
   */
  def getName(column: Column): Option[String] = column.node match {
    case alias: Alias => alias.name.lastOption
    case attribute: UnresolvedAttribute => attribute.nameParts.lastOption
    case _ => None
  }
}
