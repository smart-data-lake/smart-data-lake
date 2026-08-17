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
package io.smartdatalake.util.misc

/**
 * Helper functions to match names of DataObjects, DataFrames and transform method parameters.
 */
object NameUtil {

  /**
   * Tolerant lookup of entry in map.
   * Comparison is made case-insensitive and without underscore and hyphen.
   */
  private[smartdatalake] def tolerantGet[T](map: Map[String, T], key: String): Option[T] = {
    val tolerantMap = map.map { case (k, v) => (prepareTolerantKey(k), v) }
    tolerantMap.get(prepareTolerantKey(key))
  }

  /**
   * Normalize a key for tolerant comparison, e.g. lowercase and without underscore and hyphen.
   */
  private[smartdatalake] def prepareTolerantKey(key: String): String = {
    key.toLowerCase.replace("-", "").replace("_", "")
  }
}
