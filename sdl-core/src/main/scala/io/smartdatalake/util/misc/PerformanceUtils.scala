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
package io.smartdatalake.util.misc

import java.time.Duration

private[smartdatalake] object PerformanceUtils {
  /**
   * Measures time for some code block in seconds (float)
   * @param code2exec code block to be executed
   * @tparam T: return type of code block
   * @return tuple of code block return value and time in seconds (float)
   */
  def measureTime[T](code2exec: => T): (T,Float) = {
    val t0 = System.currentTimeMillis()
    val result = code2exec
    val t = (System.currentTimeMillis()-t0).toFloat / 1000
    (result, t)
  }

  /**
   * Measures duration for some code block
   * @param code2exec code block to be executed
   * @tparam T: return type of code block
   * @return tuple of code block return value and duration
   */
  def measureDuration[T](code2exec: => T): (T,Duration) = {
    val t0 = System.currentTimeMillis()
    val result = code2exec
    val d = Duration.ofMillis(System.currentTimeMillis()-t0)
    (result, d)
  }

}
