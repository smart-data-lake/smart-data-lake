/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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
 * Workaround to use scala.collection.parallel.CollectionConverters in Scala 2.12 and Scala 2.13,
 * see also https://github.com/scala/scala-parallel-collections/issues/22
 */
object CompatParColls {
  val Converters = {
    import Compat._
    {
      import scala.collection.parallel._
      CollectionConverters
    }
  }
  object Compat {
    object CollectionConverters
  }
}