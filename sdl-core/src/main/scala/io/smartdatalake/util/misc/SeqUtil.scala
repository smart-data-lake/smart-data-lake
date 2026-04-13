/*
 * sdl-core - Build your data lake the smart way.
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

import io.smartdatalake.definitions.Environment

object SeqUtil {
  implicit class SeqStringExtension(seq: Seq[String]) {
    def caseSensitiveDiff(in: Seq[String]): Seq[String] = {
      if (Environment.caseSensitive) seq.diff(in)
      else seq.map(_.toLowerCase).diff(in.map(_.toLowerCase))
    }
  }
  //TODO: can be removed when we switch to Scala 2.13, because it has maxOption and minOption built in
  implicit class SeqWithOrderingExtension[T: Ordering](seq: Seq[T]) {
    def maxOption(): Option[T] = {
      if (seq.isEmpty) None
      else Some(seq.max)
    }
    def minOption(): Option[T] = {
      if (seq.isEmpty) None
      else Some(seq.min)
    }
  }
  //TODO: can be removed when we switch to Scala 2.13, because it has maxOption and minOption built in
  implicit class SeqExtension[T](seq: Seq[T]) {
    def maxOption(ordering: Ordering[T]): Option[T] = {
      if (seq.isEmpty) None
      else Some(seq.max(ordering))
    }
    def minOption(ordering: Ordering[T]): Option[T] = {
      if (seq.isEmpty) None
      else Some(seq.min(ordering))
    }
  }
}