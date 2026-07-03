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
package io.smartdatalake.util

import java.lang.Math.scalb

object Constants {
  lazy val epsilonDouble: Double = Math.scalb(1d, -16) // 2^(-16)
  lazy val epsilonFloat: Float = Math.scalb(1f, -16) // 2^(-16)
  lazy val halfDouble: Double = scalb(1d, -1)
  lazy val halfFloat: Float = scalb(1f, -1)
  lazy val quarterDouble: Double = scalb(1d, -2)
  lazy val quarterFloat: Float = scalb(1f, -2)

  lazy val halfPi: Double = Math.scalb(1d, -1) * scala.math.Pi
  lazy val quarterPi: Double = Math.scalb(1d, -2) * scala.math.Pi
  lazy val twoPi: Double = 2d * scala.math.Pi

}
