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

import io.smartdatalake.util.Constants.epsilonDouble

/**
 * Case class to configure a comparison
 * (mainly used to compare columns)
 *
 * @param precision    defines preciseness of non-exact numeric comparison for all numeric columns
 * @param strict       configures whether comparison is strict (<) or not (≤)
 * @param relThreshold defines threshold to switch between relative and absolute comparison
 *                     relative comparison if absolute value of both column values are larger than relThreshold,
 *                     absolut comparison otherwise
 *                     if relThreshold is None then absolute comparison for all values
 */
case class PrecisionDef(precision: Double = epsilonDouble,
                        strict: Boolean = true,
                        relThreshold: Option[Double] = Some(epsilonDouble)
                       )
