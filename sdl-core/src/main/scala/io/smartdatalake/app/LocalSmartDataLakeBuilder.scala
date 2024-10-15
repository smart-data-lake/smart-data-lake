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
package io.smartdatalake.app

import io.smartdatalake.util.misc.SmartDataLakeLogger

/**
 * Alternative Name for SparkSmartDataLakeBuilder. Used for historical reasons.
 */
object LocalSmartDataLakeBuilder extends SmartDataLakeLogger {

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting LocalSmartDataLakeBuilder. This is an Alias for SparkSmartDataLakeBuilder.")
    SparkSmartDataLakeBuilder.main(args)
  }

}