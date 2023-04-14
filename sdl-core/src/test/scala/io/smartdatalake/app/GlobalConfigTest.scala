/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.definitions.Environment
import org.scalatest.FunSuite

class GlobalConfigTest extends FunSuite {
  test("Spark session is case sensitive if caseSensitive is true in environment") {
    // prepare
    val globalConfig = GlobalConfig(environment = Map("caseSensitive" -> "true"))
    Environment._globalConfig = globalConfig

    // execute
    val sparkSession = globalConfig.sparkSession("test", Some("local"))

    // check
    assert(sparkSession.conf.get("spark.sql.caseSensitive") == "true")

    // cleanup
    Environment._globalConfig = null
    Environment._caseSensitive = None
  }

  test("Spark session is case insensitive if caseSensitive is not set in environment") {
    // prepare
    val globalConfig = GlobalConfig()
    Environment._globalConfig = globalConfig

    // execute
    val sparkSession = globalConfig.sparkSession("test", Some("local"))

    // check
    assert(sparkSession.conf.get("spark.sql.caseSensitive") == "false")

    // cleanup
    Environment._globalConfig = null
    Environment._caseSensitive = None
  }
}
