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
package io.smartdatalake.workflow.sparkconnect

import io.smartdatalake.config.ConfigToolbox
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.connection.SparkConnectConnection
import io.smartdatalake.workflow.dataframe.sparkconnect.SparkConnectSubFeed
import io.smartdatalake.workflow.dataobject.SparkConnectTableDataObject
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

class SparkConnectConfigParsingTest extends AnyFunSuite {

  test("parse SparkConnectConnection and SparkConnectTableDataObject from config") {
    val configPath = getClass.getResource("/config/sparkconnect.conf").getPath
    val (registry, _) = ConfigToolbox.loadAndParseConfig(Seq(configPath))
    val connection = registry.get[SparkConnectConnection](ConnectionId("sparkConnectCon"))
    assert(connection.url == "sc://localhost:15002")
    val dataObject = registry.get[SparkConnectTableDataObject](DataObjectId("testTable"))
    assert(dataObject.table.name == "sdlb_test_table")
    assert(dataObject.connection.id.id == "sparkConnectCon")
  }

  test("SparkConnectSubFeed is discovered by reflection") {
    assert(DataFrameSubFeed.getKnownSubFeedTypes.exists(_ =:= typeOf[SparkConnectSubFeed]))
  }

  test("SparkConnectSubFeed companion is resolvable") {
    val companion = DataFrameSubFeed.getCompanion(typeOf[SparkConnectSubFeed])
    assert(companion == SparkConnectSubFeed)
  }
}
