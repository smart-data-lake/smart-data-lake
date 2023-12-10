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

package io.smartdatalake.util.misc

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.FunSuite

class HoconUtilTest extends FunSuite {

  val config = ConfigFactory.parseString(
    """
      |actions = {
      | a1 = {
      |   type = FileTransferAction
      |   inputId = do3
      |   outputId = do1
      |   transformers = [{
      |      type = ScalaClassSparkDfsTransformer
      |      className = io.smartdatalake.DummyTransformer
      |   }]
      | }
      |}
      |""".stripMargin).resolve

  test("reading nested configuration value inside list") {
    assert(HoconUtil.getConfigValue(config.root, Seq("actions","a1","transformers","[0]","className")).unwrapped == "io.smartdatalake.DummyTransformer")
  }

  test("set nested configuration value inside list") {
    val newConfig = HoconUtil.updateConfigValue(config.root, Seq("actions", "a1", "transformers", "[0]", "_sourceDoc"), ConfigValueFactory.fromAnyRef("abc"))
    assert(HoconUtil.getConfigValue(newConfig, Seq("actions","a1","transformers","[0]","_sourceDoc")).unwrapped == "abc")
  }

  test("update nested configuration value inside list") {
    val newConfig = HoconUtil.updateConfigValue(config.root, Seq("actions", "a1", "transformers", "[0]", "className"), ConfigValueFactory.fromAnyRef("abc"))
    assert(HoconUtil.getConfigValue(newConfig, Seq("actions", "a1", "transformers", "[0]", "className")).unwrapped == "abc")
  }
}
