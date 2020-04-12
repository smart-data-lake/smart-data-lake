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
package io.smartdatalake.config

import com.typesafe.config.ConfigException
import org.scalatest.{FlatSpec, Matchers}


class ConfigLoaderTest extends FlatSpec with Matchers {

  "ConfigLoader" must "parse a single configuration file" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config/config.conf").toString))
    config.getString("config") shouldBe "config"
  }

  it must "parse all configuration files in a directory and its sub-directories" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config").toString))
    config.getString("config") shouldBe "config"
    config.getString("config2") shouldBe "config2"
    config.getString("foo") shouldBe "foo"
  }

  it must "overwrite values in configurations in BFS order and by extension precedence" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config").toString))
    config.getString("overwrite") shouldBe "overwritten by config2"
    config.getString("donotoverwrite") shouldBe "original"
    config.getString("overwrite2") shouldBe "overwritten by bar"
  }

  it must "fail parsing a non-existing location" in {
    an [IllegalArgumentException] should be thrownBy ConfigLoader.loadConfigFromFilesystem(Seq("foo/bar"))
  }

  it must "not parse a file that is not a config file" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config/subdirectory/file.txt").toString))
    an [ConfigException] should be thrownBy config.getString("noconfig")
  }

  it must "only parse config files in directories" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config/subdirectory").toString))
    config.getString("foo") shouldBe "foo"
    config.getString("config2") shouldBe "config2"
    an [ConfigException] should be thrownBy config.getString("noconfig")
  }
}
