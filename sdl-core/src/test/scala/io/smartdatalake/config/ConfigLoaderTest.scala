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
import org.apache.hadoop.conf.Configuration
import org.scalatest.{FlatSpec, Matchers}


class ConfigLoaderTest extends FlatSpec with Matchers {

  val defaultHadoopConf: Configuration = new Configuration()

  "ConfigLoader" must "parse a single configuration file" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config/config.conf").toString), defaultHadoopConf)
    config.getString("config") shouldBe "config"
  }

  it must "parse all configuration files in a directory and its sub-directories without trailing slash" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config").toString), defaultHadoopConf)
    config.getString("config") shouldBe "config"
    config.getString("config2") shouldBe "config2"
    config.getString("foo") shouldBe "foo"
  }

  it must "parse all configuration files in a directory and its sub-directories with trailing slash" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config").toString), defaultHadoopConf)
    config.getString("config") shouldBe "config"
    config.getString("config2") shouldBe "config2"
    config.getString("foo") shouldBe "foo"
  }

  it must "overwrite values in configurations in BFS order and by extension precedence" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config").toString), defaultHadoopConf)
    config.getString("overwrite") shouldBe "overwritten by config2"
    config.getString("donotoverwrite") shouldBe "original"
    config.getString("overwrite2") shouldBe "overwritten by bar"
  }

  it must "parse a special classpath entry" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq("cp:/config/config.conf"), defaultHadoopConf)
    config.getString("config") shouldBe "config"
  }

  it must "fail parsing a non-existing location" in {
    an [ConfigurationException] should be thrownBy ConfigLoader.loadConfigFromFilesystem(Seq("foo/bar"), defaultHadoopConf)
  }

  it must "fail parsing a non-existing classpath entry" in {
    a [ConfigurationException] should be thrownBy ConfigLoader.loadConfigFromFilesystem(Seq("cp:/foo/bar.conf"), defaultHadoopConf)
  }

  it must "fail parsing a classpath entry with wrong extension" in {
    a [ConfigurationException] should be thrownBy ConfigLoader.loadConfigFromFilesystem(Seq("cp:/foo/bar"), defaultHadoopConf)
  }

  it must "not parse a file that is not a config file" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config/subdirectory").toString), defaultHadoopConf)
    a [ConfigException] should be thrownBy config.getString("noconfig")
  }

  it must "ignore hidden files" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config").toString), defaultHadoopConf)
    config.hasPath("hidden") shouldBe false
  }

  it must "only parse config files in directories" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/config/subdirectory").toString), defaultHadoopConf)
    config.getString("foo") shouldBe "foo"
    config.getString("config2") shouldBe "config2"
    a [ConfigException] should be thrownBy config.getString("noconfig")
  }

  it should "fail on duplicate configuration object IDs" in {
    a [ConfigurationException] should be thrownBy ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/configWithDuplicates").toString), defaultHadoopConf)
  }

  it must "should load configurations with templates" in {
    val config = ConfigLoader.loadConfigFromFilesystem(Seq(getClass.getResource("/configWithTemplates").toString), defaultHadoopConf)
    config.getString("dataObjects.testDataObjectFromConfig.type") shouldBe "io.smartdatalake.config.TestDataObject"
  }
}
