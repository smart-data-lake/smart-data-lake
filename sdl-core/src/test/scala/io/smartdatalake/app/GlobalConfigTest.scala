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

import io.smartdatalake.util.secrets.{SecretProvider, SecretProviderConfig, StringOrSecret}
import org.scalatest.FunSuite

class GlobalConfigTest extends FunSuite {
  test("sparkOptions secrets are resolved in Hadoop config") {
    // prepare
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map()))

    // execute
    val globalConfig = GlobalConfig(sparkOptions = Some(Map("spark.hadoop.hadoop.security.authentication" -> StringOrSecret("###TESTPROVIDER#secret###"))),
      secretProviders = Some(Map("TESTPROVIDER" -> providerConfig)))
    val hadoopConfig = globalConfig.getHadoopConfiguration

    // check
    assert(hadoopConfig.get("hadoop.security.authentication") == "resolvedSecret")
  }

  test("sparkOptions secrets are resolved in Spark session configuration") {
    // prepare
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map()))

    // execute
    val globalConfig = GlobalConfig(sparkOptions = Some(Map("spark.authenticate.secret" -> StringOrSecret("###TESTPROVIDER#secret###"))),
      secretProviders = Some(Map("TESTPROVIDER" -> providerConfig)))
    val sparkSession = globalConfig.sparkSession("test", Some("local"))

    // check
    assert(sparkSession.conf.get("spark.authenticate.secret") == "resolvedSecret")
  }
}

class TestSecretProvider(options: Map[String, String]) extends SecretProvider {
  override def getSecret(name: String): String = {
    name match {
      case "secret" => "resolvedSecret"
      case _ => throw new IllegalArgumentException("Secret cannot be resolved")
    }
  }
}
