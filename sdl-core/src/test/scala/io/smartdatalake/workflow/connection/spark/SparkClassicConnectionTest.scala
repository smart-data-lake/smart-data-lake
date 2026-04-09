/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.workflow.connection.spark

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.secrets.{SecretProvider, SecretProviderConfig, StringOrSecret}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SparkClassicConnection
import org.scalatest.funsuite.AnyFunSuite

class SparkClassicConnectionTest extends AnyFunSuite {

  test("sparkOptions secrets are resolved in Spark session configuration") {
    // prepare
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map()))

    // execute
    val globalConfig = GlobalConfig(
      secretProviders = Some(Map("TESTPROVIDER" -> providerConfig))
    )
    val sparkClassicConnection = SparkClassicConnection(
      id = "testConnection",
      master = Some("local"),
      sparkOptions = Map("spark.authenticate.secret" -> StringOrSecret("###TESTPROVIDER#secret###")),
    )
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
    implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(globalConfig = globalConfig)
    val sparkSession = sparkClassicConnection.sparkSession

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
