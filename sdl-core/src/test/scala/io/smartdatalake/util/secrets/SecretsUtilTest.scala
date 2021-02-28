/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.secrets

import org.scalatest.FunSuite

class SecretsUtilTest extends FunSuite {

  test("register custom config provider and get secret") {
    val providerConfig = new SecretProviderConfig(classOf[TestSecretProvider].getName, Map("option1" -> "1"))
    SecretsUtil.registerProvider("TEST", providerConfig.provider)
    assert(SecretsUtil.getSecret("TEST#test") == "test1")
  }

}

class TestSecretProvider(options: Map[String,String]) extends SecretProvider {
  override def getSecret(name: String): String = name + options("option1")
}