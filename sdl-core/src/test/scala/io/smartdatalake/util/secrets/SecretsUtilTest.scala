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

import io.smartdatalake.config.ConfigurationException
import org.scalatest.FunSuite

class SecretsUtilTest extends FunSuite {

  test("register custom config provider and get secret") {
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map("option1" -> "1")))
    SecretsUtil.registerProvider("TEST", providerConfig.provider)
    assert(SecretsUtil.resolveSecret("###TEST#test###") == "test1")
  }

  test("custom config provider and get secret with hashtag") {
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map("option1" -> "1")))
    SecretsUtil.registerProvider("TEST", providerConfig.provider)
    assert(SecretsUtil.resolveSecret("###TEST#te#st###") == "te#st1")
  }

  test("resolve plaintext secret") {
    assert(SecretsUtil.resolveSecret("somesecret") == "somesecret")
  }

  test("resolve plaintext secret with hashtag") {
    assert(SecretsUtil.resolveSecret("some#secret") == "some#secret")
  }

  test("resolve legacy secret") {
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map("option1" -> "1")))
    SecretsUtil.registerProvider("TEST", providerConfig.provider)
    assert(SecretsUtil.convertSecretVariableToStringOrSecret("TEST#test").resolve() == "test1")
  }

}

class TestSecretProvider(option1: String) extends SecretProvider {
  def this(options: Map[String, String]) {
    this(
      options.getOrElse("option1", throw new ConfigurationException(s"Cannot create TestSecretProvider, option 'option1' missing."))
    )
  }
  override def getSecret(name: String): String = name + option1
}