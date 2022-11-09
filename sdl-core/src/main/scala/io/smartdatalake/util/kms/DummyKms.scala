/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.kms

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.parquet.crypto.keytools.KmsClient;

// ATTENTION, this is only a dummy and does no proper key handling
//TODO still need proper implementation of a KMS
class DummyKms extends KmsClient {
  override def initialize(configuration: Configuration, s: String, s1: String, s2: String): Unit = {}

  override def wrapKey(key: Array[Byte], masterKeyIdentifier: String): String = {
    "Idonttellyouanykey"
  }

  override def unwrapKey(wrappedKey: String, masterKeyIdentifier: String): Array[Byte] = {
    masterKeyIdentifier.getBytes(StandardCharset.UTF_8)
  }
}