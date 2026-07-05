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
package io.smartdatalake.util.crypt

import org.apache.hadoop.hive.ql.exec.UDF

class EncryptColumn extends UDF with EncryptDecryptSupport {
  def evaluate(message: String, key: String, algorithm: String): String = {
    val keyBytes: Array[Byte] = key.getBytes
    val crypt: EncryptDecrypt = algorithm.toUpperCase() match {
      case "GCM"                                => new EncryptDecryptGCM(keyBytes)
      case "ECB"                                => new EncryptDecryptECB(keyBytes)
      case classname if classname.contains(".") => loadEncryptDecryptClass(classname, keyBytes)
      case _                                    => throw new UnsupportedOperationException(s"unsupported en/decryption algorithm ${algorithm}")
    }
    crypt.encrypt(message)
  }

}

class DecryptColumn extends UDF with EncryptDecryptSupport {
  def evaluate(message: String, key: String, algorithm: String): String = {
    val keyBytes: Array[Byte] = key.getBytes
    val crypt: EncryptDecrypt = algorithm.toUpperCase() match {
      case "GCM"                                => new EncryptDecryptGCM(keyBytes)
      case "ECB"                                => new EncryptDecryptECB(keyBytes)
      case classname if classname.contains(".") => loadEncryptDecryptClass(classname, keyBytes)
      case _                                    => throw new UnsupportedOperationException(s"unsupported en/decryption algorithm ${algorithm}")
    }
    crypt.decrypt(message)
  }
}
