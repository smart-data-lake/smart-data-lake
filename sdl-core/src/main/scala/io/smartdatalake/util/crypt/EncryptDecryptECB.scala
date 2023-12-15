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

package io.smartdatalake.util.crypt

import java.util.Base64
import javax.crypto.{Cipher, SecretKey}

class EncryptDecryptECB(keyBytes: Array[Byte]) extends EncryptDecrypt {
  override protected val keyAsBytes: Array[Byte] = keyBytes
  private val ALGORITHM_STRING: String = "AES/ECB/PKCS5Padding"

  @transient lazy private val cipherEncrypt: Cipher = {
    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    val secretKey: SecretKey = generateAesKey(keyAsBytes)
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)
    cipher
  }
  @transient lazy private val cipherDecrypt: Cipher = {
    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    val secretKey: SecretKey = generateAesKey(keyAsBytes)
    cipher.init(Cipher.DECRYPT_MODE, secretKey)
    cipher
  }

  override protected def encryptFunc(message: String): String = {
    val data = cipherEncrypt.doFinal(message.getBytes())
    Base64.getEncoder.encodeToString(data)
  }

  override protected def decryptFunc(encryptedDataString: String): String = {
    val data = Base64.getDecoder.decode(encryptedDataString)
    val message = cipherDecrypt.doFinal(data)
    new String(message)
  }

}
