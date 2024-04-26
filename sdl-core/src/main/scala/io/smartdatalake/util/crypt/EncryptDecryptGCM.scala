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

import java.security.SecureRandom
import java.util.Base64
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.{Cipher, SecretKey}

class EncryptDecryptGCM(keyBytes: Array[Byte]) extends EncryptDecrypt {
  override protected val keyAsBytes: Array[Byte] = keyBytes

  private val ALGORITHM_STRING: String = "AES/GCM/NoPadding"
  private val IV_SIZE = 128
  private val TAG_BIT_LENGTH = 128

  private val secureRandom = new SecureRandom()

  private val aesKey: SecretKey = generateAesKey(keyAsBytes)

  override def encrypt(message: String): String = {
    if (message == null) return message
    val gcmParameterSpec = generateGcmParameterSpec()
    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    cipher.init(Cipher.ENCRYPT_MODE, aesKey, gcmParameterSpec, new SecureRandom())

    val encryptedMessage = cipher.doFinal(message.getBytes)
    encodeData(gcmParameterSpec, encryptedMessage)
  }

  override def decrypt(encryptedDataString: String): String = {
    if (encryptedDataString == null) return encryptedDataString
    val (gcmParameterSpec, encryptedMessage) = decodeData(encryptedDataString)

    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    cipher.init(Cipher.DECRYPT_MODE, aesKey, gcmParameterSpec, new SecureRandom())

    val message = cipher.doFinal(encryptedMessage)
    new String(message)
  }

  private def generateGcmParameterSpec(): GCMParameterSpec = {
    val iv = new Array[Byte](IV_SIZE)
    secureRandom.nextBytes(iv)
    new GCMParameterSpec(TAG_BIT_LENGTH, iv)
  }

  private def encodeData(gcmParameterSpec: GCMParameterSpec, encryptedMessage: Array[Byte]): String = {
    val data = gcmParameterSpec.getIV ++ encryptedMessage
    println(Base64.getEncoder.encodeToString(gcmParameterSpec.getIV), Base64.getEncoder.encodeToString(encryptedMessage))
    Base64.getEncoder.encodeToString(data)
  }

  private def decodeData(encodedData: String): (GCMParameterSpec, Array[Byte]) = {
    val data = Base64.getDecoder.decode(encodedData)
    val iv = data.take(IV_SIZE)
    val gcmParameterSpec = new GCMParameterSpec(TAG_BIT_LENGTH, iv)
    val encryptedMessage = data.drop(IV_SIZE)
    (gcmParameterSpec, encryptedMessage)
  }
}
