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

package io.smartdatalake.workflow.action.generic.transformer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

import java.security.SecureRandom
import java.util.Base64
import javax.crypto.{Cipher, SecretKey}
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

trait EncryptDecrypt extends Serializable {
  protected def keyAsBytes: Array[Byte]

  protected def encryptUDF: UserDefinedFunction = udf(encryptFunc _)

  protected def decryptUDF: UserDefinedFunction = udf(decryptFunc _)

  protected def encryptFunc(message: String): String

  protected def decryptFunc(encryptedDataString: String): String

  def encrypt(df: DataFrame, encryptColumns: Seq[String]): DataFrame = {
    encryptColumns.foldLeft(df) {
      case (dfTemp, colName) => dfTemp.withColumn(colName, encryptUDF(col(colName)))
    }
  }

  def decrypt(df: DataFrame, encryptColumns: Seq[String]): DataFrame = {
    encryptColumns.foldLeft(df) {
      case (dfTemp, colName) => dfTemp.withColumn(colName, decryptUDF(col(colName)))
    }
  }

  def generateAesKey(keyBytes: Array[Byte]): SecretKey = {
    new SecretKeySpec(keyBytes, "AES")
  }

}

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

class EncryptDecryptGCM(keyBytes: Array[Byte]) extends EncryptDecrypt {
  override protected val keyAsBytes: Array[Byte] = keyBytes

  private val ALGORITHM_STRING: String = "AES/GCM/NoPadding"
  private val IV_SIZE = 128
  private val TAG_BIT_LENGTH = 128

  private val secureRandom = new SecureRandom()

  private val aesKey: SecretKey = generateAesKey(keyAsBytes)
//  @transient lazy private val gcmParameterSpec = generateGcmParameterSpec()

  override protected def encryptFunc(message: String): String = {
    val gcmParameterSpec = generateGcmParameterSpec()
    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    cipher.init(Cipher.ENCRYPT_MODE, aesKey, gcmParameterSpec, new SecureRandom())

    val encryptedMessage = cipher.doFinal(message.getBytes)
    encodeData(gcmParameterSpec, encryptedMessage)
  }

  override protected def decryptFunc(encryptedDataString: String): String = {
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
