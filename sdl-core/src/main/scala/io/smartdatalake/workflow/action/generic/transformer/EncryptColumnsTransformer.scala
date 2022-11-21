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

package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.secrets.SecretsUtil
import io.smartdatalake.workflow.ActionPipelineContext

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.security.SecureRandom
import java.util.Base64
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, KeyGenerator, SecretKey}
import org.apache.spark.sql.functions.{col, lit, udf}

trait EncryptDecrypt {
  val key: Array[Byte] = "test234".getBytes
  val cryptUDF: UserDefinedFunction = udf(encrypt _)
  private val ALGORITHM_STRING: String = "AES/GCM/PKCS5Padding"
  private val ALGO: String = "AES"
  private val AES_KEY_SIZE = 256
  private val AES_KEY_SIZE_IN_BYTE = AES_KEY_SIZE / 8
  private val IV_SIZE = 128
  private val TAG_BIT_LENGTH = 128

  private val secureRandom = new SecureRandom()

  def process(df: DataFrame, encryptColumns: Seq[String]): DataFrame = {
    var dfEnc = df
    for (colName <- encryptColumns) {
      dfEnc = dfEnc.withColumn(colName, cryptUDF(col(colName)))
    }
    dfEnc
  }

  def encrypt(message: String): String = {
    val aesKey: SecretKey = generateAesKey()
    val gcmParameterSpec = generateGcmParameterSpec()

    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    cipher.init(Cipher.ENCRYPT_MODE, aesKey, gcmParameterSpec, new SecureRandom())
    cipher.updateAAD(key)

    val encryptedMessage = cipher.doFinal(message.getBytes)
    encodeData(aesKey, gcmParameterSpec, encryptedMessage)
  }

  def decrypt(encryptedDataString: String): String = {
    val (aesKey, gcmParameterSpec, encryptedMessage) = decodeData(encryptedDataString)

    val cipher = Cipher.getInstance(ALGORITHM_STRING)
    cipher.init(Cipher.DECRYPT_MODE, aesKey, gcmParameterSpec, new SecureRandom())
    cipher.updateAAD(key)

    val message = cipher.doFinal(encryptedMessage)
    new String(message)
  }

  private def generateAesKey(): SecretKey = {
    val keygen = KeyGenerator.getInstance(ALGO)
    keygen.init(AES_KEY_SIZE)
    keygen.generateKey
  }

  private def generateGcmParameterSpec(): GCMParameterSpec = {
    val iv = new Array[Byte](IV_SIZE)
    secureRandom.nextBytes(iv)
    new GCMParameterSpec(TAG_BIT_LENGTH, iv)
  }

  private def encodeData(aesKey: SecretKey, gcmParameterSpec: GCMParameterSpec, encryptedMessage: Array[Byte]): String = {
    val data = aesKey.getEncoded ++ gcmParameterSpec.getIV ++ encryptedMessage
    Base64.getEncoder.encodeToString(data)
  }

  private def decodeData(encodedData: String): (SecretKeySpec, GCMParameterSpec, Array[Byte]) = {
    val data = Base64.getDecoder.decode(encodedData)
    val aesKey = new SecretKeySpec(data.take(AES_KEY_SIZE_IN_BYTE), ALGO)
    val iv = data.slice(AES_KEY_SIZE_IN_BYTE, AES_KEY_SIZE_IN_BYTE + IV_SIZE)
    val gcmParameterSpec = new GCMParameterSpec(TAG_BIT_LENGTH, iv)
    val encryptedMessage = data.drop(AES_KEY_SIZE_IN_BYTE + IV_SIZE)
    (aesKey, gcmParameterSpec, encryptedMessage)
  }
}

/**
 * Encryption of specified columns using AES/GCM algorithm.
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param encryptColumns List of columns [columnA, columnB] to be encrypted
 * @param keyVariable    contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
 *                          e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable OR CLEAR#mYsEcReTkeY
 */
case class EncryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     encryptColumns: Seq[String],
                                     keyVariable: String,
                                     )
  extends SparkDfTransformer with EncryptDecrypt {
  private[smartdatalake] val cur_key: String = SecretsUtil.getSecret(keyVariable)

  override val key = cur_key.getBytes
  override val cryptUDF: UserDefinedFunction = udf(encrypt _)

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    process(df, encryptColumns)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = EncryptColumnsTransformer
}

object EncryptColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): EncryptColumnsTransformer = {
    extract[EncryptColumnsTransformer](config)
  }
}

/**
 * Decryption of specified columns using AES/GCM algorithm.
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param decryptColumns List of columns [columnA, columnB] to be encrypted
 * @param keyVariable    contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
 *                          e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable OR CLEAR#mYsEcReTkeY
 */
case class DecryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     decryptColumns: Seq[String],
                                     keyVariable: String,
                                     algorithm: String = "AES/CBC/PKCS5Padding"
                                    )
  extends SparkDfTransformer with EncryptDecrypt {
  private[smartdatalake] val cur_key: String = SecretsUtil.getSecret(keyVariable)

  override val key = cur_key.getBytes
  override val cryptUDF: UserDefinedFunction = udf(decrypt _)

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    process(df, decryptColumns)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = DecryptColumnsTransformer
}

object DecryptColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DecryptColumnsTransformer = {
    extract[DecryptColumnsTransformer](config)
  }
}
