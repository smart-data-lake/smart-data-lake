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

/* General aspects:
 * There are various algorithms available. The most common and considered secure is:
 * AES with
 * Cipher Block Chaining (ECB is discouraged)
 * The Key should be large enough
 * e.g. see https://www.europeanpaymentscouncil.eu/sites/default/files/kb/file/2022-03/EPC342-08%20v11.0%20Guidelines%20on%20Cryptographic%20Algorithms%20Usage%20and%20Key%20Management.pdf
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

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import java.security.MessageDigest
import java.util
import org.apache.spark.sql.functions.{col, lit, udf}

trait EncryptDecrypt {
  val cryptUDF: UserDefinedFunction = udf(encrypt _)
  val cur_algorithm: String = "AES/ECB/PKCS5Padding"

  def process(df: DataFrame, encryptColumns: Seq[String], key: String): DataFrame = {

    var df_enc = df
    for (colName <- encryptColumns) {
      df_enc = df_enc.withColumn(colName, cryptUDF(lit(key), col(colName)))
      df_enc = df_enc.drop(colName.concat("key"))
    }
    df_enc
  }

  def encrypt(key: String, value: String): String = {
    val cipher: Cipher = Cipher.getInstance(cur_algorithm)
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key), keyToIv(key))
    org.apache.commons.codec.binary.Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }

  def decrypt(key: String, encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance(cur_algorithm)
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key), keyToIv(key))
    new String(cipher.doFinal(org.apache.commons.codec.binary.Base64.decodeBase64(encryptedValue)))
  }

  def keyToSpec(key: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = key.getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  def keyToIv(key:String): IvParameterSpec = {
    var keyBytes: Array[Byte] = key.getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1") // TODO do we need incease?
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16) // TODO do we need incease?
    new IvParameterSpec(keyBytes) //TODO , 42, 1024)
  }

}

//TODO add more details to the description
/**
 * Encrypt specified columns of the DataFrame using javax.crypto.cipther algorithm.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param encryptColumns List of [columnA, columnB] to be encrypted
 * @param keyVariable contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
 *                          e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable OR CLEAR#mYsEcReTkeY
 * @param algorithm Optional specifies the type of encryption algorithm, default: "AES/ECB/PKCS5Padding"
 */
case class EncryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     encryptColumns: Seq[String],
                                     keyVariable: String,
                                     algorithm: String = "AES/CBC/PKCS5Padding"
                                     )
  extends SparkDfTransformer with EncryptDecrypt {
  private[smartdatalake] val key: String = SecretsUtil.getSecret(keyVariable)

  override val cryptUDF: UserDefinedFunction = udf(encrypt _)
  override val cur_algorithm: String = algorithm

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    process(df, encryptColumns, key)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = EncryptColumnsTransformer
}

object EncryptColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): EncryptColumnsTransformer = {
    extract[EncryptColumnsTransformer](config)
  }
}

/**
 * Decrypt specified columns of the DataFrame using javax.crypto.cipther algorithm.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param decryptColumns List of [columnA, columnB] to be decrypted
 * @param keyVariable contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
 *                          e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable OR CLEAR#mYsEcReTkeY
 */
case class DecryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     decryptColumns: Seq[String],
                                     keyVariable: String,
                                     algorithm: String = "AES/CBC/PKCS5Padding"
                                    )
  extends SparkDfTransformer with EncryptDecrypt {
  private[smartdatalake] val key: String = SecretsUtil.getSecret(keyVariable)

  override val cryptUDF: UserDefinedFunction = udf(decrypt _)
  override val cur_algorithm: String = algorithm

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    process(df, decryptColumns, key)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = DecryptColumnsTransformer
}

object DecryptColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DecryptColumnsTransformer = {
    extract[DecryptColumnsTransformer](config)
  }
}
