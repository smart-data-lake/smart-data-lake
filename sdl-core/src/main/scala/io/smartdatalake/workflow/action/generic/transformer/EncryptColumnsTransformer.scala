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
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import java.security.MessageDigest
import java.util
import org.apache.spark.sql.functions.{col,lit,udf}

trait EncryptDecrypt {
  val cryptUDF = udf(encrypt _)
  def process(df: DataFrame, encryptColumns: Seq[String], key: String) = {

    var df_enc = df
    for (colName <- encryptColumns) {
      df_enc = df_enc.withColumn(colName, cryptUDF(lit(key), col(colName)))
      df_enc = df_enc.drop(colName.concat("key"))
    }
    df_enc
  }

  def encrypt(key: String, value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key))
    org.apache.commons.codec.binary.Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }

  def decrypt(key: String, encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key))
    new String(cipher.doFinal(org.apache.commons.codec.binary.Base64.decodeBase64(encryptedValue)))
  }

  def keyToSpec(key: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = (SALT + key).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  //TODO define usefull SALT
  private val SALT: String = ""
}

//TODO add more details to the description
/**
 * Encrypt specified columns of the DataFrame using javax.crypto.cipther algorithm.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param encryptColumns List of [columnA, columnB] to be encrypted
 * @param keyVariable contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
 *                          e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable OR CLEAR#mYsEcReTkeY
 */
case class EncryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     encryptColumns: Seq[String],
                                     keyVariable: String
                                     )
  extends SparkDfTransformer with EncryptDecrypt {
  private[smartdatalake] val key: String = SecretsUtil.getSecret(keyVariable)

  override val cryptUDF = udf(encrypt _)

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
                                     keyVariable: String
                                    )
  extends SparkDfTransformer with EncryptDecrypt {
  private[smartdatalake] val key: String = SecretsUtil.getSecret(keyVariable)

  override val cryptUDF = udf(decrypt _)

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
