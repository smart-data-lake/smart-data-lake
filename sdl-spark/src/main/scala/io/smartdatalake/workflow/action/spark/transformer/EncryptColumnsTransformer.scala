/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.crypt.{EncryptDecrypt, EncryptDecryptECB, EncryptDecryptGCM, EncryptDecryptSupport}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformer
import org.apache.spark.sql.DataFrame

/**
 * Encryption of specified columns using AES/GCM algorithm.
 *
 * Use this transformer to pseudonymize sensitive attributes before they are persisted. The listed columns are replaced
 * by their cipher text at the same position and get data type String, all other columns are passed through unchanged.
 * The original data type is remembered in the column metadata, so that [[DecryptColumnsTransformer]] can restore it on
 * read. Note that this only works if the output format preserves column metadata (Parquet, Hive, Delta, Iceberg); with
 * formats like CSV the original data type is lost and the decrypted columns stay String.
 * Choose "GCM" (randomized, recommended) unless you need deterministic cipher text for joins or deduplication, in which
 * case use "ECB".
 *
 * Note that the key should not be written into the configuration in clear text, but referenced as a secret, e.g.
 * `###ENV#CRYPT_KEY###`. The same key and algorithm are needed to read the data back, see
 * [[DecryptColumnsTransformer]].
 *
 * Example:
 * {{{
 * actions = {
 *   enc-customers {
 *     type = CopyAction
 *     inputId = stg-customers
 *     outputId = enc-customers
 *     transformers = [{
 *       type = EncryptColumnsTransformer
 *       encryptColumns = ["email", "phone"]
 *       key = "###ENV#CRYPT_KEY###"
 *       algorithm = "GCM"
 *     }]
 *   }
 * }
 * }}}
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param encryptColumns List of columns [columnA, columnB] to be encrypted
 * @param key            contains the id of the provider and the name of the secret with format ###<PROVIDERID>#<SECRETNAME>###,
 *                       e.g. ###ENV#<ENV_VARIABLE_NAME>### to get a secret from an environment variable OR ###CLEAR#mYsEcReTkeY###
 * @param algorithm      Specify: "GCM" (AES/GCM/NoPadding), "ECB" (AES/ECB/PKCS5Padding),
 *                       alternatively a class name extending trait EncryptDecrypt can be provided. DEFAULT: GCM
 */
case class EncryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     encryptColumns: Seq[String],
                                     private val key: StringOrSecret,
                                     algorithm: String = "GCM"
                                    )
  extends SparkDfTransformer with EncryptDecryptSupport {
  private val keyBytes: Array[Byte] = key.resolve().getBytes

  val crypt: EncryptDecrypt = algorithm match {
    case "GCM" => new EncryptDecryptGCM(keyBytes)
    case "ECB" => new EncryptDecryptECB(keyBytes)
    case classname if classname.contains(".") => loadEncryptDecryptClass(classname, keyBytes)
    case _ => throw new UnsupportedOperationException(s"unsupported en/decryption algorithm $algorithm")
  }

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    crypt.encryptColumns(df, encryptColumns)
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = EncryptColumnsTransformer
}

object EncryptColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): EncryptColumnsTransformer = {
    extract[EncryptColumnsTransformer](config)
  }
}
