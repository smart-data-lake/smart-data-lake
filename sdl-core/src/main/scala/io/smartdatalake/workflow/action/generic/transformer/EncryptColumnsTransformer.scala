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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.secrets.{SecretsUtil, StringOrSecret}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.DataFrame

/**
 * Encryption of specified columns using AES/GCM algorithm.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param encryptColumns List of columns [columnA, columnB] to be encrypted
 * @param keyVariable    contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
 *                       e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable OR CLEAR#mYsEcReTkeY
 * @param algorithm      Specify: "GCM" (AES/GCM/NoPadding), "ECB" (AES/ECB/PKCS5Padding),
 *                       alternatively a class name extending trait EncryptDecrypt can be provided. DEFAULT: GCM
 */
case class EncryptColumnsTransformer(override val name: String = "encryptColumns",
                                     override val description: Option[String] = None,
                                     encryptColumns: Seq[String],
                                     @Deprecated @deprecated("Use `key` instead", "2.5.0") private val keyVariable: Option[String] = None,
                                     private val key: Option[StringOrSecret],
                                     algorithm: String = "GCM"
                                    )
  extends SparkDfTransformer {
  private val cur_key: StringOrSecret = key.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(keyVariable.get))
  private val keyBytes: Array[Byte] = cur_key.resolve().getBytes

  val crypt: EncryptDecrypt = algorithm match {
    case "GCM" => new EncryptDecryptGCM(keyBytes)
    case "ECB" => new EncryptDecryptECB(keyBytes)
    case classname if classname.contains(".") => loadEncryptDecryptClass(classname)
    case _ => throw new UnsupportedOperationException(s"unsupported en/decryption algorithm ${algorithm}")
  }

  private def loadEncryptDecryptClass(classname: String): EncryptDecrypt = {
    val clazz = Environment.classLoader.loadClass(classname)
    assert(clazz.getConstructors.exists(con => con.getParameterTypes.toSeq == Seq(classOf[Array[Byte]])),
      s"Class $classname needs to have a constructor with 1 parameter of type 'Array[Byte]'!")
    clazz.getConstructor(classOf[Array[Byte]]).newInstance(keyBytes).asInstanceOf[EncryptDecrypt]
  }

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    crypt.encrypt(df, encryptColumns)
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = EncryptColumnsTransformer
}

object EncryptColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): EncryptColumnsTransformer = {
    extract[EncryptColumnsTransformer](config)
  }
}
