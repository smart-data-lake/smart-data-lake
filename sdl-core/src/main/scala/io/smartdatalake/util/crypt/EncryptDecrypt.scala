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

import io.smartdatalake.definitions.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._

import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec

/**
 * implements columns wise encryption/decryptions
 *  encrypted columns have data type String.
 *  The original data type is stored in DataFrame metadata and saved to output format if supported (Parquet, Hive, Delta, Iceberg).
 *  During the decryption the original data type is restored, metadata is maintained.
 */
trait EncryptDecrypt extends Serializable {
  private val metadata_key = "decryptedType"
  protected def keyAsBytes: Array[Byte]

  protected def encryptUDF: UserDefinedFunction = udf(encrypt _)

  protected def decryptUDF: UserDefinedFunction = udf(decrypt _)

  def encrypt(message: String): String

  def decrypt(encryptedDataString: String): String

  def encryptColumns(df: DataFrame, encryptColumns: Seq[String]): DataFrame = {
    encryptColumns.foldLeft(df) {
      case (dfTemp, colName) => {
        val metadata = new MetadataBuilder().putString(metadata_key, dfTemp.schema(colName).dataType.toString()).build()
        dfTemp.withColumn(colName, encryptUDF(col(colName)).as(colName,metadata))
      }
    }
  }

  def decryptColumns(df: DataFrame, encryptColumns: Seq[String]): DataFrame = {
    encryptColumns.foldLeft(df) {
      case (dfTemp, colName) => {
        if (dfTemp.schema(colName).metadata.contains(metadata_key)) {
          val col_type = dfTemp.schema(colName).metadata.getString(metadata_key).replace("Type", "")
          val data_type = DataType.fromDDL(col_type)
          dfTemp.withColumn(colName, decryptUDF(col(colName)).cast(data_type))
        } else {
          dfTemp.withColumn(colName, decryptUDF(col(colName)))
        }
      }
    }
  }

  def generateAesKey(keyBytes: Array[Byte]): SecretKey = {
    new SecretKeySpec(keyBytes, "AES")
  }
}

trait EncryptDecryptSupport {
  def loadEncryptDecryptClass(classname: String, keyAsBytes: Array[Byte]): EncryptDecrypt = {
    val clazz = Environment.classLoader().loadClass(classname)
    assert(clazz.getConstructors.exists(con => con.getParameterTypes.toSeq == Seq(classOf[Array[Byte]])),
      s"Class $classname needs to have a constructor with 1 parameter of type 'Array[Byte]'!")
    clazz.getConstructor(classOf[Array[Byte]]).newInstance(keyAsBytes).asInstanceOf[EncryptDecrypt]
  }
}