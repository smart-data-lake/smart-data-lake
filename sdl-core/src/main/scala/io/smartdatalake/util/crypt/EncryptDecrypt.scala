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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec

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
