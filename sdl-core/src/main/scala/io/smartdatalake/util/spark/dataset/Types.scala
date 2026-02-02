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

package io.smartdatalake.util.spark.dataset

import org.apache.spark.sql.types.{DataType, StructField, StructType}

trait Types extends Serializable {

  /**
   * creates Struct
   *
   * @param fields : fields as triple (name, data type, is nullable)
   * @return StructType
   */
  final def createStruct(fields: Array[(String, DataType, Boolean)]): StructType = StructType(
    fields.map(x => StructField(name = x._1, dataType = x._2: DataType, nullable = x._3))
  )

  /**
   * creates Struct with nullable fields
   *
   * @param fields : nullable fields as pair (name, data type)
   * @return StructType
   */
  final def createStruct(fields: Array[(String, DataType)]): StructType = createStruct(
    fields.map { case (fldName, dTyp) => (fldName, dTyp, true) }
  )

  /**
   * creates Struct with one field
   *
   * @param fieldName : name of field
   * @param fieldType : data type of field
   * @param nullable  : is field nullable ?
   * @return StructType
   */
  final def createStruct(fieldName: String, fieldType: DataType, nullable: Boolean = true): StructType = createStruct(Array((fieldName, fieldType, nullable)))

}
