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

import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait ReadWrite extends Serializable {

  implicit class DataFrameReaderUtils(reader: DataFrameReader) {
    final def optionalSchema(schema: Option[StructType]): DataFrameReader = {
      if (schema.isDefined) reader.schema(schema.get) else reader
    }

    final def optionalOption(key: String, value: Option[String]): DataFrameReader = {
      if (value.isDefined) reader.option(key, value.get) else reader
    }
  }

  implicit class DataFrameWriterUtils[T](writer: DataFrameWriter[T]) {
    final def optionalPartitionBy(partitions: Seq[String]): DataFrameWriter[T] = {
      if (partitions.nonEmpty) writer.partitionBy(partitions: _*) else writer
    }

    final def optionalOption(key: String, value: Option[String]): DataFrameWriter[T] = {
      if (value.isDefined) writer.option(key, value.get) else writer
    }

    final def conditionalOption(key: String, activated: Boolean, value: () => String): DataFrameWriter[T] = {
      if (activated) writer.option(key, value()) else writer
    }
  }
}
