/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package org.apache.spark.sql.classic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.StructType

/**
 * Change the metadata of the columns of a DataFrame, including the metadata of nested fields.
 *
 * This is implemented by replacing the output attributes of the logical plan, which needs `Dataset.ofRows`.
 * As that is private to Spark, this helper lives in Sparks package.
 *
 * Note that the alternatives all change the data path of the DataFrame and are expensive:
 *  - `Dataset.to(schema)` rebuilds every struct column with `named_struct`. This is not optimized away and
 *    roughly doubles the execution time of a scan over a nested column.
 *  - `SparkSession.createDataFrame(df.rdd, schema)` forces a round trip through an RDD.
 *
 * The projection created here consists of plain attribute references only, so it is removed again by Sparks
 * optimizer. Execution plan and runtime therefore stay unchanged.
 */
object SdlDataFrameMetadata {

  /**
   * Apply the metadata of the fields of the given schema to the corresponding columns of the DataFrame.
   * Columns are matched by name, columns without a matching field are left unchanged.
   *
   * @return the DataFrame with the metadata applied, or the unchanged DataFrame if there is nothing to change.
   */
  def withColumnMetadata(df: DataFrame, schema: StructType): DataFrame = {
    val plan = df.queryExecution.analyzed
    val newOutput = plan.output.map { attribute =>
      schema.find(_.name == attribute.name).map { field =>
        if (field.dataType == attribute.dataType && field.metadata == attribute.metadata) attribute
        else attribute.withDataType(field.dataType).withMetadata(field.metadata)
      }.getOrElse(attribute)
    }
    if (newOutput == plan.output) df
    else Dataset.ofRows(df.sparkSession.asInstanceOf[SparkSession], Project(newOutput, plan))
  }
}
