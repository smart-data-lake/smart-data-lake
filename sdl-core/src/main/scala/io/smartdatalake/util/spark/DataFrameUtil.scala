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

package io.smartdatalake.util.spark

import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataframe.spark.{SparkField, SparkSchema}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

/**
 * Provides utility functions for [[DataFrame]]s.
 */
@deprecated(since = "2.8.2")
object DataFrameUtil {

  implicit class DfSDL(df: DataFrame) extends SmartDataLakeLogger with dataset.Quality {

    /**
     * compares df with df2
     *
     * @param df2 : data frame to comapre with
     * @return true if both data frames have the same cardinality, schema and an empty symmetric difference
     */
    def isEqual(df2: DataFrame): Boolean = {
      // As a set-theoretic function symmetricDifference ignores multiple occurences of the same row.
      // Thus we need also to compare the cardinalities and the schemata of the two data frames.
      // For the schema, the order of columns doesn't need to match.
      // Note that we ignore the nullability of the columns to compare schemata.
      isSchemaEqualIgnoreNullabilty(df2) && symmetricDifference(df2).isEmpty && df.count() == df2.count()
    }

    def isSchemaEqualIgnoreNullabilty(df2: DataFrame): Boolean = {
      SchemaUtil.schemaDiff(SparkSchema(df.schema), SparkSchema(df2.schema), ignoreNullable = true).isEmpty && SchemaUtil.schemaDiff(SparkSchema(df2.schema), SparkSchema(df.schema), ignoreNullable = true).isEmpty
    }

    /**
     * symmetric difference of two data frames: (df∪df2)∖(df∩df2) = (df∖df2)∪(df2∖df)
     *
     * @param df2         : data frame to compare with
     * @param diffColName : name of boolean column which indicates whether the row belongs to df
     * @return data frame
     */
    def symmetricDifference(df2: DataFrame, diffColName: String = "_in_first_df"): DataFrame = {
      require(df.columns.map(_.toLowerCase).toSet == df2.columns.map(_.toLowerCase).toSet, "DataFrames must have the same columns for symmetricDifference calculation")
      // reorder columns according to the original df for calculating symmetricDifference
      val colOrder = df.columns.map(col)
      df.except(df2.select(colOrder: _*)).withColumn(diffColName, lit(true))
        .unionByName(df2.select(colOrder: _*).except(df).withColumn(diffColName, lit(false)))
    }

    /**
     * If colName is defined, creates an additional column with a given expression on a DataFrame
     */
    def withOptionalColumn(colName: Option[String], expr: Column): DataFrame = {
      if (colName.isDefined) df.withColumn(colName.get, expr)
      else df
    }

    /**
     * Execute df.show and return it as String instead of printing it directly
     */
    def showString(): String = DatasetHelper.showString(df)
  }

  /**
   * pimpMyLibrary pattern to add DataFrameReader utility functions
   */
  implicit class DataFrameReaderUtils(reader: DataFrameReader) {
    def optionalSchema(schema: Option[StructType]): DataFrameReader = {
      if (schema.isDefined) reader.schema(schema.get) else reader
    }

    def optionalOption(key: String, value: Option[String]): DataFrameReader = {
      if (value.isDefined) reader.option(key, value.get) else reader
    }
  }

  /**
   * pimpMyLibrary pattern to add DataFrameWriter utility functions
   */
  implicit class DataFrameWriterUtils[T](writer: DataFrameWriter[T]) {
    def optionalPartitionBy(partitions: Seq[String]): DataFrameWriter[T] = {
      if (partitions.nonEmpty) writer.partitionBy(partitions: _*) else writer
    }

    def optionalOption(key: String, value: Option[String]): DataFrameWriter[T] = {
      if (value.isDefined) writer.option(key, value.get) else writer
    }

    def conditionalOption(key: String, activated: Boolean, value: () => String): DataFrameWriter[T] = {
      if (activated) writer.option(key, value()) else writer
    }
  }
}
