package io.smartdatalake.dataframe

import com.snowflake.snowpark.types.{ArrayType, DataType, StructField, StructType}
import com.snowflake.snowpark.{Column, DataFrame}

object SnowparkLanguageImplementation {

  type SnowparkCaseExpression = com.snowflake.snowpark.CaseExpr
  type SnowparkDataFrame = DataFrame
  type SnowparkColumn = Column
  type SnowparkStructType = StructType
  type SnowparkArrayType = ArrayType
  type SnowparkStructField = StructField
  type SnowparkDataType = DataType
  type SnowparkLanguageType = Language[DataFrame, Column, StructType, DataType]

  val snowparkDataFrameInterpreter: SnowparkLanguageType = new SnowparkLanguageType {

      override def col(colName: String): Column = {
        com.snowflake.snowpark.functions.col(colName)
      }

      override def join(left: DataFrame,
                        right: DataFrame,
                        joinCols: Seq[String]): DataFrame = {
        left.join(right, joinCols)
      }

      override def select(dataFrame: DataFrame,
                          column: Column): DataFrame = {
        dataFrame.select(column)
      }

      override def filter(dataFrame: DataFrame,
                          column: Column): DataFrame = {
        dataFrame.filter(column)
      }

      override def and(left: Column,
                       right: Column): Column = {
        left.and(right)
      }

      override def ===(left: Column, right: Column): Column = {
        left === right
      }

      override def =!=(left: Column, right: Column): Column = {
        left =!= right
      }

      override def lit(value: Any): Column = {
        com.snowflake.snowpark.functions.lit(value)
      }

      override def schema(dataFrame: DataFrame): StructType = {
        dataFrame.schema
      }

      override def columns(dataFrame: DataFrame): Seq[String] = {
        dataFrame.schema.map(column => column.name)
      }
    }

}
