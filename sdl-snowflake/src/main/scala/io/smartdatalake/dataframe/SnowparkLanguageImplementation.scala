package io.smartdatalake.dataframe

import io.smartdatalake.dataframe.DomainSpecificLanguage.Language

object SnowparkLanguageImplementation {


  val snowparkDataFrameInterpreter: Language[SnowparkDataFrame, SnowparkColumn, SnowparkStructType, SnowparkDataType] =
    new Language[SnowparkDataFrame, SnowparkColumn, SnowparkStructType, SnowparkDataType] {
      override def col(colName: String): SnowparkColumn = {
        com.snowflake.snowpark.functions.col(colName)
      }

      override def join(left: SnowparkDataFrame,
                        right: SnowparkDataFrame,
                        joinCols: Seq[String]): SnowparkDataFrame = {
        left.join(right, joinCols)
      }

      override def select(dataFrame: SnowparkDataFrame,
                          column: SnowparkColumn): SnowparkDataFrame = {
        dataFrame.select(column)
      }

      override def filter(dataFrame: SnowparkDataFrame,
                          column: SnowparkColumn): SnowparkDataFrame = {
        dataFrame.filter(column)
      }

      override def and(left: SnowparkColumn,
                       right: SnowparkColumn): SnowparkColumn = {
        left.and(right)
      }

      override def ===(left: SnowparkColumn, right: SnowparkColumn): SnowparkColumn = {
        left === right
      }

      override def =!=(left: SnowparkColumn, right: SnowparkColumn): SnowparkColumn = {
        left =!= right
      }

      override def lit(value: Any): SnowparkColumn = {
        com.snowflake.snowpark.functions.lit(value)
      }

      override def schema(dataFrame: SnowparkDataFrame): SnowparkStructType = {
        dataFrame.schema
      }

      override def columns(dataFrame: SnowparkDataFrame): Seq[String] = {
        dataFrame.schema.map(column => column.name)
      }
    }

}
