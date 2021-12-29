package io.smartdatalake

package object dataframe {
  type SnowparkCaseExpression = com.snowflake.snowpark.CaseExpr
  type SparkDataFrame = org.apache.spark.sql.DataFrame
  type SnowparkDataFrame = com.snowflake.snowpark.DataFrame
  type SparkColumn = org.apache.spark.sql.Column
  type SnowparkColumn = com.snowflake.snowpark.Column
  type SparkStructType = org.apache.spark.sql.types.StructType
  type SnowparkStructType = com.snowflake.snowpark.types.StructType
  type SparkArrayType = org.apache.spark.sql.types.ArrayType
  type SnowparkArrayType = com.snowflake.snowpark.types.ArrayType
  type SparkMapType = org.apache.spark.sql.types.MapType
  type SparkStructField = org.apache.spark.sql.types.StructField
  type SnowparkStructField = com.snowflake.snowpark.types.StructField
  type SparkDataType = org.apache.spark.sql.types.DataType
  type SnowparkDataType = com.snowflake.snowpark.types.DataType
}

