/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.testutils

import io.circe.yaml
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, DataFrameHelpers, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, to_timestamp}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DecimalType, IntegerType, MapType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

object DataFrameTestHelper {

  lazy val emptyDf: DataFrame = createDf(Map[String, TypedValue]())
  val ts: String => TypedValue = formattedTimeStamp => TypedValue(formattedTimeStamp, TimestampType)
  val str: String => TypedValue = str => TypedValue(str, StringType)
  val int: Integer => TypedValue = int => TypedValue(int, IntegerType)
  val dec: Integer => TypedValue = dec => TypedValue(dec, DecimalType(38, 0))
  val bool: Boolean => TypedValue = bool => TypedValue(bool, BooleanType)

  val strMapArray: Array[Map[String, String]] => TypedValue = strMap => TypedValue(strMap, ArrayType(MapType(StringType, StringType)))
  val strArray: Array[String] => TypedValue = strArray => TypedValue(strArray, ArrayType(StringType))

  val typedNull: DataType => TypedValue = dataType => TypedValue(null, dataType)
  private val logger = LoggerFactory.getLogger(this.getClass)

  def createDfFromYaml(yamlString: String): DataFrame = {
    createDfFromJson(createJsonFromYaml(yamlString))
  }

  def createDfFromJson(json: String): DataFrame = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    spark.read.json(Seq(json).toDS)
  }

  def createJsonFromYaml(yamlString: String): String = {
    yaml.parser.parse(yamlString) match {
      case Left(error) => throw new Exception("Unable to parse YAML: " + error)
      case Right(json) => json.spaces2
    }
  }

  def createDefaultDf(defaultValues: Map[String, TypedValue] = Map())(input: Map[String, TypedValue]*): DataFrame = {
    createDfWithDefaultValues(input: _*)(defaultValues)
  }

  private def createDfWithDefaultValues(input: Map[String, TypedValue]*)(defaultValues: Map[String, TypedValue] = Map()): DataFrame = {

    // check for null values passed in as typed value
    for (
      rowMap <- input :+ defaultValues;
      typedValue <- rowMap.values if typedValue == null
    ) yield throw new IllegalArgumentException(
      "A null value without an explicit type cannot be used to create a DataFrame, as the column type can't be inferred. Use e.g. (null, StringType) instead.")

    // distinct list of all column names (input and default values)
    val columnNames: Seq[String] = (input.flatMap(x => x.keys) ++ defaultValues.keys).distinct

    // list of column types (input and default values, in the same order as the column names)
    val columnType: String => DataType = columnName => {
      val dataTypes = for (
        rowMap <- input :+ defaultValues;
        typedValue <- rowMap.get(columnName)
      ) yield typedValue.dataType
      if (dataTypes.distinct.length > 1) throw new IllegalArgumentException("At least one column has conflicting data types.")
      dataTypes.head
    }

    val columnTypes: Seq[DataType] =
      columnNames.map(columnName => columnType(columnName))

    val namesAndTypes: Seq[(String, DataType)] = columnNames.zip(columnTypes)

    // Special handing of Timestamp and Decimal columns:
    // 1) add the column with a prefix name and the type "String"
    // 2) Add a new Timestamp/Decimal column by converting the string column to a Timestamp/Decimal column using the to_timestamp column operation/the cast operation
    // 3) Remove the prefixed column, leaving a correctly named Timestamp column
    val stringPrefix: String => String = s => s"str$s"
    val intPrefix: String => String = s => s"int$s"
    val namesAndTypeWithTimestampAsString: Seq[(String, DataType)] =
      namesAndTypes.map {
        case (name: String, _: TimestampType) => (stringPrefix(name), StringType)
        case (name: String, _: DecimalType) => (intPrefix(name), IntegerType)
        case (name: String, dataType: Any) => (name, dataType)
      }

    // Create spark schema out of column types
    val structFields: Seq[StructField] =
      namesAndTypeWithTimestampAsString.map {
        case (name: String, dataType: DataType) => StructField(name, dataType, nullable = true)
      }
    val schema: StructType = StructType(structFields)

    // create a table containing the value or else the default value or else null for each row and column
    // creates a list of rows, where a row is a list of values of Any type
    val rowValues: Seq[Seq[Any]] =
    input.map(inputRow =>
      columnNames.map(columnName =>
        inputRow.get(columnName) match {
          case Some(valueAndType) => valueAndType.value
          case None => defaultValues.get(columnName) match {
            case Some(valueAndType) => valueAndType.value
            case None => null
          }
        }
      )
    )

    // Convert rows to spark rows
    val rowData: Seq[Row] =
      rowValues.map(row => Row(row: _*))

    // Initialize test spark session and create DataFrame with the values and schema
    val spark = SparkSession.builder().master("local").getOrCreate()
    val rdd: RDD[Row] = spark.sparkContext.parallelize(rowData)
    val df = spark.createDataFrame(rdd, schema)

    // Special handling of Timestamp and Decimal columns: Steps 2 and 3
    val castTimestampColumn: (DataFrame, String) => DataFrame = (df, columnName) =>
      df.withColumn(columnName, to_timestamp(col(stringPrefix(columnName))))
        .drop(stringPrefix(columnName))
    val castDecimalColumn: (DataFrame, String) => DataFrame = (df, columnName) =>
      df.withColumn(columnName, col(intPrefix(columnName)).cast(DecimalType(38, 0)))
        .drop(intPrefix(columnName))

    val dfTransformations = namesAndTypes.map {
      case (columnName: String, _: TimestampType) => (df: DataFrame) => castTimestampColumn(df, columnName)
      case (columnName: String, _: DecimalType) => (df: DataFrame) => castDecimalColumn(df, columnName)
      case (_: String, _: Any) => (df: DataFrame) => df
    }

    val dfResult = Function.chain(dfTransformations)(df)

    // done!
    dfResult
  }

  def createDf(input: Map[String, TypedValue]*): DataFrame = {
    createDfWithDefaultValues(input: _*)()
  }

  implicit def valueToTypedValue[T](value: T): TypedValue = value match {
    case (value: Any, dataType: DataType) => TypedValue(value, dataType)
    case (null, dataType: DataType) => TypedValue(null, dataType)
    case string: String => TypedValue(string, StringType)
    case int: Int => TypedValue(int, IntegerType)
    case bool: Boolean => TypedValue(bool, BooleanType)
    case _ => throw new Exception("Unable to convert to TypedValue")
  }

  def assertSchemasEqual(expected: StructType, actual: StructType): Unit = {
    val sameSchema = expected.mkString(" ").split(" ").sorted sameElements actual.mkString(" ").split(" ").sorted

    assert(sameSchema, s"schema differs: \nexpected: \n${expected.mkString("\n")}, \nactual: \n${actual.mkString("\n")}")
  }

  /**
   *
   * Check that two DataFrames are equal. This includes
   *  - the schema is equal
   *  - the content is equal
   *
   * @param dfExpected              the expected dataframe
   * @param dfActual             the actual dataframe
   * @param ignoreColumnOrder whether or not to ignore the order of columns
   * @param ignoreNullability whether or not to ignore nullability of fields
   * @return whether or not the dataframes are equal
   */
  def assertDataFramesEqual(dfExpected: DataFrame, dfActual: DataFrame, ignoreColumnOrder: Boolean = true, ignoreNullability: Boolean = true): Unit = {
    require(dfExpected != null && dfActual != null, "DFs must not be null")

    // check columns, if they do not match, we can return here
    val sameColumns = dfExpected.columns.toSeq.sorted == dfActual.columns.toSeq.sorted
    if (!sameColumns) {
      val colsLeftButNotRight = (dfExpected.columns.toSet -- dfActual.columns.toSet).mkString("\n")
      assert(colsLeftButNotRight.isEmpty, s"These columns are only present in the expected DataFrame: \n$colsLeftButNotRight")
      val colsRightButNotLeft = (dfActual.columns.toSet intersect dfExpected.columns.toSet).mkString("\n")
      assert(colsRightButNotLeft.isEmpty, s"These columns are only present in the actual DataFrame: \n$colsRightButNotLeft")
    }

    val (expectedPrime, actualPrime) = if (ignoreColumnOrder) {
      (dfExpected, dfActual.select(dfExpected.columns.map(col): _*))
    } else {
      (dfExpected, dfActual)
    }

    val sameSchema = if (ignoreNullability) {
      // in this case we can compare the simple-string which does include types, but
      // not nullability. This is easier than traversing the schema recursively.
      val expectedSimpleSchema = expectedPrime.schema.simpleString
      val actualSimpleSchema = actualPrime.schema.simpleString
      expectedSimpleSchema == actualSimpleSchema
    } else {
      // compare full schema
      expectedPrime.schema == actualPrime.schema
    }

    if (!sameSchema) {
      val expectedTypes = dfExpected.schema map (structType => structType.name -> structType.dataType) toMap
      val actualTypes = dfActual.schema map (structType => structType.name -> structType.dataType) toMap
      val differentTypes = (dfExpected.schema toSet) diff (dfActual.schema toSet)
      val differentTypesString = differentTypes.map(structType => {
        val treeStringExpected = new StructType(Array(StructField(structType.name, expectedTypes(structType.name), structType.nullable))).treeString
        val treeStringActual = new StructType(Array(StructField(structType.name, actualTypes(structType.name), structType.nullable))).treeString
        s"Actual schema differs from expected schema.\n" +
          s"Column: ${structType.name}\n" +
          s"Expected type: $treeStringExpected,\n" +
          s"Actual type:   $treeStringActual" +
          (if (treeStringActual.length > 20 && treeStringExpected.length > 20) s"In expected but not actual:    ${treeStringExpected diff treeStringActual}\nIn actual but not expected:   ${treeStringActual diff treeStringExpected}" else "")
      }).mkString("\n\n")
      assert(differentTypes.isEmpty, differentTypesString)
    }

    // now compare data
    val expectedPrimeCount = expectedPrime.groupBy(expectedPrime.columns.map(col): _*).agg(count("*").as("rowcount"))
    val actualPrimeCount = actualPrime.groupBy(actualPrime.columns.map(col): _*).agg(count("*").as("rowcount"))
    val (expectedMinusActual, actualMinusExpected) = symmetricDifference(expectedPrimeCount, actualPrimeCount)
    val sameData = expectedMinusActual.count == 0 && actualMinusExpected.count == 0

    if (!sameData) {
      val messageRows = s"non-equal rows\nrows which appear in expected but not in actual:\n " +
        s"${DataFrameHelpers.showString(expectedMinusActual, 100)}\n" +
        s"rows which appear in actual but not in expected:\n ${DataFrameHelpers.showString(actualMinusExpected, 100)}"

      // compute difference at column-level. This is tricky because the equality of rows involve all columns
      // What we can do is to take the rows which show a difference at row-level, and check whether we have columns
      // which do not appear in the other df
      val colsToCheck = expectedMinusActual.columns
      val colsWhichDiff = colsToCheck.collect { case c if {
        val (lmr, rml) = symmetricDifference(expectedMinusActual, actualMinusExpected, col(c))
        lmr.union(rml).count() > 0 // col diffs
      } => c
      }
      val messageColumns = if (colsWhichDiff.nonEmpty) s"The difference is probably in columns : ${colsWhichDiff.mkString(",")}" else ""
      assert(assertion = false, Seq(messageRows, messageColumns).filter(message => !message.isEmpty).mkString("\n"))
    }
  }

  /**
   * Computes the non-equal rows of 2 dataframes, must have same number of columns
   *
   * @param df1 A DataFrame to compare
   * @param df2 A DataFrame to compare
   * @param columns the columns to include in the check, if none are given, take all columns
   * @return (rows from df1 not in df2, rows from df2 not in df1)
   */
  private def symmetricDifference(df1: DataFrame, df2: DataFrame, columns: Column*) = {
    assert(df1.columns.sameElements(df2.columns), "dfs have not same columns")
    val colsToSelect = if (columns.isEmpty) df1.columns.map(col).toSeq else columns
    (df1.select(colsToSelect: _*).except(df2.select(colsToSelect: _*)), df2.select(colsToSelect: _*).except(df1.select(colsToSelect: _*)))
  }

  case class TypedValue(value: Any, dataType: DataType)

}
