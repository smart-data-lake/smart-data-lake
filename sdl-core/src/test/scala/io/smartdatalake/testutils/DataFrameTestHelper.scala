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
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

object DataFrameTestHelper extends Equality {

  val ts: String => TypedValue = formattedTimeStamp => TypedValue(formattedTimeStamp, TimestampType)
  val str: String => TypedValue = str => TypedValue(str, StringType)
  val int: Integer => TypedValue = int => TypedValue(int, IntegerType)
  val dec: Integer => TypedValue = dec => TypedValue(dec, DecimalType(38, 0))
  val bool: Boolean => TypedValue = bool => TypedValue(bool, BooleanType)
  val strMapArray: Array[Map[String, String]] => TypedValue = strMap => TypedValue(strMap, ArrayType(MapType(StringType, StringType)))
  val strArray: Array[String] => TypedValue = strArray => TypedValue(strArray, ArrayType(StringType))
  val typedNull: DataType => TypedValue = dataType => TypedValue(null, dataType)

  private val logger = LoggerFactory.getLogger(this.getClass)

  def emptyDf()(implicit session: SparkSession): DataFrame = createDf(Map[String, TypedValue]())

  def createDf(input: Map[String, TypedValue]*)(implicit session: SparkSession): DataFrame = {
    createDfWithDefaultValues(input: _*)()
  }

  private def createDfWithDefaultValues(input: Map[String, TypedValue]*)(defaultValues: Map[String, TypedValue] = Map())(implicit session: SparkSession): DataFrame = {

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
    val rdd: RDD[Row] = session.sparkContext.parallelize(rowData)
    val df = session.createDataFrame(rdd, schema)

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

  def createDfFromYaml(yamlString: String)(implicit session: SparkSession): DataFrame = {
    createDfFromJson(createJsonFromYaml(yamlString))
  }

  def createDfFromJson(json: String)(implicit session: SparkSession): DataFrame = {
    import session.implicits._

    session.read.json(Seq(json).toDS())
  }

  def createJsonFromYaml(yamlString: String): String = {
    yaml.parser.parse(yamlString) match {
      case Left(error) => throw new Exception("Unable to parse YAML: " + error)
      case Right(json) => json.spaces2
    }
  }

  def createDefaultDf(defaultValues: Map[String, TypedValue] = Map())(input: Map[String, TypedValue]*)(implicit session: SparkSession): DataFrame = {
    createDfWithDefaultValues(input: _*)(defaultValues)
  }

  implicit def valueToTypedValue[T](value: T): TypedValue = value match {
    case (value: Any, dataType: DataType) => TypedValue(value, dataType)
    case (null, dataType: DataType) => TypedValue(null, dataType)
    case string: String => TypedValue(string, StringType)
    case int: Int => TypedValue(int, IntegerType)
    case bool: Boolean => TypedValue(bool, BooleanType)
    case _ => throw new Exception("Unable to convert to TypedValue")
  }

  def assertDataFramesEqualGeneric(dfExpected: GenericDataFrame,
                                   dfActual: GenericDataFrame,
                                   ignoreColumnOrder: Boolean = true,
                                   ignoreNullability: Boolean = true)
                                  (implicit logger: Logger): Unit = {
    (dfExpected, dfActual) match {
      case (dfExpected: SparkDataFrame, dfActual: SparkDataFrame) =>
        assert(dfExpected.inner.equal(dfActual.inner, ignoreColumnOrder, ignoreNullability))
    }
  }

  case class TypedValue(value: Any, dataType: DataType)

  case class ComplexTypeTest(a: String, b: Int)
}
