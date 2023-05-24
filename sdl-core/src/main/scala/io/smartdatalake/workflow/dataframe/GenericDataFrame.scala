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

package io.smartdatalake.workflow.dataframe

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{SQLUtil, SchemaUtil}
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.util.spark.DataFrameUtil.{normalizeToAscii, strCamelCase2LowerCaseWithUnderscores}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.Type

/**
 * Interface for all Generic objects defining it's subfeed type
 */
trait GenericTypedObject {
  def subFeedType: Type
}

/**
 * Interface for a generic data frame.
 */
trait GenericDataFrame extends GenericTypedObject {

  def schema: GenericSchema

  def join(other: GenericDataFrame, joinCols: Seq[String]): GenericDataFrame

  def select(columns: Seq[GenericColumn]): GenericDataFrame
  def select(column: GenericColumn): GenericDataFrame = select(Seq(column))

  def groupBy(columns: Seq[GenericColumn]): GenericGroupedDataFrame

  def agg(columns: Seq[GenericColumn]): GenericDataFrame

  def unionByName(other: GenericDataFrame): GenericDataFrame

  def except(other: GenericDataFrame): GenericDataFrame

  def filter(expression: GenericColumn): GenericDataFrame
  def where(expression: GenericColumn): GenericDataFrame = filter(expression)

  def collect: Seq[GenericRow]

  def withColumn(colName: String, expression: GenericColumn): GenericDataFrame

  def drop(colName: String): GenericDataFrame

  def createOrReplaceTempView(viewName: String): Unit

  def isEmpty: Boolean

  def count: Long

  def cache: GenericDataFrame

  def uncache: GenericDataFrame

  // instantiate subfeed helper
  private lazy val functions = DataFrameSubFeed.getFunctions(subFeedType)

  /**
   * Log message and DataFrame content
   * @param msg Log message to add before DataFrame content
   * @param loggerFunc Function used to log
   */
  def log(msg: String, loggerFunc: String => Unit): Unit

  /**
   * Create an Observation of metrics on this DataFrame.
   * @param name name of the observation
   * @param aggregateColumns aggregate columns to observe on the DataFrame
   * @return an Observation object which can return observed metrics after execution
   */
  def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean = false): (GenericDataFrame, DataFrameObservation)

  /**
   * returns data frame which consists of those rows which contain at least a null in the specified columns
   */
  def getNulls(cols: Seq[String]): GenericDataFrame = {
    import functions._
    filter(cols.map(col(_).isNull).reduce(_ or _))
  }

  /**
   * Count n-lets of this data frame with respect to specified columns.
   * The result data frame possesses the columns cols and an additional count column countColname.
   */
  def getNonuniqueStats(cols: Seq[String] = schema.columns, countColname: String = "_cnt_"): GenericDataFrame = {
    import functions._
    // for better usability we define empty Array of cols to mean all columns of df
    val colsInDf = if (cols.isEmpty) schema.columns else schema.columns.intersect(cols)
    groupBy(colsInDf.map(col))
      .agg(Seq(functions.count(col("*")).as(countColname)))
      .filter(col(countColname) > lit(1))
  }
  /**
   * Returns rows of this data frame which violate uniqueness for specified columns cols.
   * The result data frame possesses an additional count column countColname.
   *
   * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
   * @return subdataframe of n-lets
   */
  def getNonuniqueRows(cols: Seq[String] = schema.columns): GenericDataFrame = {
    import functions._
    val dfNonUnique = getNonuniqueStats(cols, "_duplicationCount_").drop("_duplicationCount_")
    join(dfNonUnique, cols).select(schema.columns.map(col))
  }

  /**
   * returns data frame which consists of those rows which violate PK condition for specified columns
   */
  def getPKviolators(cols: Seq[String] = schema.columns): GenericDataFrame = getNulls(cols).unionByName(getNonuniqueRows(cols))

  /**
   * Move partition columns at end of DataFrame as required when writing to Hive in Spark > 2.x
   */
  def movePartitionColsLast(partitions: Seq[String])(implicit function: DataFrameFunctions): GenericDataFrame = {
    import function._
    val (partitionCols, nonPartitionCols) = schema.columns.partition(c => partitions.contains(c))
    val newColOrder = nonPartitionCols ++ partitionCols
    select(newColOrder.map(col))
  }

  /**
   * Convert column names to lower case
   */
  def colNamesLowercase(implicit function: DataFrameFunctions): GenericDataFrame = {
    import function._
    select(schema.columns.map(c => col(c).as(c.toLowerCase())))
  }

  /**
   * Standardize column names according to enabled rules.
   */
  def standardizeColNames(camelCaseToLower: Boolean = true, normalizeToAscii: Boolean = true, removeNonStandardSQLNameChars: Boolean = true)( implicit function: DataFrameFunctions): GenericDataFrame = {
    def standardizeColName(name: String): String = {
      var standardName = name
      standardName = if (normalizeToAscii) DataFrameUtil.normalizeToAscii(standardName) else standardName
      standardName = if (camelCaseToLower) DataFrameUtil.strCamelCase2LowerCaseWithUnderscores(standardName) else standardName.toLowerCase
      standardName = if (removeNonStandardSQLNameChars) DataFrameUtil.removeNonStandardSQLNameChars(standardName) else standardName
      // return
      standardName
    }
    import function._
    select(schema.columns.map(c => col(SQLUtil.sparkQuoteSQLIdentifier(c)).as(standardizeColName(c))))
  }

  /**
   * symmetric difference of two data frames: (df∪df2)∖(df∩df2) = (df∖df2)∪(df2∖df)
   * @param diffColName : name of boolean column which indicates whether the row belongs to df
   * @return data frame
   */
  def symmetricDifference(other: GenericDataFrame, diffColName: String = "_in_first_df"): GenericDataFrame = {
    require(schema.columns.map(_.toLowerCase).toSet == other.schema.columns.map(_.toLowerCase).toSet, "DataFrames must have the same columns for symmetricDifference calculation")
    // reorder columns according to the original df for calculating symmetricDifference
    val colOrder = schema.columns.map(functions.col)
    val dfOtherPrep = other.select(colOrder)
    this.except(dfOtherPrep).withColumn(diffColName, functions.lit(true))
      .unionByName(dfOtherPrep.except(this).withColumn(diffColName, functions.lit(false)))
  }

  /**
   * compares df with df2
   * @return true if both data frames have the same cardinality, schema and an empty symmetric difference
   */
  def isEqual(other: GenericDataFrame): Boolean = {
    // As a set-theoretic function symmetricDifference ignores multiple occurences of the same row.
    // Thus we need also to compare the cardinalities and the schemata of the two data frames.
    // For the schema, the order of columns doesn't need to match.
    // Note that we ignore the nullability of the columns to compare schemata.
    isSchemaEqualIgnoreNullabilty(other) && symmetricDifference(other).isEmpty && this.count == other.count
  }

  /**
   * compares df with df2 ignoring schema nullability
   * @return true if both data frames have the same cardinality, schema (ignoring nullability) and an empty symmetric difference
   */
  def isSchemaEqualIgnoreNullabilty(other: GenericDataFrame): Boolean = {
    SchemaUtil.schemaDiff(this.schema, other.schema, ignoreNullable = true).isEmpty && SchemaUtil.schemaDiff(other.schema, this.schema, ignoreNullable = true).isEmpty
  }

  /**
   * Create an empty SubFeed for this subFeedType.
   */
  def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed

}

/**
 * Interface for the result of a df.groupBy on a GenericDataFrame
 */
trait GenericGroupedDataFrame extends GenericTypedObject {
  def agg(columns: Seq[GenericColumn]): GenericDataFrame
}

/**
 * Interface for the schema of a GenericDataFrame
 */
trait GenericSchema extends GenericTypedObject {

  /**
   * Convert schema to another SubFeedType.
   */
  def convert(toSubFeedType: Type): GenericSchema = {
    SchemaConverter.convert(this, toSubFeedType)
  }
  def equalsSchema(schema: GenericSchema): Boolean = {
    diffSchema(schema).nonEmpty || schema.diffSchema(this).nonEmpty
  }
  def diffSchema(schema: GenericSchema): Option[GenericSchema]
  def columns: Seq[String]
  def fields: Seq[GenericField]
  def sql: String
  def add(colName: String, dataType: GenericDataType): GenericSchema
  def add(field: GenericField): GenericSchema
  def remove(colName: String): GenericSchema
  def filter(func: GenericField => Boolean): GenericSchema
  def getEmptyDataFrame(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame
  def getDataType(colName: String): GenericDataType
  def makeNullable: GenericSchema
  def toLowerCase: GenericSchema
  def removeMetadata: GenericSchema
}

/**
 * Interface for the columns of a GenericDataFrame
 */
trait GenericColumn extends GenericTypedObject {
  def ===(other: GenericColumn): GenericColumn
  def >(other: GenericColumn): GenericColumn
  def <(other: GenericColumn): GenericColumn
  def +(other: GenericColumn): GenericColumn
  def -(other: GenericColumn): GenericColumn
  def /(other: GenericColumn): GenericColumn
  def *(other: GenericColumn): GenericColumn
  def and(other: GenericColumn): GenericColumn
  def or(other: GenericColumn): GenericColumn
  @scala.annotation.varargs
  def isin(list: Any*): GenericColumn
  def isNull: GenericColumn
  def as(name: String): GenericColumn
  def cast(dataType: GenericDataType): GenericColumn
  /**
   * Convert expression to SQL representation
   */
  def exprSql: String
}

/**
 * Interface for the fields of a GenericSchema or struct type
 */
trait GenericField extends GenericTypedObject {
  def name: String
  def dataType: GenericDataType
  def nullable: Boolean
  def makeNullable: GenericField
  def toLowerCase: GenericField
  def removeMetadata: GenericField
}

/**
 * Interface for the data type of a GenericField
 */
trait GenericDataType extends GenericTypedObject {
  def isSortable: Boolean
  def isSimpleType: Boolean
  def typeName: String
  def sql: String
  def makeNullable: GenericDataType
  def toLowerCase: GenericDataType
  def removeMetadata: GenericDataType
}

/**
 * Mixin trait for a StructDataType in addition to interface GenericDataType.
 */
trait GenericStructDataType { this: GenericDataType =>
  def fields: Seq[GenericField]
  def withOtherFields[T](other: GenericStructDataType with GenericDataType, func: (Seq[GenericField],Seq[GenericField]) => T): T
}

/**
 * Mixin trait for a ArrayDataType in addition to interface GenericDataType.
 */
trait GenericArrayDataType { this: GenericDataType =>
  def elementDataType: GenericDataType
  def withOtherElementType[T](other: GenericArrayDataType with GenericDataType, func: (GenericDataType,GenericDataType) => T): T
  def containsNull: Boolean // Indicates array might contain null entries
}

/**
 * Mixin trait for a MapDataType in addition to interface GenericDataType.
 */
trait GenericMapDataType { this: GenericDataType =>
  def keyDataType: GenericDataType
  def valueDataType: GenericDataType
  def withOtherKeyType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType,GenericDataType) => T): T
  def withOtherValueType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType,GenericDataType) => T): T
  def valueContainsNull: Boolean // Indicates if map values might be set to null
}

/**
 * Interface for the rows of a GenericDataFrame.
 */
trait GenericRow extends GenericTypedObject {
  def get(index: Int): Any
  def getAs[T](index: Int): T
  //Note: getAs[T](fieldName: String) can not be implemented as in Snowpark a Row does not know the names of its fields!
  def toSeq: Seq[Any]
}
