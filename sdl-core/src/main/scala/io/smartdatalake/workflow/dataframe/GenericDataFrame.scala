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
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.reflect.runtime.universe.Type


trait GenericDataFrame {
  def subFeedType: Type

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

  // instantiate subfeed helper
  private lazy val helper = DataFrameSubFeed.getHelper(subFeedType)

  /**
   * returns data frame which consists of those rows which contain at least a null in the specified columns
   */
  def getNulls(cols: Seq[String]): GenericDataFrame = {
    import helper._
    filter(cols.map(col(_).isNull).reduce(_ or _))
  }

  /**
   * Count nlets of this data frame with respect to specified columns.
   * The result data frame possesses the columns cols and an additional count column countColname.
   */
  def getNonuniqueStats(cols: Seq[String] = schema.columns, countColname: String = "_cnt_"): GenericDataFrame = {
    import helper._
    // for better usability we define empty Array of cols to mean all columns of df
    val colsInDf = if (cols.isEmpty) schema.columns else schema.columns.intersect(cols)
    groupBy(colsInDf.map(col))
      .agg(Seq(helper.count(col("*")).as(countColname)))
      .filter(col(countColname) > lit(1))
  }
  /**
   * Returns rows of this data frame which violate uniqueness for specified columns cols.
   * The result data frame possesses an additional count column countColname.
   *
   * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
   * @return subdataframe of nlets
   */
  def getNonuniqueRows(cols: Seq[String] = schema.columns): GenericDataFrame = {
    import helper._
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
  def movePartitionColsLast(partitions: Seq[String])(implicit helper: DataFrameSubFeedCompanion): GenericDataFrame = {
    import helper._
    val (partitionCols, nonPartitionCols) = schema.columns.partition(c => partitions.contains(c))
    val newColOrder = nonPartitionCols ++ partitionCols
    select(newColOrder.map(col))
  }

  /**
   * Convert column names to lower case
   */
  def colNamesLowercase(implicit helper: DataFrameSubFeedCompanion): GenericDataFrame = {
    import helper._
    select(schema.columns.map(c => col(c).as(c.toLowerCase())))
  }

  /**
   * symmetric difference of two data frames: (df∪df2)∖(df∩df2) = (df∖df2)∪(df2∖df)
   *
   * @param other         : data frame to compare with
   * @param diffColName : name of boolean column which indicates whether the row belongs to df
   * @return data frame
   */
  def symmetricDifference(other: GenericDataFrame, diffColName: String = "_in_first_df"): GenericDataFrame = {
    require(schema.columns.map(_.toLowerCase).toSet == other.schema.columns.map(_.toLowerCase).toSet, "DataFrames must have the same columns for symmetricDifference calculation")
    // reorder columns according to the original df for calculating symmetricDifference
    val colOrder = schema.columns.map(helper.col)
    val dfOtherPrep = other.select(colOrder)
    this.except(dfOtherPrep).withColumn(diffColName, helper.lit(true))
      .unionByName(dfOtherPrep.except(this).withColumn(diffColName, helper.lit(false)))
  }

  /**
   * compares df with df2
   *
   * @param other : data frame to comapre with
   * @return true if both data frames have the same cardinality, schema and an empty symmetric difference
   */
  def isEqual(other: GenericDataFrame): Boolean = {
    // As a set-theoretic function symmetricDifference ignores multiple occurences of the same row.
    // Thus we need also to compare the cardinalities and the schemata of the two data frames.
    // For the schema, the order of columns doesn't need to match.
    // Note that we ignore the nullability of the columns to compare schemata.
    isSchemaEqualIgnoreNullabilty(other) && symmetricDifference(other).isEmpty && other.count == other.count
  }

  def isSchemaEqualIgnoreNullabilty(other: GenericDataFrame): Boolean = {
    SchemaUtil.schemaDiff(this.schema, other.schema, ignoreNullable = true).isEmpty && SchemaUtil.schemaDiff(other.schema, this.schema, ignoreNullable = true).isEmpty
  }

  def getDataFrameSubFeed(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed

}
trait GenericGroupedDataFrame {
  def subFeedType: Type
  def agg(columns: Seq[GenericColumn]): GenericDataFrame
}

trait GenericSchema {
  def subFeedType: Type
  def convertIfNeeded(toSubFeedType: Type): GenericSchema = {
    if (this.subFeedType != toSubFeedType) SchemaConverter.convert(this, toSubFeedType)
    else this
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
trait GenericColumn {
  def subFeedType: Type
  def ===(other: GenericColumn): GenericColumn
  def >(other: GenericColumn): GenericColumn
  def <(other: GenericColumn): GenericColumn
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
trait GenericField {
  def subFeedType: Type
  def name: String
  def dataType: GenericDataType
  def nullable: Boolean
  def makeNullable: GenericField
  def toLowerCase: GenericField
  def removeMetadata: GenericField
}
trait GenericDataType {
  def subFeedType: Type
  def convertIfNeeded(toSubFeedType: Type): GenericDataType = {
    if (this.subFeedType != toSubFeedType) SchemaConverter.convertDataType(this, toSubFeedType)
    else this
  }
  def isSortable: Boolean
  def typeName: String
  def sql: String
  def makeNullable: GenericDataType
  def toLowerCase: GenericDataType
  def removeMetadata: GenericDataType
}
trait GenericStructDataType { this: GenericDataType =>
  def withOtherFields[T](other: GenericStructDataType, func: (Seq[GenericField],Seq[GenericField]) => T): T
}
trait GenericArrayDataType { this: GenericDataType =>
  def withOtherElementType[T](other: GenericArrayDataType, func: (GenericDataType,GenericDataType) => T): T
  def containsNull: Boolean // Indicates array might contain null entries
}
trait GenericMapDataType { this: GenericDataType =>
  def withOtherKeyType[T](other: GenericMapDataType, func: (GenericDataType,GenericDataType) => T): T
  def withOtherValueType[T](other: GenericMapDataType, func: (GenericDataType,GenericDataType) => T): T
  def valueContainsNull: Boolean // Indicates if map values might be set to null
}
trait GenericRow {
  def subFeedType: Type
  def get(index: Int): Any
  def getAs[T](index: Int): T
}

// TODO
/*
object DataFrameSubFeedHelper {
  // search all classes implementing DataFrameSubFeedHelper
  private lazy val helpersCache = mutable.Map[Type, DataFrameSubFeedHelper]()

  /**
   * Get the DataFrameSubFeedHelper for a given SubFeed type
   */
  def apply(subFeedType: Type): DataFrameSubFeedHelper = {
    helpersCache.getOrElseUpdate(subFeedType, {
      val mirror = scala.reflect.runtime.currentMirror
      try {
        val module = mirror.staticModule(subFeedType.typeSymbol.name.toString)
        mirror.reflectModule(module).instance.asInstanceOf[DataFrameSubFeedHelper]
      } catch {
        case e: Exception => throw new IllegalStateException(s"No DataFrameSubFeedHelper implementation found for SubFeed type ${subFeedType.typeSymbol.name}", e)
      }
    })
  }
}
*/

trait SchemaConverter {
  def fromSubFeedType: Type
  def toSubFeedType: Type
  def convert(schema: GenericSchema): GenericSchema
  def convertDataType(dataType: GenericDataType): GenericDataType
}
object SchemaConverter {
  private val converters: mutable.Map[(Type,Type), SchemaConverter] = mutable.Map()
  def registerConverter(converter: SchemaConverter): Unit = {
    converters.put(
      (converter.fromSubFeedType,converter.toSubFeedType),
      converter
    )
  }
  def convert(schema: GenericSchema, toSubFeedType: Type): GenericSchema = {
    val converter = converters.get(schema.subFeedType, toSubFeedType)
      .getOrElse(throw new IllegalStateException(s"No schema converter found from ${schema.subFeedType.typeSymbol.name} to ${toSubFeedType.typeSymbol.name}"))
    converter.convert(schema)
  }
  def convertDataType(dataType: GenericDataType, toSubFeedType: Type): GenericDataType = {
    val converter = converters.get(dataType.subFeedType, toSubFeedType)
      .getOrElse(throw new IllegalStateException(s"No SchemaConverter found from ${dataType.subFeedType.typeSymbol.name} to ${toSubFeedType.typeSymbol.name}"))
    converter.convertDataType(dataType)
  }
}
