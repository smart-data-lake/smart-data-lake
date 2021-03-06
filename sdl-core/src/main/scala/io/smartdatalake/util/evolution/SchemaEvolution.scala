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
package io.smartdatalake.util.evolution

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try


/**
  * Functions for schema evolution
  */
private[smartdatalake] object SchemaEvolution extends SmartDataLakeLogger {

  /**
    * Converts column names to lowercase
    *
    * @param df
    * @return
    */
  def schemaColNames(df: DataFrame): Seq[String] = {
    df.columns
  }

  def newColumns(left: DataFrame, right: DataFrame): Seq[String] = {
    schemaColNames(right).diff(schemaColNames(left))
  }

  def deletedColumns(left: DataFrame, right: DataFrame): Seq[String] = {
    schemaColNames(left).diff(schemaColNames(right))
  }

  /**
    * Sorts all columns of a [[DataFrame]] according to defined sort order
    *
    * @param df
    * @param cols
    * @return
    */
  def sortColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    val dfCols = df.columns
    val colsToSelect = cols.filter( c => dfCols.contains(c)).map(col)
    df.select(colsToSelect: _*)
  }

  def getFieldTuples(dataFrame: DataFrame): Set[(String, String)] = {
    getFieldTuples(dataFrame.schema)
  }
  def getFieldTuples(schema: StructType): Set[(String, String)] = {
    schema.fields.map(f => (f.name, f.dataType.simpleString)).toSet
  }

  /**
    * Verifies that two [[DataFrame]]s contain the same columns.
    *
    * @param oldDf
    * @param newDf
    * @return
    */
  def hasSameColNamesAndTypes(oldDf: DataFrame, newDf: DataFrame): Boolean = {
    hasSameColNamesAndTypes(oldDf.schema, newDf.schema)
  }
  def hasSameColNamesAndTypes(oldSchema: StructType, newSchema: StructType): Boolean = {
    getFieldTuples(oldSchema) == getFieldTuples(newSchema)
  }

  /**
   * Checks if a DataType is castable to another
   */
  def isSimpleTypeCastable(left: DataType, right: DataType): Boolean = {
    Try(ValueProjector.getSimpleTypeConverter(left, right, Seq())).isSuccess
  }

  /**
   * Converts a col from one DataType to another
   *
   * The following conversion of data types are supported:
   * - numeric type (int, double, float, ...) to string
   * - char and boolean to string
   * - decimal with precision <= 7 to float
   * - decimal with precision <= 16 to double
   * - numerical type to to numerical type with higher precision, e.g int to long
   * - delete column in complex type (array, struct, map)
   * - new column in complex type (array, struct, map)
   * - changed data type in complex type (array, struct, map) according to the rules above
   *
   * @param column a Column
   * @param left original DataType
   * @param right new DataType
   * @return A column with the transformation expression applied
   */
  def convertDataType(column:Column, left: DataType, right: DataType, ignoreOldDeletedNestedColumns: Boolean): Option[(Column,Column,DataType)] = {
    (left,right) match {
      // simple type
      case (_, _) if isSimpleTypeCastable(left, right) =>
        Some(column.cast(right), column, right)
      // complex type
      case (_:StructType, _:StructType) | (_:ArrayType, _:ArrayType) | (_:MapType, _:MapType) =>
        val tgtType = ComplexTypeEvolution.consolidateType(left,right, ignoreOldDeletedNestedColumns)
        val udf_convertLeft = ComplexTypeEvolution.schemaEvolutionUdf(left, tgtType)
        val udf_convertRight = ComplexTypeEvolution.schemaEvolutionUdf(right, tgtType)
        Some(udf_convertLeft(column), udf_convertRight(column), tgtType)
      // default
      case _ => None
    }
  }

  /**
   * Checks if a schema evolution is necessary and if yes creates the evolved [[DataFrame]]s.
   *
   * The following schema changes are supported
   * - Deleted columns: newDf contains less columns than oldDf and the remaining are identical
   * - New columns: newDf contains additional columns, all other columns are the same as in in oldDf
   * - Renamed columns: this is a combination of a deleted column and a new column
   * - Changed data type: see method [[convertDataType]] for allowed changes of data type. In case of unsupported changes
   *   of data types an [[SchemaEvolutionException]] is thrown
   *
   * @param oldDf [[DataFrame]] with old data
   * @param newDf [[DataFrame]] with new data with potential changes in schema
   * @param colsToIgnore technical columns to be ignored in oldDf (e.g TechnicalTableColumn.captured and TechnicalTableColumn.delimited for historization)
   * @param ignoreOldDeletedColumns if true, remove no longer existing columns in result DataFrame's
   * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns in result DataFrame's. Keeping deleted
   *                                      columns in complex data types has performance impact as all new data in the future
   *                                      has to be converted by a complex function.
   * @return tuple of (newDf,oldDf) evolved to new schema
   */
  def process(oldDf: DataFrame, newDf: DataFrame, colsToIgnore: Seq[String] = Seq(), ignoreOldDeletedColumns: Boolean = false, ignoreOldDeletedNestedColumns: Boolean = true): (DataFrame, DataFrame) = {
    // internal structure and functions
    case class ColumnDetail(name: String, oldToNewColumn: Option[Column], newColumn: Option[Column], infoMsg: Option[String], errMsg: Option[String] )
    def getNullColumnOfType(d: DataType) = lit(null).cast(d)

    // log entry point
    logger.debug(s"old schema: ${oldDf.schema.treeString}")
    logger.debug(s"new schema: ${newDf.schema.treeString}")

    val oldColsWithoutTechCols = oldDf.columns.filter(c => !colsToIgnore.contains(c)).toSeq
    val newColsWithoutTechCols = newDf.columns.filter(c => !colsToIgnore.contains(c)).toSeq

    // check if schema is identical
    if (hasSameColNamesAndTypes(oldDf.select(oldColsWithoutTechCols.map(col): _*), newDf.select(newColsWithoutTechCols.map(col): _*))) {
      // check column order
      if (oldColsWithoutTechCols == newColsWithoutTechCols) {
        logger.info("Schemas are identical: no evolution needed")
        (oldDf, newDf)
      } else {
        logger.info("Schemas are identical but column order differs: columns of newDf are sorted according to oldDf")
        val newSchemaOnlyCols = newDf.columns.diff(oldColsWithoutTechCols)
        (oldDf, newDf.select((oldColsWithoutTechCols ++ newSchemaOnlyCols).map(col): _*))
      }
    } else {

      // prepare target column names
      // this defines the ordering of the resulting DataFrame's
      val tgtCols = if (Environment.schemaEvolutionNewColumnsLast) {
        // new columns last
        oldColsWithoutTechCols ++ newColumns(oldDf, newDf) ++ colsToIgnore
      } else {
        // deleted columns last
        newColsWithoutTechCols ++ deletedColumns(oldDf, newDf) ++ colsToIgnore
      }

      // create mapping
      val tgtColumns = tgtCols.map {
        c =>
          val oldType = oldDf.schema.fields.find(_.name == c).map(_.dataType)
          val newType = newDf.schema.fields.find(_.name == c).map(_.dataType)
          val thisColumn = Some(col(c))
          // define conversion
          val (oldToNewColumn, newColumn, infoMsg, errMsg) = (oldType,newType) match {
            // column is new -> fill in old data with null
            case (None,Some(n)) =>
              val nullColumn = Some(getNullColumnOfType(n).as(c))
              val info = Some(s"column $c is new")
              (nullColumn, thisColumn, info, None)
            // column is old -> fill in new data with null
            case (Some(o),None) =>
              val (oldToNewColumn,newColumn,info) = if (colsToIgnore.contains(c)) (thisColumn, None, Some(s"column $c is ignored because it is in the list of columns to ignore"))
              else if (ignoreOldDeletedColumns) (None, None, Some(s"column $c is old and will be removed because ignoreOldDeletedColumns=true"))
              else (thisColumn, Some(getNullColumnOfType(o).as(c)), Some(s"column $c is old and will be set to null for new records"))
              (oldToNewColumn, newColumn, info, None)
            // datatypes are *not* equal -> conversion of old to new datatype required
            case (Some(o),Some(n)) if o.simpleString != n.simpleString =>
              val convertedColumns = convertDataType(col(c), o, n, ignoreOldDeletedNestedColumns)
              val info = if (convertedColumns.isDefined) Some(s"column $c is converted from ${o.simpleString}/${n.simpleString} to ${convertedColumns.get._3.simpleString}") else None
              val err = if (convertedColumns.isEmpty) Some(s"column $c cannot be converted from ${o.simpleString} to ${n.simpleString}") else None
              (convertedColumns.map(_._1.as(c)), convertedColumns.map(_._2.as(c)), info, err)
            // datatypes are equal -> no conversion required
            case (Some(o),Some(n)) => (thisColumn,thisColumn,None,None)
          }
          ColumnDetail(c, oldToNewColumn, newColumn, infoMsg, errMsg)
      }

      // stop on errors
      if (tgtColumns.exists(_.errMsg.isDefined)) {
        val errList = tgtColumns.flatMap(_.errMsg).mkString(", ")
        throw SchemaEvolutionException(s"Data types are different: $errList")
      }

      // log information
      val infoList = tgtColumns.flatMap(_.infoMsg).mkString("\n\t")
      logger.info(s"schema evolution needed. mapping is: \n\t$infoList")
      logger.info(s"old schema: ${oldDf.schema.treeString}")
      logger.info(s"new schema: ${newDf.schema.treeString}")

      // prepare dataframes
      val oldExtendedDf = oldDf.select(tgtColumns.flatMap(_.oldToNewColumn):_*)
      val newExtendedDf = newDf.select(tgtColumns.flatMap(_.newColumn):_*)

      // return
      (oldExtendedDf, newExtendedDf)
    }
  }
}
