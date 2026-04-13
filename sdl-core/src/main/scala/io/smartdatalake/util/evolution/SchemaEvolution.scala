/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.util.evolution

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe._


/**
  * Functions for schema evolution
  */
object SchemaEvolution extends SmartDataLakeLogger {

  def newColumns(left: GenericDataFrame, right: GenericDataFrame, caseSensitive: Boolean = Environment.caseSensitive): Seq[String] = {
    SchemaUtil.checkMissingCols(right.columns, left.columns, caseSensitive)
  }

  def deletedColumns(left: GenericDataFrame, right: GenericDataFrame, caseSensitive: Boolean = Environment.caseSensitive): Seq[String] = {
    SchemaUtil.checkMissingCols(left.columns, right.columns, caseSensitive)
  }

  /**
   * Sorts all columns of a DataFrame according to defined sort order
   */
  def sortColumns(df: GenericDataFrame, cols: Seq[String], caseSensitive: Boolean = false): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    val dfCols = if (caseSensitive) df.columns else df.columns.map(_.toLowerCase)
    val colsToSelect = if (caseSensitive) cols.filter(c => dfCols.contains(c)).map(functions.col) else cols.filter(c => dfCols.contains(c.toLowerCase)).map(functions.col)
    df.select(colsToSelect)
  }

  /**
   * Verifies that two DataFrames contain the same columns.
   */
  def hasSameColNamesAndTypes(oldDf: GenericDataFrame, newDf: GenericDataFrame, caseSensitiveComparison: Boolean = false): Boolean = {
    hasSameColNamesAndTypes(oldDf.schema, newDf.schema, caseSensitiveComparison)
  }

  def hasSameColNamesAndTypes(oldSchema: GenericSchema, newSchema: GenericSchema, caseSensitiveComparison: Boolean): Boolean = {
    hasSameColNamesAndTypes(oldSchema.fields, newSchema.fields, caseSensitiveComparison)
  }

  def hasSameColNamesAndTypes(oldSchema: Seq[GenericField], newSchema: Seq[GenericField], caseSensitiveComparison: Boolean): Boolean = {
    val (diff1, diff2) = SchemaUtil.schemaDiff2(oldSchema, newSchema, ignoreNullable = true, caseSensitive = caseSensitiveComparison)
    diff1.isEmpty && diff2.isEmpty
  }

  /**
   * Converts a col from one DataType to another
   *
   * The following conversion of data types are supported:
   * - simple type to compatible simple type
   * - delete column in complex type (array, struct, map)
   * - new column in complex type (array, struct, map)
   * - changed data type in complex type (array, struct, map) according to the rules above
   *
   * @param column a Column
   * @param left original DataType
   * @param right new DataType
   * @return A column with the transformation expression applied
   */
  def convertDataType(column: GenericColumn, left: GenericDataType, right: GenericDataType, ignoreOldDeletedNestedColumns: Boolean): Option[(GenericColumn, GenericColumn, GenericDataType)] = {
    val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(column.subFeedType)
    (left,right) match {
      // simple type
      case (left: GenericDataType with GenericSimpleDataType, right: GenericDataType with GenericSimpleDataType) =>
        Some(column.cast(right), column.cast(right), right)
      // same complex type
      case (left: GenericDataType, right: GenericDataType) if left.typeName == right.typeName =>
        val tgtType = TypeConsolidation.consolidateType(left, right, ignoreOldDeletedNestedColumns)
        val convertLeftUdf = functions.schemaEvolutionUdf(left, tgtType)
        val convertRightUdf = functions.schemaEvolutionUdf(right, tgtType)
        Some(convertLeftUdf.convert(column), convertRightUdf.convert(column), tgtType)
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
   *   of data types a [[SchemaEvolutionException]] is thrown
   *
   * @param oldDf [[DataFrame]] with old data
   * @param newDf [[DataFrame]] with new data with potential changes in schema
   * @param colsToIgnore technical columns to be ignored in oldDf (e.g Environment.capturedColumnName and Environment.delimitedColumnName for historization)
   * @param ignoreOldDeletedColumns if true, remove no longer existing columns in result DataFrame's
   * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns in result DataFrame's. Keeping deleted
   *                                      columns in complex data types has performance impact as all new data in the future
   *                                      has to be converted by a complex function.
   * @param caseSensitiveComparison if true, all column names are handled case sensitive
   * @return tuple of (oldExtendedDf, newExtendedDf) evolved to new schema
   */
  def process(oldDf: GenericDataFrame, newDf: GenericDataFrame, colsToIgnore: Seq[String] = Seq(), ignoreOldDeletedColumns: Boolean = false, ignoreOldDeletedNestedColumns: Boolean = true, caseSensitiveComparison: Boolean = Environment.caseSensitive): (GenericDataFrame, GenericDataFrame) = {
    assert(oldDf.subFeedType == newDf.subFeedType)
    val functions = DataFrameSubFeed.getFunctions(oldDf.subFeedType)
    import functions._

    // internal structure and functions
    case class ColumnDetail(name: String, oldToNewColumn: Option[GenericColumn], newColumn: Option[GenericColumn], infoMsg: Option[String], errMsg: Option[String])

    def getNullColumnOfType(d: GenericDataType) = lit(null).cast(d)

    // log entry point
    logger.debug(s"old schema: ${oldDf.schema.treeString()}")
    logger.debug(s"new schema: ${newDf.schema.treeString()}")

    val oldColsWithoutTechCols = if (caseSensitiveComparison) {
      oldDf.columns.filter(c => !colsToIgnore.contains(c)).toSeq
    } else {
      oldDf.columns.filter(c => !colsToIgnore.map(_.toLowerCase).contains(c.toLowerCase)).toSeq
    }

    val newColsWithoutTechCols = if (caseSensitiveComparison) {
      newDf.columns.filter(c => !colsToIgnore.contains(c)).toSeq
    } else {
      newDf.columns.filter(c => !colsToIgnore.map(_.toLowerCase).contains(c.toLowerCase)).toSeq
    }

    // check if schema is identical
    if (hasSameColNamesAndTypes(oldDf.select(oldColsWithoutTechCols.map(col)), newDf.select(newColsWithoutTechCols.map(col)), caseSensitiveComparison)) {
      // check column order
      if (isStringListEqual(oldColsWithoutTechCols, newColsWithoutTechCols, caseSensitiveComparison)) {
        logger.info("Schemas are identical: no evolution needed")
        (oldDf, newDf)
      } else {
        logger.info("Schemas are identical but column order differs: columns of newDf are sorted according to oldDf")
        val newSchemaOnlyCols = stringListDiff(newDf.columns, oldColsWithoutTechCols, caseSensitiveComparison)
        (oldDf, newDf.select((oldColsWithoutTechCols ++ newSchemaOnlyCols).map(col)))
      }
    } else {

      // prepare target column names
      // this defines the ordering of the resulting DataFrame's
      val tgtCols = if (Environment.schemaEvolutionNewColumnsLast) {
        // new columns last
        oldColsWithoutTechCols ++ newColumns(oldDf, newDf) ++ (if (caseSensitiveComparison) colsToIgnore else colsToIgnore.map(_.toLowerCase))
      } else {
        // deleted columns last
        newColsWithoutTechCols ++ deletedColumns(oldDf, newDf) ++ (if (caseSensitiveComparison) colsToIgnore else colsToIgnore.map(_.toLowerCase))
      }

      // create mapping
      val tgtColumns = tgtCols.map {
        c =>
          val oldType = if (caseSensitiveComparison) oldDf.schema.fields.find(_.name == c).map(_.dataType) else oldDf.schema.fields.find(_.name.toLowerCase == c.toLowerCase).map(_.dataType)
          val newType = if (caseSensitiveComparison) newDf.schema.fields.find(_.name == c).map(_.dataType) else newDf.schema.fields.find(_.name.toLowerCase == c.toLowerCase).map(_.dataType)
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
              val (oldToNewColumn,newColumn,info) = if (colsToIgnore.contains(if(caseSensitiveComparison) c else c.toLowerCase)) (thisColumn, None, Some(s"column $c is ignored because it is in the list of columns to ignore"))
              else if (ignoreOldDeletedColumns) (None, None, Some(s"column $c is old and will be removed because ignoreOldDeletedColumns=true"))
              else (thisColumn, Some(getNullColumnOfType(o).as(c)), Some(s"column $c is old and will be set to null for new records"))
              (oldToNewColumn, newColumn, info, None)
            // datatypes are *not* equal -> conversion of old to new datatype required
            case (Some(o), Some(n)) if !hasSameColNamesAndTypes(Seq(functions.field(c, o, true)), Seq(functions.field(c, n, true)), caseSensitiveComparison) =>
              val convertedColumns = convertDataType(col(if(caseSensitiveComparison) c else c.toLowerCase), o, n, ignoreOldDeletedNestedColumns)
              val info = if (convertedColumns.isDefined) Some(s"column $c is converted from ${o.typeName}/${n.typeName} to ${convertedColumns.get._3.typeName}") else None
              val err = if (convertedColumns.isEmpty) Some(s"column $c cannot be converted from ${o.typeName} to ${n.typeName}") else None
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
      val infoList = tgtColumns.flatMap(_.infoMsg).map("-> " + _).mkString("\n")
      val infoTxt = indent(s"$infoList\nold schema:\n${oldDf.schema.treeString().stripTrailing()}\nnew schema:\n${newDf.schema.treeString().stripTrailing()}", 2)
      logger.info(s"schema evolution needed. mapping is:\n$infoTxt"
      )

      // prepare dataframes
      val oldExtendedDf = oldDf.select(tgtColumns.flatMap(_.oldToNewColumn))
      val newExtendedDf = newDf.select(tgtColumns.flatMap(_.newColumn))

      // return
      (oldExtendedDf, newExtendedDf)
    }
  }

  def indent(s: String, spaces: Int): String = {
    val pad = " " * spaces
    s.linesIterator.map("  " + _).mkString(System.lineSeparator())
  }

  def isStringListEqual(a: Seq[String], b: Seq[String], caseSensitiveComparison: Boolean): Boolean = {
    if (caseSensitiveComparison) a == b
    else a.map(_.toLowerCase) == b.map(_.toLowerCase)
  }

  def stringListDiff(a: Seq[String], b: Seq[String], caseSensitiveComparison: Boolean): Seq[String] = {
    if (caseSensitiveComparison) a.diff(b)
    else {
      val bLowerSet = b.map(_.toLowerCase).toSet
      a.filter(x => !bLowerSet.contains(x.toLowerCase))
    }
  }

  def listFind[A](a: Seq[A], str: String, extractor: A => String, caseSensitiveComparison: Boolean): Option[A] = {
    if (caseSensitiveComparison) a.find(e => extractor(e) == str)
    else a.find(e => extractor(e).equalsIgnoreCase(str))
  }

}
