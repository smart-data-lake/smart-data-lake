/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
import io.smartdatalake.util.misc.SchemaUtil.{colListDiff, findByName, isColListEqual, normalizeColName}
import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger, StringUtil}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe._

/**
 * Result of converting a column from one DataType to another, see [[SchemaEvolution.convertDataType]].
 *
 * @param oldColumn expression converting the column of the old DataFrame to the target DataType
 * @param newColumn expression converting the column of the new DataFrame to the target DataType
 * @param dataType target DataType
 */
case class ColumnConversion(oldColumn: GenericColumn, newColumn: GenericColumn, dataType: GenericDataType)

/**
 * Describes how a column of the evolved schema is created from the old and the new DataFrame,
 * see [[SchemaEvolution.createColumnMappings]].
 *
 * @param name name of the column in the evolved schema
 * @param oldColumn expression to create the column from the old DataFrame, or None if it is not selected from the old DataFrame
 * @param newColumn expression to create the column from the new DataFrame, or None if it is not selected from the new DataFrame
 * @param info description of the evolution applied to the column, or None if the column is unchanged
 */
case class ColumnMapping(name: String, oldColumn: Option[GenericColumn], newColumn: Option[GenericColumn], info: Option[String])

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
  @deprecated("not used by SDLB anymore, use GenericDataFrame.select instead", "3.0.0")
  def sortColumns(df: GenericDataFrame, cols: Seq[String], caseSensitive: Boolean = Environment.caseSensitive): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    val dfCols = df.columns.map(normalizeColName(_, caseSensitive)).toSet
    df.select(cols.filter(c => dfCols.contains(normalizeColName(c, caseSensitive))).map(functions.col))
  }

  /**
   * Verifies that two DataFrames contain the same columns.
   */
  def hasSameColNamesAndTypes(oldDf: GenericDataFrame, newDf: GenericDataFrame, caseSensitiveComparison: Boolean = Environment.caseSensitive): Boolean = {
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
   * @param caseSensitive if true, names of nested fields are compared case-sensitive
   * @return the expressions to convert the old and the new column to the target DataType, or None if the conversion is not supported
   */
  def convertDataType(column: GenericColumn, left: GenericDataType, right: GenericDataType, ignoreOldDeletedNestedColumns: Boolean, caseSensitive: Boolean = Environment.caseSensitive): Option[ColumnConversion] = {
    val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(column.subFeedType)
    (left, right) match {
      // simple type
      case (_: GenericSimpleDataType, _: GenericSimpleDataType) =>
        Some(ColumnConversion(column.cast(right), column.cast(right), right))
      // same complex type
      case _ if left.typeName == right.typeName =>
        val tgtType = TypeConsolidation.consolidateType(left, right, ignoreOldDeletedNestedColumns, caseSensitive = caseSensitive)
        val convertLeftUdf = functions.schemaEvolutionUdf(left, tgtType)
        val convertRightUdf = functions.schemaEvolutionUdf(right, tgtType)
        Some(ColumnConversion(convertLeftUdf.convert(column), convertRightUdf.convert(column), tgtType))
      // default
      case _ => None
    }
  }

  /**
   * Creates the mapping of old and new columns to the columns of the evolved schema.
   * See [[process]] for the supported schema changes and the meaning of the parameters.
   *
   * @return one [[ColumnMapping]] per column of the evolved schema, in the order of the evolved schema.
   * @throws SchemaEvolutionException if a data type change is not supported
   */
  def createColumnMappings(oldSchema: GenericSchema, newSchema: GenericSchema, colsToIgnore: Seq[String] = Seq(), ignoreOldDeletedColumns: Boolean = false, ignoreOldDeletedNestedColumns: Boolean = true, caseSensitiveComparison: Boolean = Environment.caseSensitive): Seq[ColumnMapping] = {
    require(oldSchema.subFeedType == newSchema.subFeedType, s"subFeedType of old and new schema must be the same, got ${oldSchema.subFeedType} and ${newSchema.subFeedType}")
    val functions = DataFrameSubFeed.getFunctions(oldSchema.subFeedType)
    import functions._
    def norm(name: String) = normalizeColName(name, caseSensitiveComparison)

    val colsToIgnoreSet = colsToIgnore.map(norm).toSet
    def isColToIgnore(name: String) = colsToIgnoreSet.contains(norm(name))
    val oldFields = oldSchema.fields.filterNot(f => isColToIgnore(f.name))
    val newFields = newSchema.fields.filterNot(f => isColToIgnore(f.name))
    val oldFieldsMap = oldSchema.fields.map(f => norm(f.name) -> f).toMap
    val newFieldsMap = newSchema.fields.map(f => norm(f.name) -> f).toMap

    // prepare target column names. This defines the ordering of the resulting DataFrame's.
    // Columns to ignore are placed at the end, if they exist in one of the schemas.
    val existingColsToIgnore = colsToIgnore.flatMap(c => oldFieldsMap.get(norm(c)).orElse(newFieldsMap.get(norm(c)))).map(_.name).distinct
    val tgtCols = if (Environment.schemaEvolutionNewColumnsLast) {
      oldFields.map(_.name) ++ colListDiff(newFields.map(_.name), oldFields.map(_.name), caseSensitiveComparison) ++ existingColsToIgnore
    } else {
      newFields.map(_.name) ++ colListDiff(oldFields.map(_.name), newFields.map(_.name), caseSensitiveComparison) ++ existingColsToIgnore
    }

    // select a column with its name in the source schema, and rename it if the target name is spelled differently.
    def colAs(srcName: String, tgtName: String) = if (srcName == tgtName) col(srcName) else col(srcName).as(tgtName)

    // create mapping
    val mappingsOrErrors = tgtCols.map { c =>
      (oldFieldsMap.get(norm(c)), newFieldsMap.get(norm(c))) match {
        // column is new -> fill in old data with null
        case (None, Some(n)) =>
          Right(ColumnMapping(c, Some(lit(null).cast(n.dataType).as(c)), Some(colAs(n.name, c)), Some(s"column $c is new")))
        // column is old -> fill in new data with null
        case (Some(o), None) =>
          if (isColToIgnore(c)) Right(ColumnMapping(c, Some(colAs(o.name, c)), None, Some(s"column $c is ignored because it is in the list of columns to ignore")))
          else if (ignoreOldDeletedColumns) Right(ColumnMapping(c, None, None, Some(s"column $c is old and will be removed because ignoreOldDeletedColumns=true")))
          else Right(ColumnMapping(c, Some(colAs(o.name, c)), Some(lit(null).cast(o.dataType).as(c)), Some(s"column $c is old and will be set to null for new records")))
        // datatypes are *not* equal -> conversion of old to new datatype required
        case (Some(o), Some(n)) if !hasSameColNamesAndTypes(Seq(o), Seq(n), caseSensitiveComparison) =>
          convertDataType(col(o.name), o.dataType, n.dataType, ignoreOldDeletedNestedColumns, caseSensitiveComparison) match {
            case Some(conversion) =>
              // the column has the same name in both DataFrames except for case (if case-insensitive), so it's ok to use the same expression for both.
              Right(ColumnMapping(c, Some(conversion.oldColumn.as(c)), Some(conversion.newColumn.as(c)),
                Some(s"column $c is converted from ${o.dataType.typeName}/${n.dataType.typeName} to ${conversion.dataType.typeName}")))
            case None => Left(s"column $c cannot be converted from ${o.dataType.typeName} to ${n.dataType.typeName}")
          }
        // datatypes are equal -> no conversion required
        case (Some(o), Some(n)) =>
          Right(ColumnMapping(c, Some(colAs(o.name, c)), Some(colAs(n.name, c)), None))
        case (None, None) => throw new IllegalStateException(s"column $c must exist in old or new schema")
      }
    }

    // stop on errors
    val errors = mappingsOrErrors.collect { case Left(err) => err }
    if (errors.nonEmpty) throw SchemaEvolutionException(s"Data types are different: ${errors.mkString(", ")}")
    mappingsOrErrors.collect { case Right(mapping) => mapping }
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
    require(oldDf.subFeedType == newDf.subFeedType, s"subFeedType of old and new DataFrame must be the same, got ${oldDf.subFeedType} and ${newDf.subFeedType}")
    val functions = DataFrameSubFeed.getFunctions(oldDf.subFeedType)
    val oldSchema = oldDf.schema
    val newSchema = newDf.schema

    // log entry point
    logger.debug(s"old schema: ${oldSchema.treeString()}")
    logger.debug(s"new schema: ${newSchema.treeString()}")

    val colsToIgnoreSet = colsToIgnore.map(normalizeColName(_, caseSensitiveComparison)).toSet
    def isColToIgnore(name: String) = colsToIgnoreSet.contains(normalizeColName(name, caseSensitiveComparison))
    val oldFieldsWithoutTechCols = oldSchema.fields.filterNot(f => isColToIgnore(f.name))
    val newFieldsWithoutTechCols = newSchema.fields.filterNot(f => isColToIgnore(f.name))

    // check if schema is identical
    if (hasSameColNamesAndTypes(oldFieldsWithoutTechCols, newFieldsWithoutTechCols, caseSensitiveComparison)) {
      val oldColsWithoutTechCols = oldFieldsWithoutTechCols.map(_.name)
      // check column order
      if (isColListEqual(oldColsWithoutTechCols, newFieldsWithoutTechCols.map(_.name), caseSensitiveComparison)) {
        logger.info("Schemas are identical: no evolution needed")
        (oldDf, newDf)
      } else {
        logger.info("Schemas are identical but column order differs: columns of newDf are sorted according to oldDf")
        val newSchemaOnlyCols = colListDiff(newDf.columns, oldColsWithoutTechCols, caseSensitiveComparison)
        (oldDf, newDf.select((oldColsWithoutTechCols ++ newSchemaOnlyCols).map(functions.col)))
      }
    } else {
      val mappings = createColumnMappings(oldSchema, newSchema, colsToIgnore, ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns, caseSensitiveComparison)

      // log information
      val infoList = mappings.flatMap(_.info).map("-> " + _).mkString("\n")
      val infoTxt = StringUtil.indent(s"$infoList\nold schema:\n${oldSchema.treeString().stripTrailing()}\nnew schema:\n${newSchema.treeString().stripTrailing()}", 2)
      logger.info(s"schema evolution needed. mapping is:\n$infoTxt")

      // prepare dataframes
      val oldExtendedDf = oldDf.select(mappings.flatMap(_.oldColumn))
      val newExtendedDf = newDf.select(mappings.flatMap(_.newColumn))
      (oldExtendedDf, newExtendedDf)
    }
  }

  @deprecated("use StringUtil.indent instead", "3.0.0")
  def indent(s: String, spaces: Int): String = StringUtil.indent(s, spaces)

  @deprecated("use SchemaUtil.isColListEqual instead", "3.0.0")
  def isStringListEqual(a: Seq[String], b: Seq[String], caseSensitiveComparison: Boolean): Boolean = isColListEqual(a, b, caseSensitiveComparison)

  @deprecated("use SchemaUtil.colListDiff instead", "3.0.0")
  def stringListDiff(a: Seq[String], b: Seq[String], caseSensitiveComparison: Boolean): Seq[String] = colListDiff(a, b, caseSensitiveComparison)

  @deprecated("use SchemaUtil.findByName instead", "3.0.0")
  def listFind[A](a: Seq[A], str: String, extractor: A => String, caseSensitiveComparison: Boolean): Option[A] = findByName(a, str, extractor, caseSensitiveComparison)

}
