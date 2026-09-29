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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.util.sqlglot.SqlGlotBridge
import io.smartdatalake.workflow.{ActionPipelineContext, SchemaViolationException}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.sql._
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.generic.TableSchemaChange
import org.json4s.{DefaultFormats, Formats}

import java.sql.{ResultSet, ResultSetMetaData, SQLException}
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * SQL engine implementation of reading and writing a [[JdbcTableDataObject]], see [[JdbcTableEngine]].
 *
 * It is used by Actions with the JdbcTableConnection of the DataObject as engine connection. Reading creates an
 * SQLGlot query, and writing executes it on the database with an `INSERT INTO ... SELECT` statement, or a merge
 * statement from a temporary table created with `CREATE TABLE ... AS SELECT`. So data is never transferred out of
 * the database.
 *
 * All input and output DataObjects of the Action must use the engine connection, this is validated on reading and
 * writing.
 */
class JdbcTableSqlEngine(dataObject: JdbcTableDataObject) extends JdbcTableEngine with SmartDataLakeLogger {
  import dataObject.{connection, id, table, tmpTable}

  override def subFeedType: Type = typeOf[SQLSubFeed]

  private implicit val formats: Formats = DefaultFormats

  private def bridge: SqlGlotBridge = SqlGlotBridge.get()

  private def validateEngineConnection(implicit context: ActionPipelineContext): Unit = {
    val engineConnection = SQLSubFeed.getEngineConnection
    if (!engineConnection.exists(_.id == connection.id)) throw ConfigurationException(
      s"($id) The SQL engine executes SQL statements on the database of its engine connection" +
        s" ${engineConnection.map(_.id.id).getOrElse("<none>")}, but this JdbcTableDataObject uses connection ${connection.id}." +
        " All inputs and outputs of an Action using the SQL engine must use its engine connection.")
  }

  private def sqlDataFrame(df: GenericDataFrame): SQLDataFrame = {
    val sqlDf = SQLDataFrame.of(df)
    if (sqlDf.connection.id != connection.id) throw ConfigurationException(
      s"($id) Can not write a DataFrame of connection ${sqlDf.connection.id} with the SQL engine to connection ${connection.id}")
    sqlDf
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): GenericDataFrame = {
    validateEngineConnection
    val schema = getExistingSqlSchema
      .getOrElse(throw new IllegalStateException(s"($id) Table ${table.fullName} does not exist"))
    val df = table.query.map(q => SQLDataFrame.query(connection, q, schema))
      .getOrElse(SQLDataFrame.table(connection, table.fullName, schema))
    applyIncrementalOutput(df)
  }

  private def applyIncrementalOutput(df: SQLDataFrame)(implicit context: ActionPipelineContext): SQLDataFrame = {
    import SQLSubFeed._
    dataObject.incrementalOutputState match {
      case Some((lastExpr, lastHighWatermark)) if context.isExecPhase =>
        val incrementalOutputExpr = dataObject.incrementalOutputExpr
          .getOrElse(throw new IllegalStateException(s"($id) incrementalOutputExpr must be set to use DataObjectStateIncrementalMode"))
        if (lastExpr != incrementalOutputExpr) logger.warn(s"($id) incrementalOutputState has different column as incrementalOutputExpr ($lastExpr != $incrementalOutputExpr")
        val dfHighWatermark = df.agg(Seq(max(expr(incrementalOutputExpr)).as("high_watermark")))
        val dataType = dfHighWatermark.schema.fields.head.dataType
        val newHighWatermarkValue = Option(dfHighWatermark.collect.head.get(0))
          .getOrElse(throw NoDataToProcessWarning(id.id, s"No data to process found for $id by DataObjectStateIncrementalMode."))
        dataObject.incrementalOutputState = Some((incrementalOutputExpr, Some((newHighWatermarkValue.toString, dataType.sql))))
        logger.info(s"($id) incremental output selected records with '$incrementalOutputExpr > '${lastHighWatermark.map(_._1).getOrElse("none")}'" +
          s" and <= '$newHighWatermarkValue'")
        var dfFiltered = df.filter(expr(incrementalOutputExpr) <= lit(newHighWatermarkValue.toString).cast(dataType))
        lastHighWatermark.foreach { case (value, lastDataType) =>
          if (value == newHighWatermarkValue.toString) {
            throw NoDataToProcessWarning(id.id, s"No data to process found for $id by DataObjectStateIncrementalMode. High watermark is $newHighWatermarkValue")
          }
          dfFiltered = dfFiltered.filter(expr(lastExpr) > lit(value).cast(SQLDataType.simple(lastDataType)))
        }
        dfFiltered
      case _ => df
    }
  }

  override def initDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = {
    validateEngineConnection
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df)
    validate(targetDf)
    if (dataObject.isTableExisting) {
      if (dataObject.allowSchemaEvolution) throw new NotImplementedError(s"($id) Schema evolution is not yet implemented for the SQL engine")
      validateColumnsOnWrite(targetDf)
    } else {
      // create an empty table with the schema of the DataFrame
      connection.execJdbcStatement(createTableAsStatement(sqlDataFrame(targetDf), table.fullName, withData = false))
      dataObject.resetCachedIsTableExisting()
      require(dataObject.isTableExisting, s"($id) Strangely table ${table.fullName} doesn't exist even though we tried to create it")
    }
  }

  private def validate(df: GenericDataFrame): Unit = {
    dataObject.validateSchemaMin(df.schema, "write")
    dataObject.validateSchemaHasPartitionCols(df.columns, "write")
    dataObject.validateSchemaHasPrimaryKeyCols(df.columns, "write")
  }

  // Note that data types are not validated, as the types inferred by SQLGlot are not exact.
  private def validateColumnsOnWrite(df: GenericDataFrame)(implicit context: ActionPipelineContext): Unit = {
    getExistingSqlSchema.foreach { schema =>
      val existingColumns = schema.columns.map(_.toLowerCase).toSet
      val missingColumns = df.columns.filterNot(c => existingColumns.contains(c.toLowerCase))
      if (missingColumns.nonEmpty) throw new SchemaViolationException(
        s"($id) Columns ${missingColumns.mkString(", ")} of the DataFrame do not exist in table ${table.fullName} (${schema.columns.mkString(", ")})")
    }
  }

  private def createTableAsStatement(df: SQLDataFrame, tableName: String, withData: Boolean): String =
    bridge.call("create_table_as", "df" -> df.id, "table" -> tableName, "dialect" -> connection.sqlGlotDialect, "with_data" -> withData).extract[String]

  private def insertStatement(df: SQLDataFrame)(implicit context: ActionPipelineContext): String = {
    val columns = df.columns
    s"INSERT INTO ${table.fullName} (${columns.map(dataObject.quoteCaseSensitiveColumn).mkString(", ")}) ${df.select(columns.map(c => SQLColumn.byName(c))).toDatabaseSql}"
  }

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    validateEngineConnection
    val targetDf = sqlDataFrame(saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df))
    validate(targetDf)
    if (!dataObject.allowSchemaEvolution) validateColumnsOnWrite(targetDf)
    saveModeOptions.map(_.saveMode).getOrElse(dataObject.saveMode) match {
      case SDLSaveMode.Overwrite =>
        val transaction = connection.beginTransaction()
        try {
          // replace existing data in one transaction
          if (partitionValues.nonEmpty) transaction.execJdbcStatement(dataObject.deletePartitionsStatement(partitionValues))
          else transaction.execJdbcStatement(dataObject.deleteAllDataStatement())
          val rowsInserted = transaction.execJdbcDmlStatement(insertStatement(targetDf))
          transaction.commit()
          Map("rows_inserted" -> rowsInserted)
        } catch {
          case e: SQLException =>
            transaction.rollback()
            throw e
        }
      case SDLSaveMode.Append =>
        Map("rows_inserted" -> connection.execJdbcDmlStatement(insertStatement(targetDf)))
      case SDLSaveMode.Merge =>
        mergeDataFrameByPrimaryKey(sqlDataFrame(df), saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
      case x => throw new IllegalStateException(s"($id) Unsupported saveMode $x")
    }
  }

  /**
   * Merges the DataFrame with existing table data by creating a temp-table with the data of the DataFrame, and
   * using an SQL merge statement. See also JdbcTableSparkClassicEngine.mergeDataFrameByPrimaryKey.
   */
  private def mergeDataFrameByPrimaryKey(df: SQLDataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): MetricsMap = {
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use SaveMode Merge")
    if (connection.catalog.isTableExisting(tmpTable.fullName)) {
      logger.error(s"($id) Temporary table ${tmpTable.fullName} already exists! There might be a potential conflict with another job. It will be dropped and recreated.")
      connection.dropTable(tmpTable.fullName)
    }
    try {
      connection.execJdbcStatement(createTableAsStatement(df, tmpTable.fullName, withData = true))
      SQLUtil.createUpdateExistingStatement(table, df.columns, tmpTable.fullName, saveModeOptions, dataObject.quoteCaseSensitiveColumn(_))
        .foreach(connection.execJdbcDmlStatement(_))
      val mergeStmt = SQLUtil.createMergeStatement(table, df.columns, tmpTable.fullName, saveModeOptions, dataObject.quoteCaseSensitiveColumn(_))
      Map("rows_affected" -> connection.execJdbcDmlStatement(mergeStmt))
    } finally {
      connection.dropTable(tmpTable.fullName)
    }
  }

  // cache response to avoid jdbc queries
  private var cachedExistingSchema: Option[SQLSchema] = None

  /**
   * Schema of the existing table from the JDBC metadata of an empty query, with the database types converted to SQLGlot types.
   */
  private def getExistingSqlSchema(implicit context: ActionPipelineContext): Option[SQLSchema] = {
    if (cachedExistingSchema.isEmpty && (table.query.isDefined || dataObject.isTableExisting)) {
      val query = table.query.map(q => s"SELECT * FROM ($q) q WHERE 1=0").getOrElse(s"SELECT * FROM ${table.fullName} WHERE 1=0")
      val columns = connection.execJdbcQuery(query, (rs: ResultSet) => {
        val metadata = rs.getMetaData
        (1 to metadata.getColumnCount).map(i => (metadata.getColumnName(i), metadata.getColumnTypeName(i), metadata.getPrecision(i),
          metadata.getScale(i), metadata.isNullable(i) != ResultSetMetaData.columnNoNulls))
      })
      val types = bridge.call("parse_types", "types" -> columns.map { case (_, tpe, precision, scale, _) => Seq(tpe, precision, scale) },
        "dialect" -> connection.sqlGlotDialect).children.map(SQLDataType.fromBridge)
      cachedExistingSchema = Some(SQLSchema(columns.zip(types).map { case ((name, _, _, _, nullable), tpe) => SQLField(name, tpe, nullable) }))
    }
    cachedExistingSchema
  }

  override def getExistingSchema(implicit context: ActionPipelineContext): Option[GenericSchema] = getExistingSqlSchema

  override def getCurrentSchema(implicit context: ActionPipelineContext): Option[GenericSchema] =
    getExistingSqlSchema.map(schema => schema.copy(fields = schema.fields.map(field =>
      dataObject.getJdbcColumn(field.name).flatMap(_.isNullable).map(nullable => field.copy(nullable = nullable)).getOrElse(field)
    )))

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    PartitionValues.fromDataFrame(getDataFrame(Seq()).select(dataObject.partitions.map(c => SQLColumn.byName(c))))
  }

  override def createTable(schema: GenericSchema)(implicit context: ActionPipelineContext): Unit = {
    val sqlSchema = schema.convert(subFeedType) match {
      case s: SQLSchema => s
      case s => throw new IllegalStateException(s"($id) can not create table from schema of type ${s.getClass.getSimpleName}")
    }
    val stmt = bridge.call("create_table", "table" -> table.fullName, "dialect" -> connection.sqlGlotDialect,
      "columns" -> sqlSchema.fields.map(f => Seq(f.name, f.dataType.sql, f.nullable))).extract[String]
    connection.execJdbcStatement(stmt)
  }

  override def applySchemaChanges(changes: Seq[TableSchemaChange])(implicit context: ActionPipelineContext): Unit =
    throw new NotImplementedError(s"($id) Applying schema changes is not yet implemented for the SQL engine")

  override def resetCachedSchema(): Unit = cachedExistingSchema = None
}
