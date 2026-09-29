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

import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ProductUtil, SQLUtil, SchemaUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.{SparkExpressionUtil, SparkStageMetricsListener}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.connection.jdbc.SparkJdbcCatalog
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkDataType, SparkField, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericDataType, GenericSchema}
import io.smartdatalake.workflow.dataobject.generic.{AddColumn, ChangeColumnNullable, ChangeColumnType, TableSchemaChange}
import io.smartdatalake.workflow.DataFrameSubFeed
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.SQLException
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.{Failure, Success, Try}

/**
 * Classic Spark implementation of reading and writing a [[JdbcTableDataObject]], see [[JdbcTableEngine]].
 *
 * Data is read and written with the Spark jdbc data source. As Sparks distributed processing can not directly
 * write to a JDBC table in one transaction, data is written to a temporary table first, and copied or merged into
 * the final table with SQL statements in one transaction.
 */
class JdbcTableSparkClassicEngine(dataObject: JdbcTableDataObject) extends JdbcTableEngine with SmartDataLakeLogger {
  import dataObject.{connection, id, table, tmpTable, options}

  override def subFeedType: Type = typeOf[SparkSubFeed]

  // Spark creates tables with the dialect it determines itself, so the custom dialects must be registered upfront
  SparkJdbcCatalog.registerDialects()

  private lazy val sparkCatalog = new SparkJdbcCatalog(connection, connection.url)

  private def toSparkDataFrame(df: GenericDataFrame): DataFrame = df match {
    case sparkDf: SparkDataFrame => sparkDf.inner
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): GenericDataFrame =
    SparkDataFrame(getSparkDataFrame)

  private def getSparkDataFrame(implicit context: ActionPipelineContext): DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query",q)).getOrElse("dbtable"->table.fullName))
    logger.debug(s"getSparkDataFrame: queryOrTable = $queryOrTable")
    var df = getSparkSession.read.format("jdbc")
      .options(options)
      .options(connection.getAuthModeSparkOptions)
      .options(queryOrTable)
      .load()
    if (!context.isExecPhase) df = df.limit(1)
    dataObject.incrementalOutputState.foreach { case (lastExpr, lastHighWatermark)  =>
      val incrementalOutputExpr = dataObject.incrementalOutputExpr
      assert(incrementalOutputExpr.isDefined, s"($id) incrementalOutputExpr must be set to use DataObjectStateIncrementalMode")
      if (lastExpr != incrementalOutputExpr.get) logger.warn(s"($id) incrementalOutputState has different column as incrementalOutputExpr ($lastExpr != ${incrementalOutputExpr.get}")
      val resolvedExpr = SparkExpressionUtil.resolveExpression(incrementalOutputExpr.get, df.schema)
      // check if expression is fully resolved
      if (!resolvedExpr.resolved) {
        val attrs = ExpressionEvaluator.findUnresolvedAttributes(resolvedExpr).map(_.name)
        throw new IllegalStateException(s"($id) incrementalOutputExpr can not be resolved" + (if (attrs.nonEmpty) s", unresolved attributes are ${attrs.mkString(", ")}" else ""))
      }
      val newDataType = resolvedExpr.dataType
      if (context.isExecPhase) {
        val newHighWatermarkValue = Option(df.agg(max(expr(incrementalOutputExpr.get))).head().get(0))
          .getOrElse(throw NoDataToProcessWarning(id.id, s"No data to process found for $id by DataObjectStateIncrementalMode."))
        dataObject.incrementalOutputState = Some((incrementalOutputExpr.get, Some((newHighWatermarkValue.toString, newDataType.sql))))
        logger.info(s"getSparkDataFrame: ($id) incremental output selected records with" +
          s" '${incrementalOutputExpr.get} > '${lastHighWatermark.map(_._1).getOrElse("none")}'" +
          s" and <= '$newHighWatermarkValue'")
        df = df.where(expr(incrementalOutputExpr.get) <= lit(newHighWatermarkValue).cast(newDataType))
        lastHighWatermark.foreach { case (value, dataType) =>
          if (value == newHighWatermarkValue.toString) {
            throw NoDataToProcessWarning(id.id, s"No data to process found for $id by DataObjectStateIncrementalMode. High watermark is $newHighWatermarkValue")
          }
          df = df.where(expr(lastExpr) > lit(value).cast(DataType.fromDDL(dataType)))
        }
      }
    }
    df
  }

  override def initDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = {
    val sparkDf = toSparkDataFrame(df)
    dataObject.validateSchemaMin(df.schema, "write")
    dataObject.validateSchemaHasPartitionCols(sparkDf.columns.toSeq, "write")
    dataObject.validateSchemaHasPrimaryKeyCols(sparkDf.columns.toIndexedSeq, "write")
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df) match {
      case sparkDf: SparkDataFrame => sparkDf.inner
      case x => DataFrameSubFeed.throwIllegalSubFeedTypeException(x)
    }
    if (dataObject.isTableExisting) {
      if (dataObject.allowSchemaEvolution) evolveTableSchema(saveModeTargetDf.schema)
      else validateSchemaOnWrite(saveModeTargetDf)
    } else {
      sparkCatalog.createTableFromSchema(table.fullName, saveModeTargetDf.schema, options)
      require(dataObject.isTableExisting, s"($id) Strangely table ${table.fullName} doesn't exist even though we tried to create it")
    }
  }

  /**
   * SDL Schema evolution allows to add new columns or change datatypes.
   * Deleted columns will remain in the table and are made nullable.
   */
  private def evolveTableSchema(newSchemaRaw: StructType)(implicit context: ActionPipelineContext): Unit = {
    val existingSchema = SparkSchema(getExistingSparkSchema.get)
    val newSchema = if (Environment.caseSensitive) SparkSchema(newSchemaRaw) else SparkSchema(StructType(SchemaUtil.prepareSchemaForDiff(SparkSchema(newSchemaRaw).fields, ignoreNullable = false, caseSensitive = false).map(_.asInstanceOf[SparkField].inner)))
    // prepare changes
    val newColumns = newSchema.columns.diff(existingSchema.columns) // add new column
    val missingNotNullColumns = existingSchema.columns.diff(newSchema.columns) // make missing columns nullable
      .filter { col =>
        // as Spark doesn't know if a field is nullable in the database, but we can check jdbc metadata
        val jdbcColumn = dataObject.getJdbcColumn(col)
        !jdbcColumn.flatMap(_.isNullable).getOrElse(false)
      }
    val newSchemaWithoutNewColumns = newSchema.filter(f => !newColumns.contains(f.name))
    val changedDatatypeColumns = SchemaUtil.schemaDiff(newSchemaWithoutNewColumns, existingSchema, ignoreNullable = true).map(_.asInstanceOf[SparkField]) // change column datatype if supported
    // apply changes
    if (newColumns.nonEmpty || missingNotNullColumns.nonEmpty || changedDatatypeColumns.nonEmpty)
      logger.info(s"($id) schema evolution needed: newColumns=${newColumns.mkString(",")} missingNotNullColumns=${missingNotNullColumns.mkString(",")} changedDatatypeColumns=${changedDatatypeColumns.map(f => s"${f.name}:${f.dataType.sql}").mkString(",")}")
    newColumns.foreach{ col =>
      val field = newSchema.inner(col)
      val sqlType = sparkCatalog.getSqlType(field.dataType) // new columns must be nullable because of existing data
      val sql = sparkCatalog.getAddColumnSql(table.fullName, dataObject.quoteCaseSensitiveColumn(col), sqlType)
      connection.execJdbcStatement(sql)
    }
    missingNotNullColumns.foreach{ col =>
      // as Spark doesn't know if a field is nullable in the database, but we can check jdbc metadata
      val jdbcColumn = dataObject.getJdbcColumn(col)
      if (!jdbcColumn.flatMap(_.isNullable).getOrElse(false)) {
        val sql = sparkCatalog.getAlterColumnNullableSql(table.fullName, dataObject.quoteCaseSensitiveColumn(col))
        connection.execJdbcStatement(sql)
      }
    }
    changedDatatypeColumns.foreach { field =>
      val sqlType = sparkCatalog.getSqlType(field.inner.dataType, field.nullable || existingSchema.inner(field.name).nullable)
      val sql = sparkCatalog.getAlterColumnSql(table.fullName, dataObject.quoteCaseSensitiveColumn(field.name), sqlType)
      connection.execJdbcStatement(sql)
    }
    // reset cached schema
    if (newColumns.nonEmpty || changedDatatypeColumns.nonEmpty) dataObject.resetCachedSchema()
  }

  override def writeDataFrame(genericDf: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    val df = toSparkDataFrame(genericDf)
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(genericDf)).getOrElse(genericDf) match {
      case sparkDf: SparkDataFrame => sparkDf.inner
      case x => DataFrameSubFeed.throwIllegalSubFeedTypeException(x)
    }
    val targetSchema = targetDf.schema
    dataObject.validateSchemaMin(SparkSchema(targetSchema), "write")
    dataObject.validateSchemaHasPartitionCols(targetDf.columns.toSeq, "write")
    dataObject.validateSchemaHasPrimaryKeyCols(targetDf.columns.toIndexedSeq, "write")
    if (!dataObject.allowSchemaEvolution) validateSchemaOnWrite(targetDf)

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(dataObject.saveMode)

    // write
    val metMap: MetricsMap = Try (finalSaveMode match {

      case SDLSaveMode.Overwrite =>
        val metrics = overwriteTableWithDataframe(df, partitionValues)
        metrics ++ metrics.get("records_written").map("rows_inserted" -> _) // standardize inserted metric

      case SDLSaveMode.Merge =>
        // write to tmp-table and merge by primary key
        if (connection.directTableOverwrite) logger.warn(s"($id) directTableOverwrite=true can not be applied with SaveMode=Merge")
        mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions)
          .getOrElse(SaveModeMergeOptions()))

      case SDLSaveMode.Append =>
        // write target table with SaveMode.Append
        val metrics = writeDataFrameInternal(df, table.fullName, SaveMode.Append)
        metrics ++ metrics.get("records_written").map("rows_inserted" -> _) // standardize inserted metric
    }) match {
        case Success(m) => logger.debug(s"writeSparkDataFrame ($id):" +
          s" successfully written dataframe to jdbc table with metrics: $m")
          m
        case Failure(e) =>
          logger.error(s"writeSparkDataFrame ($id) failed. error message: ${e.getMessage}", e)
          logger.error(s"writeSparkDataFrame ($id) schema of dataFrame:")
          df.printSchema()
          throw e
    }

    metMap
  }

  private def overwriteTableWithDataframe(df: DataFrame, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): MetricsMap = {
    if (connection.directTableOverwrite || !dataObject.isTableExisting) {
      writeDataFrameInternal(df, table.fullName, SaveMode.Overwrite)
    } else try {
      // create & write to temp-table
      val tableSchema = getExistingSparkSchema.getOrElse(df.schema)
      val metrics = writeToTempTable(df, tableSchema)
      overwriteTableWithTempTableInTransaction(partitionValues)
      // return
      metrics
    } finally {
      // cleanup temp table
      connection.dropTable(tmpTable.fullName)
    }
  }

  private def overwriteTableWithTempTableInTransaction(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    val transaction = connection.beginTransaction()
    try {
      // cleanup existing data
      if (partitionValues.nonEmpty) transaction.execJdbcStatement(dataObject.deletePartitionsStatement(partitionValues))
      else transaction.execJdbcStatement(dataObject.deleteAllDataStatement())
      // append into final table in one step, then commit
      transaction.execJdbcStatement(s"insert into ${table.fullName} select * from ${tmpTable.fullName}")
      transaction.commit()
    } catch {
      case e: SQLException =>
        transaction.rollback()
        throw e
    }
  }

  private def writeToTempTable(df: DataFrame, tempTableSchema: StructType)(implicit context: ActionPipelineContext): MetricsMap = {
    // cleanup temp table if existing
    if(connection.catalog.isTableExisting(tmpTable.fullName)) {
      logger.error(s"($id) Temporary table ${tmpTable.fullName} already exists! There might be a potential conflict with another job. It will be dropped and recreated.")
      connection.dropTable(tmpTable.fullName)
    }
    // create & write to temp-table
    sparkCatalog.createTableFromSchema(tmpTable.fullName, tempTableSchema, options)
    writeDataFrameInternal(df, tmpTable.fullName, SaveMode.Append)
  }

  /**
   * Merges DataFrame with existing table data by writing DataFrame to a temp-table and using SQL Merge-statement.
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)
                                (implicit context: ActionPipelineContext): MetricsMap = {
    assert(table.primaryKey.exists(_.nonEmpty),
      s"mergeDataFrameByPrimaryKey: ($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")

    try {
      // write data to temp table
      val metrics: MetricsMap = writeToTempTable(df, df.schema)

      val updateExistingStatement = SQLUtil.createUpdateExistingStatement(table, df.columns.toSeq, tmpTable.fullName, saveModeOptions, dataObject.quoteCaseSensitiveColumn(_))
      updateExistingStatement.foreach{stmt =>
        logger.info(s"mergeDataFrameByPrimaryKey: ($id) executing update existing statement with options:" +
          s" ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1 + "=" + e._2).mkString(" ")}")
        connection.execJdbcDmlStatement(stmt)
      }

      // prepare SQL merge statement
      val mergeStmt = SQLUtil.createMergeStatement(table, df.columns.toSeq, tmpTable.fullName, saveModeOptions, dataObject.quoteCaseSensitiveColumn(_))
      // execute
      logger.info(s"mergeDataFrameByPrimaryKey: ($id) executing merge statement with options:" +
        s" ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1+"="+e._2).mkString(" ")}")
      logger.debug(s"mergeDataFrameByPrimaryKey: ($id) merge statement: $mergeStmt")
      val rowAffected = connection.execJdbcDmlStatement(mergeStmt)
      metrics + ("rows_affected" -> rowAffected)
    } finally {
      // cleanup temp table
      connection.dropTable(tmpTable.fullName)
    }
  }

  private def writeDataFrameInternal(df: DataFrame, tableName: String, saveMode: SaveMode)(implicit context: ActionPipelineContext): MetricsMap = {
    // No need to define any partitions as parallelization will be defined according to the data frame's partitions
    SparkStageMetricsListener.execWithMetrics(id,
      df.write.mode(saveMode).format("jdbc")
        .options(options)
        .options(connection.getAuthModeSparkOptions)
        .option("dbtable", tableName)
        .save()
    )
  }

  // cache response to avoid jdbc query.
  private var cachedExistingSchema: Option[StructType] = None
  private def getExistingSparkSchema(implicit context: ActionPipelineContext): Option[StructType] = {
    if (dataObject.isTableExisting && cachedExistingSchema.isEmpty) {
      cachedExistingSchema = Some(getSparkDataFrame.schema)
      // convert to lowercase when Spark is in non case-sensitive mode
      if (!Environment.caseSensitive) cachedExistingSchema = Some(StructType(SchemaUtil.prepareSchemaForDiff(SparkSchema(cachedExistingSchema.get).fields, ignoreNullable = false, caseSensitive = false).map(_.asInstanceOf[SparkField].inner)))
    }
    cachedExistingSchema
  }

  override def getExistingSchema(implicit context: ActionPipelineContext): Option[GenericSchema] = getExistingSparkSchema.map(SparkSchema)

  private def validateSchemaOnWrite(df: DataFrame)(implicit context: ActionPipelineContext): Unit = {
    getExistingSparkSchema.foreach(schema => dataObject.validateSchema(SparkSchema(df.schema), SparkSchema(schema), "write"))
  }

  /**
   * Listing virtual partitions by a "select distinct partition-columns" query
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    PartitionValues.fromDataFrame(SparkDataFrame(getSparkDataFrame.select(dataObject.partitions.map(col):_*).distinct()))
  }

  /**
   * The schema of the existing table, with the nullability taken from the jdbc metadata,
   * as Spark doesn't know if a field is nullable in the database.
   */
  override def getCurrentSchema(implicit context: ActionPipelineContext): Option[GenericSchema] = {
    getExistingSparkSchema.map { schema =>
      SparkSchema(StructType(schema.map(field =>
        dataObject.getJdbcColumn(field.name).flatMap(_.isNullable).map(nullable => field.copy(nullable = nullable)).getOrElse(field)
      )))
    }
  }

  override def createTable(schema: GenericSchema)(implicit context: ActionPipelineContext): Unit = {
    val sparkSchema = schema.convert(subFeedType) match {
      case sparkSchema: SparkSchema => sparkSchema.inner
      case otherSchema => throw new IllegalStateException(s"($id) can not create table from schema of type ${otherSchema.getClass.getSimpleName}")
    }
    sparkCatalog.createTableFromSchema(table.fullName, sparkSchema, options)
  }

  override def applySchemaChanges(changes: Seq[TableSchemaChange])(implicit context: ActionPipelineContext): Unit = {
    changes.foreach { change =>
      assert(change.columnPath.size == 1, s"($id) can not change nested column ${change.columnName}, jdbc tables have no nested columns")
      val column = dataObject.quoteCaseSensitiveColumn(change.columnPath.head)
      val sql = change match {
        // note that getSqlType creates a nullable column, which is needed as existing records have no value for it
        case AddColumn(_, dataType, _) => sparkCatalog.getAddColumnSql(table.fullName, column, sparkCatalog.getSqlType(toSparkDataType(dataType)))
        case ChangeColumnType(_, dataType, _) => sparkCatalog.getAlterColumnSql(table.fullName, column, sparkCatalog.getSqlType(toSparkDataType(dataType)))
        case ChangeColumnNullable(_, nullable) => sparkCatalog.getAlterColumnNullableSql(table.fullName, column, nullable)
      }
      connection.execJdbcStatement(sql)
    }
  }

  private def toSparkDataType(dataType: GenericDataType): DataType = dataType match {
    case sparkDataType: SparkDataType => sparkDataType.inner
    case otherDataType => throw new IllegalStateException(s"($id) unsupported data type ${otherDataType.getClass.getSimpleName}")
  }

  override def resetCachedSchema(): Unit = cachedExistingSchema = None
}
