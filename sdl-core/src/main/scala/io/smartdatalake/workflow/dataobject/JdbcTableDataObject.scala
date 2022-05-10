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
package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.connection.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkField, SparkSchema}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{ResultSet, ResultSetMetaData}
import scala.util.Try

/**
 * [[DataObject]] of type JDBC.
 * Provides details for an action to read and write tables in a database through JDBC.
 * Note that writing into a table is done as one transaction. This is implemented by writing to a temporary-table with Spark,
 * then using a separate "insert into ... select" statement to copy data into the final table.
 *
 * @param id unique name of this data object
 * @param createSql DDL-statement to be executed in prepare phase, using output jdbc connection.
 *                  Note that it is also possible to let Spark create the table in Init-phase. See jdbcOptions to customize column data types for auto-created DDL-statement.
 * @param preReadSql SQL-statement to be executed in exec phase before reading input table, using input jdbc connection.
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param postReadSql SQL-statement to be executed in exec phase after reading input table and before action is finished, using input jdbc connection
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param preWriteSql SQL-statement to be executed in exec phase before writing output table, using output jdbc connection
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param postWriteSql SQL-statement to be executed in exec phase after writing output table, using output jdbc connection
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param saveMode [[SDLSaveMode]] to use when writing table, default is "Overwrite". Only "Append" and "Overwrite" supported.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param table The jdbc table to be read
 * @param jdbcFetchSize Number of rows to be fetched together by the Jdbc driver
 * @param connectionId Id of JdbcConnection configuration
 * @param jdbcOptions Any jdbc options according to [[https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html]].
 *                    Note that some options above set and override some of this options explicitly.
 *                    Use "createTableOptions" and "createTableColumnTypes" to control automatic creating of database tables.
 * @param virtualPartitions Virtual partition columns. Note that this doesn't need to be the same as the database partition
 *                   columns for this table. But it is important that there is an index on these columns to efficiently
 *                   list existing "partitions".
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param incrementalOutputColumn Optional column to use for creating incremental output with DataObjectStateIncrementalMode.
 */
case class JdbcTableDataObject(override val id: DataObjectId,
                               createSql: Option[String] = None,
                               preReadSql: Option[String] = None,
                               postReadSql: Option[String] = None,
                               preWriteSql: Option[String] = None,
                               postWriteSql: Option[String] = None,
                               override val schemaMin: Option[GenericSchema] = None,
                               override var table: Table,
                               jdbcFetchSize: Int = 1000,
                               saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               override val allowSchemaEvolution: Boolean = false,
                               connectionId: ConnectionId,
                               jdbcOptions: Map[String, String] = Map(),
                               virtualPartitions: Seq[String] = Seq(),
                               override val expectedPartitionsCondition: Option[String] = None,
                               incrementalOutputColumn: Option[String] = None,
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with CanHandlePartitions with CanEvolveSchema with CanMergeDataFrame with CanCreateIncrementalOutput {

  /**
   * Connection defines driver, url and db in central location
   */
  @DeveloperApi
  val connection: JdbcTableConnection = getConnection[JdbcTableConnection](connectionId)

  override val options = jdbcOptions ++ Map(
    "url" -> connection.url,
    "driver" -> connection.driver,
    "fetchSize" -> jdbcFetchSize.toString
  )

  // Define partition columns
  override val partitions: Seq[String] = if (SchemaUtil.isSparkCaseSensitive) virtualPartitions else virtualPartitions.map(_.toLowerCase)

  // prepare final table
  table = table.overrideDb(connection.db)
  if(table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  // prepare tmp table used for merge statement
  private val tmpTable = {
    val tmpTableName = if (connection.catalog.isQuotedIdentifier(table.name)) {
      connection.catalog.quoteIdentifier(connection.catalog.removeQuotes(table.name) + "_sdltmp")
    } else s"${table.name}_sdltmp"
    table.copy(name = tmpTableName)
  }

  assert(saveMode==SDLSaveMode.Append || saveMode==SDLSaveMode.Overwrite || saveMode==SDLSaveMode.Merge, s"($id) Only saveMode Append, Overwrite and Merge are supported.")

  override def prepare(implicit context: ActionPipelineContext): Unit = {

    // test connection
    try {
      connection.test()
    } catch {
      case ex: Throwable => throw ConnectionTestException(s"($id) Can not connect. Error: ${ex.getMessage}", ex)
    }

    // test table existing
    if (!isTableExisting) {
      createSql.foreach{ sql =>
        logger.info(s"($id) createSQL is being executed")
        connection.execJdbcStatement(sql)
      }
    }

    // test partition columns exist
    if (virtualPartitions.nonEmpty && isTableExisting) {
      val missingPartitionColumns = partitions.toSet.diff(getExistingSchema.get.fieldNames.toSet)
      assert(missingPartitionColumns.isEmpty, s"($id) Virtual partition columns ${missingPartitionColumns.mkString(",")} missing in table definition")
    }
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query",q)).getOrElse("dbtable"->table.fullName))
    var df = context.sparkSession.read.format("jdbc")
      .options(options)
      .options(connection.getAuthModeSparkOptions)
      .options(queryOrTable)
      .load()
    incrementalOutputState.foreach { case (lastColumn, lastWatermark)  =>
      assert(incrementalOutputColumn.isDefined)
      if (lastColumn != incrementalOutputColumn.get) logger.warn(s"($id) incrementalOutputState has different column as incrementalOutputColumn ($lastColumn != ${incrementalOutputColumn.get}")
      val newDataType = if (SchemaUtil.isSparkCaseSensitive) df.schema.find(_.name == incrementalOutputColumn.get).get.dataType
      else df.schema.find(_.name.equalsIgnoreCase(incrementalOutputColumn.get)).get.dataType
      if (context.phase == ExecutionPhase.Exec) {
        val newWatermarkValue = df.agg(max(col(incrementalOutputColumn.get))).head.get(0)
        incrementalOutputState = Some((incrementalOutputColumn.get, Some((newWatermarkValue.toString, newDataType))))
        logger.info(s"($id) incremental output selected records with '${incrementalOutputColumn.get} > '${lastWatermark.map(_._1).getOrElse("none")}' and <= '${newWatermarkValue}'")
        df = df.where(col(incrementalOutputColumn.get) <= lit(newWatermarkValue).cast(newDataType))
        lastWatermark.foreach { case (value, dataType) =>
          if (value == newWatermarkValue.toString) {
            throw NoDataToProcessWarning(id.id, s"No data to process found for $id by DataObjectStateIncrementalMode. Watermark is $newWatermarkValue")
          }
          df = df.where(col(lastColumn) > lit(value).cast(dataType))
        }
      }
    }
    validateSchemaMin(SparkSchema(df.schema), "read")
    df.colNamesLowercase
  }

  // Store incremental output state. It is stored as tuple of incrementalOutputColumn, lastWatermarkValue, dataType
  private var incrementalOutputState: Option[(String,Option[(String,DataType)])] = None

  /**
   * Set state for incremental output.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    incrementalOutputState = state.map { s =>
      Try {
        s.split(';') match {
          case Array(column, lastWatermarkVal, dataType) => (column, Some((lastWatermarkVal, DataType.fromDDL(dataType))))
          case Array(column) => (column, None)
        }
      }.getOrElse(throw new IllegalStateException(s"($id) Cannot parse state '$s' into format <incrementalOutputColumn>;<lastWatermark>;<dataType>"))
    }.orElse{
      assert(incrementalOutputColumn.isDefined, s"($id) incrementalOutputColumn must be set to use DataObjectStateIncrementalMode")
      Some((incrementalOutputColumn.get, None))
    }
  }
  override def getState: Option[String] = {
    incrementalOutputState.map{
      case (column, Some((lastWatermarkVal, dataType))) => s"$column;$lastWatermarkVal;${dataType.sql}"
      case (column, None) => s"$column"
    }
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    validateSchemaHasPrimaryKeyCols(df, table.primaryKey.getOrElse(Seq()), "write")
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df)
    if (isTableExisting) {
      if (allowSchemaEvolution) evolveTableSchema(saveModeTargetDf.schema)
      else validateSchemaOnWrite(saveModeTargetDf)
    } else {
      connection.createTableFromSchema(table.fullName, saveModeTargetDf.schema, options)
      require(isTableExisting, s"($id) Strangely table ${table.fullName} doesn't exist even though we tried to create it")
    }
  }

  /**
   * SDL Schema evolution allows to add new columns or change datatypes.
   * Deleted columns will remain in the table and are made nullable.
   */
  private def evolveTableSchema(newSchemaRaw: StructType)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    val existingSchema = SparkSchema(getExistingSchema.get)
    val newSchema = if (SchemaUtil.isSparkCaseSensitive) SparkSchema(newSchemaRaw) else SchemaUtil.prepareSchemaForDiff(SparkSchema(newSchemaRaw), ignoreNullable = false, caseSensitive = false).asInstanceOf[SparkSchema]
    // prepare changes
    val newColumns = newSchema.columns.diff(existingSchema.columns) // add new column
    val missingNotNullColumns = existingSchema.columns.diff(newSchema.columns) // make missing columns nullable
      .filter { col =>
        // as Spark doesn't know if a field is nullable in the database, but we can check jdbc metadata
        val jdbcColumn = getJdbcColumn(col)
        !jdbcColumn.flatMap(_.isNullable).getOrElse(false)
      }
    val newSchemaWithoutNewColumns = newSchema.filter(f => !newColumns.contains(f.name))
    val changedDatatypeColumns = SchemaUtil.schemaDiff(newSchemaWithoutNewColumns, existingSchema, ignoreNullable = true).map(_.asInstanceOf[SparkField]) // change column datatype if supported
    // apply changes
    if (newColumns.nonEmpty || missingNotNullColumns.nonEmpty || changedDatatypeColumns.nonEmpty)
      logger.info(s"($id) schema evolution needed: newColumns=${newColumns.mkString(",")} missingNotNullColumns=${missingNotNullColumns.mkString(",")} changedDatatypeColumns=${changedDatatypeColumns.map(f => s"${f.name}:${f.dataType.sql}").mkString(",")}")
    newColumns.foreach{ col =>
      val field = newSchema.inner(col)
      val sqlType = connection.catalog.getSqlType(field.dataType, isNullable = true) // new columns must be nullable because of existing data
      val sql = connection.catalog.getAddColumnSql(table.fullName, quoteCaseSensitiveColumn(col), sqlType)
      connection.execJdbcStatement(sql)
    }
    missingNotNullColumns.foreach{ col =>
      // as Spark doesn't now if a field is nullable in the database, but we can check jdbc metadata
      val jdbcColumn = getJdbcColumn(col)
      if (!jdbcColumn.flatMap(_.isNullable).getOrElse(false)) {
        val sql = connection.catalog.getAlterColumnNullableSql(table.fullName, quoteCaseSensitiveColumn(col))
        connection.execJdbcStatement(sql)
      }
    }
    changedDatatypeColumns.foreach { field =>
      val sqlType = connection.catalog.getSqlType(field.inner.dataType, field.nullable || existingSchema.inner(field.name).nullable)
      val sql = connection.catalog.getAlterColumnSql(table.fullName, quoteCaseSensitiveColumn(field.name), sqlType)
      connection.execJdbcStatement(sql)
    }
    // reset cached schema
    if (newColumns.nonEmpty || changedDatatypeColumns.nonEmpty) {
      cachedExistingSchema = None
      _cachedJdbcColumnMetadata = None
    }
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    require(table.query.isEmpty, s"($id) Cannot write to jdbc DataObject defined by a query.")
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    validateSchemaHasPrimaryKeyCols(df, table.primaryKey.getOrElse(Seq()), "write")
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df)
    if (!allowSchemaEvolution) validateSchemaOnWrite(saveModeTargetDf)

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // write
    finalSaveMode match {

      case SDLSaveMode.Overwrite =>
        // cleanup existing data if saveMode=overwrite, commit is done after append
        if (partitionValues.nonEmpty) deletePartitions(partitionValues, doCommit = false)
        else deleteAllData(doCommit = false)
        try {
          // create & write to temp-table
          writeToTempTable(df)
          // append into final table in one step, then commit
          connection.execJdbcStatement(s"insert into ${table.fullName} select * from $tmpTable", doCommit = true)
        } finally {
          // cleanup temp table
          connection.dropTable(tmpTable.fullName)
        }

      case SDLSaveMode.Merge =>
        // write to tmp-table and merge by primary key
        mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))

      case SDLSaveMode.Append =>
        // write target table with SaveMode.Append
        writeDataFrameInternalWithAppend(df, table.fullName)
    }
  }

  private def writeToTempTable(df: DataFrame)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    // cleanup temp table if existing
    if(connection.catalog.isTableExisting(tmpTable.fullName)) {
      logger.error(s"($id) Temporary table ${tmpTable.fullName} for merge already exists! There might be a potential conflict with another job. It will be dropped and recreated.")
      connection.dropTable(tmpTable.fullName)
    }
    // create & write to temp-table
    connection.createTableFromSchema(tmpTable.fullName, df.schema, options)
    writeDataFrameInternalWithAppend(df, tmpTable.fullName)
  }

  /**
   * Merges DataFrame with existing table data by writing DataFrame to a temp-table and using SQL Merge-statement.
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")

    try {
      // write data to temp table
      writeToTempTable(df)
      // prepare SQL merge statement
      val additionalMergePredicateStr = saveModeOptions.additionalMergePredicate.map(p => s" AND $p").getOrElse("")
      val joinConditionStr = table.primaryKey.get.map(quoteCaseSensitiveColumn).map(colName => s"new.$colName = existing.$colName").reduce(_+" AND "+_)
      val deleteClauseStr = saveModeOptions.deleteCondition.map(c => s"\nWHEN MATCHED AND $c THEN DELETE").getOrElse("")
      val updateConditionStr = saveModeOptions.updateCondition.map(c => s" AND $c").getOrElse("")
      val updateSpecStr = saveModeOptions.updateColumnsOpt.getOrElse(df.columns.toSeq.diff(table.primaryKey.get)).map(quoteCaseSensitiveColumn).map(colName => s"existing.$colName = new.$colName").reduce(_+", "+_)
      val insertConditionStr = saveModeOptions.insertCondition.map(c => s" AND $c").getOrElse("")
      val insertSpecStr = df.columns.diff(saveModeOptions.insertColumnsToIgnore).map(quoteCaseSensitiveColumn).reduce(_+", "+_)
      val insertValueSpecStr = df.columns.diff(saveModeOptions.insertColumnsToIgnore).map(colName => saveModeOptions.insertValuesOverride.getOrElse(colName, s"new.${quoteCaseSensitiveColumn(colName)}")).reduce(_+", "+_)
      val mergeStmt = s"""
        | MERGE INTO ${table.fullName} as existing
        | USING (SELECT * from ${tmpTable.fullName}) as new
        | ON $joinConditionStr $additionalMergePredicateStr $deleteClauseStr
        | WHEN MATCHED $updateConditionStr THEN UPDATE SET $updateSpecStr
        | WHEN NOT MATCHED $insertConditionStr THEN INSERT ($insertSpecStr) VALUES ($insertValueSpecStr)
        """.stripMargin
      // execute
      connection.execJdbcStatement(mergeStmt, doCommit = true)
    } finally {
      // cleanup temp table
      connection.dropTable(tmpTable.fullName)
    }
  }

  private def writeDataFrameInternalWithAppend(df: DataFrame, tableName: String): Unit = {
    // No need to define any partitions as parallelization will be defined according to the data frame's partitions
    df.write.mode(SaveMode.Append).format("jdbc")
      .options(options)
      .options(connection.getAuthModeSparkOptions)
      .option("dbtable", tableName)
      .save
  }

  override def preRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.preRead(partitionValues)
    prepareAndExecSql(preReadSql, Some("preReadSql"), partitionValues)
  }
  override def postRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postRead(partitionValues)
    prepareAndExecSql(postReadSql, Some("postReadSql"), partitionValues)
  }
  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    prepareAndExecSql(preWriteSql, Some("preWriteSql"), Seq()) // no partition values here...
  }
  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
    prepareAndExecSql(postWriteSql, Some("postWriteSql"), partitionValues)
  }
  private def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    sqlOpt.foreach { sql =>
      val data = DefaultExpressionData.from(context, partitionValues)
      val preparedSql = SparkExpressionUtil.substitute(id, configName, sql, data)
      logger.info(s"($id) ${configName.getOrElse("SQL")} is being executed: $preparedSql")
      connection.execJdbcStatement(preparedSql, logging = false)
    }
  }

  // cache response to avoid jdbc query.
  private var cachedIsDbExisting: Option[Boolean] = None
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    implicit val session: SparkSession = context.sparkSession
    cachedIsDbExisting.getOrElse {
      cachedIsDbExisting = Option(connection.catalog.isDbExisting(table.db.get))
      cachedIsDbExisting.get
    }
  }
  // cache if table is existing to avoid jdbc query.
  private var cachedIsTableExisting: Option[Boolean] = None
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    implicit val session: SparkSession = context.sparkSession
    cachedIsTableExisting.getOrElse {
      val existing = connection.catalog.isTableExisting(table.fullName)
      if (existing) cachedIsTableExisting = Some(existing) // only cache if existing, otherwise query again later
      existing
    }
  }
  // cache response to avoid jdbc query.
  private var cachedExistingSchema: Option[StructType] = None
  private def getExistingSchema(implicit context: ActionPipelineContext): Option[StructType] = {
    if (isTableExisting && cachedExistingSchema.isEmpty) {
      cachedExistingSchema = Some(getSparkDataFrame().schema)
      // convert to lowercase when Spark is in non-casesensitive mode
      if (!SchemaUtil.isSparkCaseSensitive) cachedExistingSchema = Some(SchemaUtil.prepareSchemaForDiff(SparkSchema(cachedExistingSchema.get), ignoreNullable = false, caseSensitive = true).asInstanceOf[SparkSchema].inner)
    }
    cachedExistingSchema
  }

  private def validateSchemaOnWrite(df: DataFrame)(implicit context: ActionPipelineContext): Unit = {
    getExistingSchema.foreach(schema => validateSchema(SparkSchema(df.schema), SparkSchema(schema), "write"))
  }

  def deleteAllData(doCommit: Boolean = true): Unit = {
    connection.execJdbcStatement(s"delete from ${table.fullName}", doCommit)
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    connection.dropTable(table.fullName)
  }

  override def factory: FromConfigFactory[DataObject] = JdbcTableDataObject

  /**
   * Listing virtual partitions by a "select distinct partition-columns" query
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (partitions.nonEmpty) {
      PartitionValues.fromDataFrame(getSparkDataFrame().select(partitions.map(col):_*).distinct)
    } else Seq()
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    deletePartitions(partitionValues)
  }

  /**
   * Delete virtual partitions by "delete from" statement
   */
  def deletePartitions(partitionValues: Seq[PartitionValues], doCommit: Boolean = true)(implicit context: ActionPipelineContext): Unit = {
    if (partitionValues.nonEmpty) {
      val partitionsColss = partitionValues.map(_.keys).distinct
      assert(partitionsColss.size == 1, "All partition values must have the same set of partition columns defined!")
      val partitionCols = partitionsColss.head
      val deletePartitionQuery = if (partitionCols.size == 1) {
        s"delete from ${table.fullName} where ${quoteCaseSensitiveColumn(partitionCols.head)} in ('${partitionValues.map(pv => pv(partitionCols.head)).mkString("','")}')"
      } else {
        val partitionValuesStr = partitionValues.map(pv => s"(${partitionCols.map(c => s"'${pv(c).toString}'").mkString(",")})")
        s"delete from ${table.fullName} where (${partitionCols.map(quoteCaseSensitiveColumn).mkString(",")}) in (${partitionValuesStr.mkString(",")})"
      }
      connection.execJdbcStatement(deletePartitionQuery, doCommit)
    }
  }

  // jdbc column metadata - exact column metadata needed to check schema with case-sensitive column names
  private var _cachedJdbcColumnMetadata: Option[Seq[JdbcColumn]] = None
  private def jdbcColumnMetadata(implicit context: ActionPipelineContext): Option[Seq[JdbcColumn]] = {
    if (isTableExisting && _cachedJdbcColumnMetadata.isEmpty) {
      // try reading from jdbc database metadata
      _cachedJdbcColumnMetadata = if (table.query.isEmpty) Try {
        connection.execWithJdbcConnection { con =>
          var rs: ResultSet = null
          try {
            rs = con.getMetaData.getColumns(null, connection.catalog.removeQuotes(table.db.get), connection.catalog.removeQuotes(table.name), null)
            class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
              def hasNext: Boolean = rs.next()
              def next(): ResultSet = rs
            }
            logger.info(s"($id) get jdbc column metadata from database")
            new RsIterator(rs).map(JdbcColumn.from).toSeq
          } finally {
            if (rs != null) rs.close()
          }
        }
      }.toOption else None
      // otherwise make empty query and use resultset metadata
      if (_cachedJdbcColumnMetadata.isEmpty) {
        val metadataQuery = table.query.getOrElse(s"select * from ${table.fullName}") + " where 1=0"
        def evalColumnNames(rs: ResultSet): Seq[JdbcColumn] = {
          (1 to rs.getMetaData.getColumnCount).map(i => JdbcColumn.from(rs.getMetaData, i))
        }
        logger.info(s"($id) get jdbc column metadata from query")
        _cachedJdbcColumnMetadata = Some(connection.execJdbcQuery(metadataQuery, evalColumnNames))
      }
    }
    _cachedJdbcColumnMetadata
  }
  private def getJdbcColumn(sparkColName: String)(implicit context: ActionPipelineContext): Option[JdbcColumn] = {
    if (SchemaUtil.isSparkCaseSensitive) jdbcColumnMetadata.flatMap(_.find(_.name == sparkColName))
    else jdbcColumnMetadata.flatMap(_.find(_.nameEqualsIgnoreCaseSensitive(sparkColName)))
  }

  // if we generate sql statements with column names we need to care about quoting them properly
  private def quoteCaseSensitiveColumn(column: String)(implicit context: ActionPipelineContext): String = {
    if (SchemaUtil.isSparkCaseSensitive) connection.catalog.quoteIdentifier(column)
    else {
      val jdbcColumn = getJdbcColumn(column)
      if (jdbcColumn.isDefined) {
        if (jdbcColumn.get.isNameCaseSensitiv) connection.catalog.quoteIdentifier(jdbcColumn.get.name)
        else column
      } else {
        // quote identifier if it contains special characters
        if (JdbcTableDataObject.hasIdentifierSpecialChars(column)) connection.catalog.quoteIdentifier(column)
        else column
      }
    }
  }
}

private[smartdatalake] case class JdbcColumn(name: String, isNameCaseSensitiv: Boolean, jdbcType: Option[Int] = None, dbTypeName: Option[String] = None, precision: Option[Int] = None, scale: Option[Int] = None, isNullable: Option[Boolean] = None) {
  def nameEquals(other: JdbcColumn): Boolean = {
    if (this.isNameCaseSensitiv || other.isNameCaseSensitiv) this.name.equals(other.name)
    else this.name.equalsIgnoreCase(other.name)
  }
  def nameEqualsIgnoreCaseSensitive(name: String): Boolean = {
    this.name.equalsIgnoreCase(name)
  }
  def getSparkField(schema: StructType): Option[StructField] = {
    if (isNameCaseSensitiv) schema.find(_.name.toLowerCase == name.toLowerCase)
    else schema.find(_.name == name)
  }
}
private[smartdatalake] object JdbcColumn {
  def from(metadata: ResultSetMetaData, colIdx: Int): JdbcColumn = {
    val name = metadata.getColumnName(colIdx)
    val isNameCaseSensitiv = name != name.toUpperCase || JdbcTableDataObject.hasIdentifierSpecialChars(name)
    JdbcColumn(name, isNameCaseSensitiv, Option(metadata.getColumnType(colIdx)), Option(metadata.getColumnTypeName(colIdx)), Option(metadata.getPrecision(colIdx)), Option(metadata.getScale(colIdx)), None)
  }
  def from(rs: ResultSet): JdbcColumn = {
    val name = rs.getString("COLUMN_NAME")
    val isNameCaseSensitiv = name != name.toUpperCase || JdbcTableDataObject.hasIdentifierSpecialChars(name)
    JdbcColumn(name, isNameCaseSensitiv, None, Option(rs.getString("DATA_TYPE")), Option(rs.getInt("COLUMN_SIZE")), Option(rs.getInt("DECIMAL_DIGITS")), Some(rs.getInt("NULLABLE")>0))
  }
}

object JdbcTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableDataObject = {
    extract[JdbcTableDataObject](config)
  }
  private[smartdatalake] def hasIdentifierSpecialChars(colName: String): Boolean = {
    !colName.matches("[a-zA-Z][a-zA-Z0-9_]*")
  }
}
