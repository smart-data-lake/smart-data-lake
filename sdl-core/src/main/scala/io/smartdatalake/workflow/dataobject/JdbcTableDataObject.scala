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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import java.sql.{ResultSet, ResultSetMetaData, SQLException}
import scala.reflect.runtime.universe.Type
import scala.util.Try

/**
 * [[DataObject]] of type JDBC.
 * Provides details for an action to read and write tables in a database through JDBC.
 *
 * Reading and writing data is implemented by engine specific implementations of [[JdbcTableEngine]], which are
 * discovered on the classpath: sdl-spark reads and writes with Spark.
 *
 * Note that Sparks distributed processing can not directly write to a JDBC table in one transaction.
 * JdbcTableDataObject implements this in one transaction by writing to a temporary-table with Spark,
 * then using a separate "insert into ... select" SQL statement to copy data into the final table.
 *
 * JdbcTableDataObject implements
 * - [[CanMergeDataFrame]] by writing a temp table and using one SQL merge statement.
 * - [[CanEvolveSchema]] by generating corresponding alter table DDL statements.
 * - Overwriting partitions is implemented by using SQL delete and insert statement embedded in one transaction.
 *
 * Requires a [[JdbcTableConnection]] referenced by `connectionId`, which holds url, driver and credentials.
 *
 * Example:
 * {{{
 * connections = {
 *   jdbc-dwh {
 *     type = JdbcTableConnection
 *     url = "jdbc:postgresql://dwh:5432/mydb"
 *     driver = org.postgresql.Driver
 *   }
 * }
 * dataObjects = {
 *   int-airports {
 *     type = JdbcTableDataObject
 *     connectionId = jdbc-dwh
 *     table = { db = public, name = airports, primaryKey = [ident] }
 *     saveMode = Merge
 *   }
 * }
 * }}}
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
 * @param saveMode [[SDLSaveMode]] to use when writing table, default is "Overwrite". Only "Append", "Overwrite" and "Merge" are supported.
 *                 "Merge" requires a primary key to be defined on `table`.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param table The jdbc table to be read
 * @param jdbcFetchSize Number of rows to be fetched together by the Jdbc driver
 * @param connectionId Id of JdbcConnection configuration
 * @param jdbcOptions Any jdbc options according to [[https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html]].
 *                    Note that some options above set and override some of these options explicitly.
 *                    Use "createTableOptions" and "createTableColumnTypes" to control automatic creating of database tables.
 * @param virtualPartitions Virtual partition columns. Note that this doesn't need to be the same as the database partition
 *                   columns for this table. But it is important that there is an index on these columns to efficiently
 *                   list existing "partitions".
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param incrementalOutputExpr Optional expression to use for creating incremental output with DataObjectStateIncrementalMode.
 *                              The expression is used to get the high-water-mark for the incremental update state.
 *                              Normally this can be just a column name, e.g. an id or updated timestamp which is continually increasing.
 * @param constraints List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write.
 *                         E.g. it can be used to clean up or archive virtual partitions, see [[HousekeepingMode]].
 *                         Note that housekeeping works on `virtualPartitions`, so these need to be defined.
 *                         Default is None.
 */
case class JdbcTableDataObject(override val id: DataObjectId,
                               createSql: Option[String] = None,
                               override val preReadSql: Option[String] = None,
                               override val postReadSql: Option[String] = None,
                               override val preWriteSql: Option[String] = None,
                               override val postWriteSql: Option[String] = None,
                               override val schemaMin: Option[GenericSchema] = None,
                               override var table: Table,
                               override val constraints: Seq[Constraint] = Seq(),
                               override val expectations: Seq[Expectation] = Seq(),
                               jdbcFetchSize: Int = 1000,
                               saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               override val allowSchemaEvolution: Boolean = false,
                               connectionId: ConnectionId,
                               jdbcOptions: Map[String, String] = Map(),
                               virtualPartitions: Seq[String] = Seq(),
                               override val expectedPartitionsCondition: Option[String] = None,
                               incrementalOutputExpr: Option[String] = None,
                               override val housekeepingMode: Option[HousekeepingMode] = None,
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with HasEngineImplementation[JdbcTableEngine]
    with CanHandlePartitions with CanEvolveSchema with CanMergeDataFrame
    with CanCreateIncrementalOutput with ExpectationValidation with CanHandleConstraints
    with CanHandleForeignKeys with CanHandleCatalogMetadata with CanHandleTableSchema {

  /**
   * Connection defines driver, url and db in central location
   */
  val connection: JdbcTableConnection = getConnection[JdbcTableConnection](connectionId)

  val options: Map[String, String] = jdbcOptions ++ Map(
    "url" -> connection.url,
    "driver" -> connection.driver,
    "fetchSize" -> jdbcFetchSize.toString
  )

  // Define partition columns
  override val partitions: Seq[String] = if (Environment.caseSensitive) virtualPartitions else virtualPartitions.map(_.toLowerCase)

  // Note: Spark jdbc data source does not execute Spark observations, e.g. CopyWithMergeModeActionTest fails...
  // Using generic observations is forced therefore.
  override val forceGenericObservation = true

  // prepare final table
  table = table.overrideCatalogAndDb(None, connection.db)
  if(table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  /**
   * Temporary table used to write data before it is copied or merged into the final table in one transaction.
   */
  val tmpTable: Table = {
    val tmpTableName = if (connection.catalog.isQuotedIdentifier(table.name)) {
      connection.catalog.quoteIdentifier(connection.catalog.removeQuotes(table.name) + "_sdltmp")
    } else s"${table.name}_sdltmp"
    table.copy(name = tmpTableName)
  }

  assert(saveMode==SDLSaveMode.Append || saveMode==SDLSaveMode.Overwrite || saveMode==SDLSaveMode.Merge, s"($id) Only saveMode Append, Overwrite and Merge are supported.")

  override protected def createEngines: Seq[JdbcTableEngine] =
    DataObjectEngine.createEngines[JdbcTableEngine, JdbcTableDataObject](this, classOf[JdbcTableDataObject])

  override protected def engineNotFoundHint: String = "Add sdl-spark to the classpath to read and write with Spark."

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    // prepare housekeeping mode and validate lazy parsed schemas
    super.prepare

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

    //If enabled, create or replace the primary Key of the table

    // test partition columns exist
    if (virtualPartitions.nonEmpty && isTableExisting) {
      val missingPartitionColumns = partitions.toSet.diff(engine.getExistingSchema.get.columns.toSet)
      assert(missingPartitionColumns.isEmpty, s"($id) Virtual partition columns ${missingPartitionColumns.mkString(",")} missing in table definition")
    }

    if (isTableExisting)
      validateSchemaHasPrimaryKeyCols(engine.getDataFrame(Seq()).columns.toIndexedSeq, role = "prepare", obj = "Existing table")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = getSubFeedSupportedTypes.head)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val df = engine(subFeedType).getDataFrame(partitionValues)
    validateSchemaMin(df.schema, "read")
    df
  }

  override def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    DataFrameSubFeed.getCompanion(subFeedType).getSubFeed(getDataFrame(partitionValues, subFeedType), id, partitionValues)
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    engine(df.subFeedType).initDataFrame(df, partitionValues, saveModeOptions)
  }

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    require(table.query.isEmpty, s"writeDataFrame ($id): Cannot write to jdbc DataObject defined by a query.")
    engine(df.subFeedType).writeDataFrame(df, partitionValues, isRecursiveInput, saveModeOptions)
  }

  /**
   * Incremental output state. It is stored as tuple of incrementalOutputExpr, lastHighWatermarkValue and the SQL
   * of its data type. It is updated by the engine when reading data.
   */
  var incrementalOutputState: Option[(String,Option[(String,String)])] = None

  /**
   * Set state for incremental output.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    incrementalOutputState = state.map { s =>
      Try {
        s.split(';') match {
          case Array(column, lastHighWatermarkVal, dataType) => (column, Some((lastHighWatermarkVal, dataType)))
          case Array(column) => (column, None)
        }
      }.getOrElse(throw new IllegalStateException(s"($id) Cannot parse state '$s' into format <incrementalOutputExpr>;<lastHighWatermark>;<dataType>"))
    }.orElse{
      assert(incrementalOutputExpr.isDefined, s"($id) incrementalOutputExpr must be set to use DataObjectStateIncrementalMode")
      Some((incrementalOutputExpr.get, None))
    }
  }
  override def getState: Option[String] = {
    incrementalOutputState.map{
      case (column, Some((lastHighWatermarkVal, dataType))) => s"$column;$lastHighWatermarkVal;$dataType"
      case (column, None) => s"$column"
    }
  }

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    sqlOpt.foreach { sql =>
      val data = DefaultExpressionData.from(context, partitionValues)
      val preparedSql = ExpressionUtil.substitute(id, configName, sql, data)
      logger.info(s"prepareAndExecSql: ($id) ${configName.getOrElse("SQL")} is being executed: $preparedSql")
      connection.execJdbcStatement(preparedSql, logging = false)
    }
  }

  // cache response to avoid jdbc query.
  private var cachedIsDbExisting: Option[Boolean] = None
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsDbExisting.getOrElse {
      cachedIsDbExisting = Option(connection.catalog.isDbExisting(table.db.get))
      cachedIsDbExisting.get
    }
  }
  // cache if table is existing to avoid jdbc query.
  private var cachedIsTableExisting: Option[Boolean] = None
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsTableExisting.getOrElse {
      val existing = connection.catalog.isTableExisting(table.fullName)
      if (existing) cachedIsTableExisting = Some(existing) // only cache if existing, otherwise query again later
      existing
    }
  }
  def deleteAllDataStatement(): String = {
     s"delete from ${table.fullName}"
  }

  def deleteAllData(): Unit = {
    connection.execJdbcStatement(deleteAllDataStatement())
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    connection.dropTable(table.fullName)
  }

  /**
   * Listing virtual partitions by a "select distinct partition-columns" query
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (partitions.nonEmpty) engine.listPartitions
    else Seq()
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    if (partitionValues.nonEmpty) {
      connection.execJdbcStatement(deletePartitionsStatement(partitionValues))
    }
  }

  /**
   * Move virtual partitions by updating the partition columns with an "update" statement.
   * All updates are executed in one transaction.
   *
   * Note that in contrast to file based DataObjects no data is moved physically, only the value of the
   * virtual partition columns changes. If the target partition exists already, the records are merged into it.
   */
  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    if (partitionValues.nonEmpty) {
      val transaction = connection.beginTransaction()
      try {
        partitionValues.foreach { case (pvFrom, pvTo) =>
          transaction.execJdbcStatement(SQLUtil.createMovePartitionStatement(table.fullName, pvFrom, pvTo, quoteCaseSensitiveColumn(_)))
        }
        transaction.commit()
      } catch {
        case e: SQLException =>
          transaction.rollback()
          throw e
      }
    }
  }

  /**
   * Delete virtual partitions by "delete from" statement
   * @param partitionValues nonempty list of partition values
   */
  def deletePartitionsStatement(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): String = {
    SQLUtil.createDeletePartitionStatement(table.fullName, partitionValues, quoteCaseSensitiveColumn(_))
  }

  // jdbc column metadata - exact column metadata needed to check schema with case-sensitive column names
  private var _cachedJdbcColumnMetadata: Option[Seq[JdbcColumn]] = None
  def jdbcColumnMetadata(implicit context: ActionPipelineContext): Option[Seq[JdbcColumn]] = {
    if (isTableExisting && _cachedJdbcColumnMetadata.isEmpty) {
      // try reading from jdbc database metadata
      _cachedJdbcColumnMetadata = if (table.query.isEmpty) Try {
        connection.execWithJdbcConnection { con =>
          var rs: ResultSet = null
          try {
            // identifiers must be given in the case the database stores them in, see normalizeMetadataIdentifier
            rs = con.getMetaData.getColumns(null, connection.normalizeMetadataIdentifier(table.db.get), connection.normalizeMetadataIdentifier(table.name), null)
            class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
              def hasNext: Boolean = rs.next()
              def next(): ResultSet = rs
            }
            logger.info(s"jdbcColumnMetadata: ($id) get jdbc column metadata from database")
            new RsIterator(rs).map(JdbcColumn.from).toSeq
          } finally {
            if (rs != null) rs.close()
          }
        }
      }.toOption.filter(_.nonEmpty) else None
      // otherwise make empty query and use resultset metadata
      if (_cachedJdbcColumnMetadata.isEmpty) {
        val metadataQuery = table.query.getOrElse(s"select * from ${table.fullName}") + " where 1=0"
        logger.info(s"jdbcColumnMetadata: ($id) get jdbc column metadata from metadataQuery: $metadataQuery")
        def evalColumnNames(rs: ResultSet): Seq[JdbcColumn] = {
          (1 to rs.getMetaData.getColumnCount).map(i => JdbcColumn.from(rs.getMetaData, i))
        }
        _cachedJdbcColumnMetadata = Some(connection.execJdbcQuery(metadataQuery, evalColumnNames))
      }
    }
    _cachedJdbcColumnMetadata
  }
  def getJdbcColumn(sparkColName: String)(implicit context: ActionPipelineContext): Option[JdbcColumn] = {
    if (Environment.caseSensitive) jdbcColumnMetadata.flatMap(_.find(_.name == sparkColName))
    else jdbcColumnMetadata.flatMap(_.find(_.nameEqualsIgnoreCaseSensitive(sparkColName)))
  }

  // if we generate SQL statements with column names we need to care about quoting them properly
  def quoteCaseSensitiveColumn(column: String)(implicit context: ActionPipelineContext): String = {
    if (Environment.caseSensitive) connection.catalog.quoteIdentifier(column)
    else {
      val jdbcColumn = getJdbcColumn(column)
      if (jdbcColumn.isDefined) {
        if (jdbcColumn.get.isNameCaseSensitiv) connection.catalog.quoteIdentifier(jdbcColumn.get.name)
        else column
      } else {
        // quote identifier if it contains special characters
        if (SQLUtil.hasIdentifierSpecialChars(column)) connection.catalog.quoteIdentifier(column)
        else column
      }
    }
  }

  def getExistingPKConstraint(catalog: Option[String],
                                       schema: Option[String],
                                       tableName: String)(implicit context: ActionPipelineContext): Option[PrimaryKeyDefinition] = {
    connection.getJdbcPrimaryKey(catalog, schema, tableName)
  }

  def dropPrimaryKeyConstraint(tableName: String, constraintName: String)(implicit context: ActionPipelineContext): Unit =
    connection.catalog.dropPrimaryKeyConstraint(tableName, constraintName)

  def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String])(implicit context: ActionPipelineContext): Unit = {
    connection.catalog.createPrimaryKeyConstraint(tableName, constraintName, cols)
  }

  override def getExistingForeignKeys(implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition] = {
    connection.getJdbcForeignKeys(table.catalog, table.db, table.name)
  }

  override def dropForeignKeyConstraint(constraintName: String)(implicit context: ActionPipelineContext): Unit = {
    connection.catalog.dropForeignKeyConstraint(table.fullName, constraintName)
  }

  override def createForeignKeyConstraint(foreignKey: ForeignKeyDefinition)(implicit context: ActionPipelineContext): Unit = {
    connection.catalog.createForeignKeyConstraint(table.fullName, foreignKey, quoteCaseSensitiveColumn(_))
  }

  override def getTableComment(implicit context: ActionPipelineContext): Option[String] = {
    connection.getJdbcTableComment(table.db, table.name)
  }

  override def setTableComment(comment: String)(implicit context: ActionPipelineContext): Unit = {
    connection.execJdbcStatement(connection.catalog.getCommentOnTableSql(table.fullName, comment))
  }

  /**
   * Read the column comments from the JDBC metadata (REMARKS).
   * Note that not all JDBC drivers return the comments of the columns. If they are not returned, the comments
   * are written on every "apply" of CatalogSchemaUpdater, as it can not detect that they are up to date.
   */
  override def getColumnComments(implicit context: ActionPipelineContext): Map[Seq[String], String] = {
    jdbcColumnMetadata.toSeq.flatten.flatMap(col => col.comment.map(comment => Seq(col.name) -> comment)).toMap
  }

  override def setColumnComments(comments: Map[Seq[String], String])(implicit context: ActionPipelineContext): Unit = {
    comments.foreach { case (columnPath, comment) =>
      assert(columnPath.size == 1, s"($id) can not set the comment of nested column ${columnPath.mkString(".")}, jdbc tables have no nested columns")
      connection.execJdbcStatement(connection.catalog.getCommentOnColumnSql(table.fullName, quoteCaseSensitiveColumn(columnPath.head), comment))
    }
    resetCachedSchema()
  }

  /**
   * The schema of the existing table, with the nullability taken from the jdbc metadata.
   */
  override def getCurrentSchema(implicit context: ActionPipelineContext): Option[GenericSchema] = engine.getCurrentSchema

  /**
   * Create the table with the given schema, like it would be created on the first write,
   * see also attribute `createSql` to create it with a custom statement.
   */
  override def createTable(schema: GenericSchema)(implicit context: ActionPipelineContext): Unit = {
    engine.createTable(schema)
    resetCachedSchema()
    cachedIsTableExisting = None
    require(isTableExisting, s"($id) Strangely table ${table.fullName} doesn't exist even though we tried to create it")
  }

  /**
   * Apply the schema changes with "alter table" statements, see [[CanHandleTableSchema]].
   * This is the deployment time counterpart of the schema evolution applied on write when
   * `allowSchemaEvolution` is set.
   */
  override def applySchemaChanges(changes: Seq[TableSchemaChange])(implicit context: ActionPipelineContext): Unit = {
    engine.applySchemaChanges(changes)
    resetCachedSchema()
  }

  /**
   * Reset cached schema information, e.g. after the schema of the table has been changed.
   */
  def resetCachedSchema(): Unit = {
    _cachedJdbcColumnMetadata = None
    engines.foreach(_.resetCachedSchema())
  }

  /**
   * Reset the cached information if the table is existing, e.g. after it has been created.
   */
  def resetCachedIsTableExisting(): Unit = cachedIsTableExisting = None
}

/**
 * An engine specific implementation of reading and writing a [[JdbcTableDataObject]], see [[DataObjectEngine]].
 * Implementations must have a public constructor with the JdbcTableDataObject as single parameter.
 */
trait JdbcTableEngine extends DataObjectEngine {

  /**
   * Create a DataFrame reading the table, applying the incremental output state of the DataObject if set.
   */
  def getDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): GenericDataFrame

  /**
   * Validate the DataFrame to be written, and create or evolve the table if needed.
   */
  def initDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit

  /**
   * Write the DataFrame to the table according to the save mode.
   */
  def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap

  /**
   * The schema of the existing table, if it exists.
   */
  def getExistingSchema(implicit context: ActionPipelineContext): Option[GenericSchema]

  /**
   * The schema of the existing table, with the nullability taken from the jdbc metadata.
   */
  def getCurrentSchema(implicit context: ActionPipelineContext): Option[GenericSchema]

  /**
   * List the virtual partitions of the table.
   */
  def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues]

  /**
   * Create the table with the given schema.
   */
  def createTable(schema: GenericSchema)(implicit context: ActionPipelineContext): Unit

  /**
   * Apply schema changes with "alter table" statements.
   */
  def applySchemaChanges(changes: Seq[TableSchemaChange])(implicit context: ActionPipelineContext): Unit

  /**
   * Reset cached schema information.
   */
  def resetCachedSchema(): Unit
}

private[smartdatalake] case class JdbcColumn(name: String,
                                             isNameCaseSensitiv: Boolean,
                                             jdbcType: Option[Int] = None,
                                             dbTypeName: Option[String] = None,
                                             precision: Option[Int] = None,
                                             scale: Option[Int] = None,
                                             isNullable: Option[Boolean] = None,
                                             comment: Option[String] = None) {
  def nameEquals(other: JdbcColumn): Boolean = {
    if (this.isNameCaseSensitiv || other.isNameCaseSensitiv) this.name.equals(other.name)
    else this.name.equalsIgnoreCase(other.name)
  }
  def nameEqualsIgnoreCaseSensitive(name: String): Boolean = {
    this.name.equalsIgnoreCase(name)
  }
}
private[smartdatalake] object JdbcColumn {
  def from(metadata: ResultSetMetaData, colIdx: Int): JdbcColumn = {
    val name = metadata.getColumnName(colIdx)
    val isNameCaseSensitiv = name != name.toUpperCase || SQLUtil.hasIdentifierSpecialChars(name)
    JdbcColumn(name, isNameCaseSensitiv, Option(metadata.getColumnType(colIdx)), Option(metadata.getColumnTypeName(colIdx)), Option(metadata.getPrecision(colIdx)), Option(metadata.getScale(colIdx)), None)
  }
  def from(rs: ResultSet): JdbcColumn = {
    val name = rs.getString("COLUMN_NAME")
    val isNameCaseSensitiv = name != name.toUpperCase || SQLUtil.hasIdentifierSpecialChars(name)
    JdbcColumn(name, isNameCaseSensitiv, None, Option(rs.getString("DATA_TYPE")), Option(rs.getInt("COLUMN_SIZE")), Option(rs.getInt("DECIMAL_DIGITS")), Some(rs.getInt("NULLABLE")>0), Option(rs.getString("REMARKS")).filter(_.nonEmpty))
  }
}

object JdbcTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableDataObject = {
    extract[JdbcTableDataObject](config)
  }
}
