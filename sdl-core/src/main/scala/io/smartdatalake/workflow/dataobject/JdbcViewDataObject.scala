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
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.jdbc.JdbcConnectionImpl
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, SchemaViolationException}

import scala.reflect.runtime.universe.Type
import scala.util.{Failure, Success, Try}

/**
 * [[DataObject]] of a view in a database accessed through JDBC.
 *
 * Writing a DataFrame creates or replaces the view with the query of the DataFrame, i.e. `CREATE OR REPLACE VIEW ... AS
 * SELECT ...`, so no data is written. This needs an engine which can render its DataFrames as SQL, i.e. the SQL
 * engine of sdl-sql: the Action writing the view must use the JdbcConnection of the view as engine connection
 * (`engineConnectionId`), and all its inputs must be `JdbcTableDataObject`s or `JdbcViewDataObject`s of this
 * connection. The view is replaced in exec phase. In init phase a missing view is created, like a missing table is
 * created by JdbcTableDataObject, and the query is validated by executing it on the database without fetching any rows.
 *
 * With `allowSchemaEvolution = false`, an existing view is not replaced by an SDLB run, and the run fails if the columns
 * of the view changed. The view is then deployed with CatalogSchemaUpdater, from its query exported by a dry-run with
 * schema export, like the tables are created and migrated.
 *
 * As the view is evaluated on every read, its query must not depend on the current run. The Action writing the view
 * therefore ignores the partition values and filters of its inputs, and must not have an execution mode, see
 * [[ViewDataObject]].
 *
 * A view can have `virtualPartitions`, like a JdbcTableDataObject. The partition values of the Action writing the view
 * are then not applied to the view, but passed on to the next Action, which reads the view filtered by them.
 * With `incrementalOutputExpr`, the next Action can read the view incrementally with DataObjectStateIncrementalMode,
 * like a JdbcTableDataObject.
 *
 * Reading a view is the same as reading a table, it is done by all engines supporting [[JdbcTableDataObject]], e.g.
 * Spark. So a view created by the SQL engine can be the input of a Spark Action in the same feed.
 *
 * With `materialized = true` a materialized view is created, which stores the result of its query in the database.
 * It is supported for the SQL dialects postgres, redshift, oracle, snowflake and databricks. The Action writing it
 * refreshes it on every run, except Snowflake, which refreshes materialized views automatically. If its query changed
 * and `allowSchemaEvolution = true`, it is replaced instead. Postgres, Redshift and Oracle can not replace a
 * materialized view, so it is dropped and created again in one transaction (where the database supports
 * transactional DDL), and the privileges granted on it are read before and granted again afterwards (Postgres and
 * Oracle). Note that Postgres can not drop a materialized view while other views depend on it.
 * The query of an existing materialized view is read from pg_matviews for Postgres and from ALL_MVIEWS for Oracle.
 * For other databases it can not be compared, and the materialized view is replaced on every run.
 * To switch an existing view between materialized and not materialized, drop it first.
 *
 * Example:
 * {{{
 * dataObjects = {
 *   int-airports {
 *     type = JdbcTableDataObject
 *     connectionId = jdbc-dwh
 *     table = { db = public, name = airports }
 *   }
 *   btl-swiss-airports {
 *     type = JdbcViewDataObject
 *     connectionId = jdbc-dwh
 *     table = { db = public, name = swiss_airports }
 *   }
 * }
 * actions = {
 *   create-swiss-airports {
 *     type = CopyAction
 *     inputId = int-airports
 *     outputId = btl-swiss-airports
 *     engineConnectionId = jdbc-dwh
 *     transformers = [{
 *       type = SQLDfTransformer
 *       code = "select ident, name from %{inputViewName} where iso_country = 'CH'"
 *     }]
 *   }
 * }
 * }}}
 *
 * @param id unique name of this data object
 * @param table The view to be created and read. `query` is not supported.
 * @param connectionId Id of the JdbcConnection of the database
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param jdbcFetchSize Number of rows to be fetched together by the Jdbc driver when reading the view
 * @param jdbcOptions Any jdbc options for reading the view according to [[https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html]].
 * @param virtualPartitions Virtual partition columns, see JdbcTableDataObject. Partition values written to the view are
 *                   passed on to the next Action, and existing partitions are listed with a "select distinct" query.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param incrementalOutputExpr Optional expression to use for creating incremental output with DataObjectStateIncrementalMode.
 *                              The expression is used to get the high-water-mark for the incremental update state.
 *                              Normally this can be just a column name, e.g. an id or updated timestamp which is continually increasing.
 * @param allowSchemaEvolution If true (default), the view is replaced by every run of the Action writing it.
 *                             If false, an existing view is only replaced by CatalogSchemaUpdater, and a run fails if
 *                             the columns of the view changed.
 *                             For a materialized view, a run then only refreshes it.
 * @param materialized If true, a materialized view is created and refreshed on every run, see above. Default is false.
 */
case class JdbcViewDataObject(override val id: DataObjectId,
                              override var table: Table,
                              connectionId: ConnectionId,
                              override val schemaMin: Option[GenericSchema] = None,
                              jdbcFetchSize: Int = 1000,
                              jdbcOptions: Map[String, String] = Map(),
                              virtualPartitions: Seq[String] = Seq(),
                              override val expectedPartitionsCondition: Option[String] = None,
                              incrementalOutputExpr: Option[String] = None,
                              override val allowSchemaEvolution: Boolean = true,
                              materialized: Boolean = false,
                              override val metadata: Option[DataObjectMetadata] = None
                             )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject with CanWriteDataFrame with CanHandlePartitions with CanCreateIncrementalOutput with CanEvolveSchema with ViewDataObject {

  /**
   * Connection defines driver, url and db in central location
   */
  val connection: JdbcConnectionImpl = getConnection[JdbcConnectionImpl](connectionId)

  if (table.query.isDefined) throw ConfigurationException(s"($id) table.query is not supported for a view, the query of a view is defined by the Action writing it.", Some(s"dataObjects.$id.table.query"))

  // Define partition columns
  override val partitions: Seq[String] = if (Environment.caseSensitive) virtualPartitions else virtualPartitions.map(_.toLowerCase)

  // prepare final view name
  table = table.overrideCatalogAndDb(None, connection.db)
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  /**
   * Reading a view is the same as reading a table, so it is delegated to a JdbcTableDataObject for the view,
   * which supports all engines implementing [[JdbcTableEngine]].
   */
  @transient lazy val tableDataObject: JdbcTableDataObject = JdbcTableDataObject(id, table = table, connectionId = connectionId,
    schemaMin = schemaMin, jdbcFetchSize = jdbcFetchSize, jdbcOptions = jdbcOptions, virtualPartitions = virtualPartitions,
    expectedPartitionsCondition = expectedPartitionsCondition, incrementalOutputExpr = incrementalOutputExpr, metadata = metadata)

  @transient private lazy val viewEngines: Seq[JdbcViewEngine] =
    DataObjectEngine.createEngines[JdbcViewEngine, JdbcViewDataObject](this, classOf[JdbcViewDataObject])

  private def viewEngine(subFeedType: Type): JdbcViewEngine = viewEngines.find(_.subFeedType =:= subFeedType)
    .getOrElse(throw new IllegalStateException(s"($id) Can not create a view with subFeedType ${subFeedType.typeSymbol.name}." +
      s" Views are created by the SQL engine: add sdl-sql to the classpath, and use the connection $connectionId as engineConnectionId of the Action."))

  // the engine for creating a view from a query, e.g. by CatalogSchemaUpdater, where no Action defines the engine
  private def anyViewEngine: JdbcViewEngine = viewEngines.headOption
    .getOrElse(throw new IllegalStateException(s"($id) Can not create a view, as no engine for views is found. Add sdl-sql to the classpath."))

  // true if the view was created by this run, so that a materialized view is not refreshed again directly afterwards
  @transient private var createdInThisRun = false

  override def isMaterialized: Boolean = materialized

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    // fail early if the database does not support materialized views. Without engine for views it is only read.
    if (materialized) viewEngines.headOption.foreach(_.checkMaterializedViewSupported())
    // test connection and primary key columns of an existing view
    tableDataObject.prepare
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = getSubFeedSupportedTypes.head)(implicit context: ActionPipelineContext): GenericDataFrame =
    tableDataObject.getDataFrame(partitionValues, subFeedType)

  override def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed =
    DataFrameSubFeed.getCompanion(subFeedType).getSubFeed(getDataFrame(partitionValues, subFeedType), id, partitionValues)

  override def getSubFeedSupportedTypes: Seq[Type] = tableDataObject.getSubFeedSupportedTypes

  override def writeSubFeedSupportedTypes: Seq[Type] = viewEngines.map(_.subFeedType)

  private def validateWrite(df: GenericDataFrame, saveModeOptions: Option[SaveModeOptions]): Unit = {
    validateSchemaMin(df.schema, "write")
    validateSchemaHasPartitionCols(df.columns, "write")
    validateSchemaHasPrimaryKeyCols(df.columns, "write")
    saveModeOptions.map(_.saveMode).filter(_ != SDLSaveMode.Overwrite).foreach(saveMode =>
      throw ConfigurationException(s"($id) A view is always replaced, saveMode $saveMode is not supported."))
  }

  /**
   * Validate the DataFrame, and create the view if it does not exist. If it exists and `allowSchemaEvolution = false`,
   * the columns of the DataFrame must be the same as the columns of the view, as it is not replaced.
   */
  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    validateWrite(df, saveModeOptions)
    createdInThisRun = !isTableExisting
    if (createdInThisRun) {
      logger.info(s"($id) creating $viewKind ${table.fullName}")
      createOrReplaceView(getViewQuery(df))
    } else if (!allowSchemaEvolution) validateColumnsOfView(df)
    viewEngine(df.subFeedType).initDataFrame(df)
  }

  private def validateColumnsOfView(df: GenericDataFrame)(implicit context: ActionPipelineContext): Unit = {
    def normalize(columns: Seq[String]) = if (Environment.caseSensitive) columns else columns.map(_.toLowerCase)
    tableDataObject.getCurrentSchema.map(_.columns).foreach { viewColumns =>
      if (normalize(viewColumns) != normalize(df.columns)) throw new SchemaViolationException(
        s"($id) The columns of the DataFrame (${df.columns.mkString(", ")}) differ from the columns of view ${table.fullName} (${viewColumns.mkString(", ")})." +
          " The view is not replaced as allowSchemaEvolution = false, deploy it with CatalogSchemaUpdater.")
    }
  }

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    validateWrite(df, saveModeOptions)
    if (partitionValues.nonEmpty) logger.info(s"($id) partition values ${partitionValues.mkString(", ")} are not applied to the view, but passed on to the next Action")
    if (materialized) writeMaterializedView(getViewQuery(df))
    else if (allowSchemaEvolution) createOrReplaceView(getViewQuery(df))
    else logger.info(s"($id) view ${table.fullName} is not replaced as allowSchemaEvolution = false, it is deployed with CatalogSchemaUpdater")
    createdInThisRun = false
    Map()
  }

  /**
   * A materialized view is refreshed if its query is unchanged, and replaced otherwise if `allowSchemaEvolution = true`.
   * One created in init phase of this run is neither refreshed nor replaced.
   */
  private def writeMaterializedView(query: String)(implicit context: ActionPipelineContext): Unit = {
    if (!isTableExisting) {
      logger.info(s"($id) creating $viewKind ${table.fullName}")
      createOrReplaceView(query)
    } else if (createdInThisRun) {
      logger.info(s"($id) $viewKind ${table.fullName} is not refreshed, as it was created by this run")
    } else if (allowSchemaEvolution && !getExistingViewDefinition.exists(isSameViewQuery(_, query))) {
      logger.info(s"($id) replacing $viewKind ${table.fullName}, as its query changed")
      createOrReplaceView(query)
    } else {
      anyViewEngine.refreshMaterializedView()
    }
  }

  private def viewKind: String = if (materialized) "materialized view" else "view"

  override def getViewQuery(df: GenericDataFrame)(implicit context: ActionPipelineContext): String =
    viewEngine(df.subFeedType).renderQuery(df)

  override def getExistingViewDefinition(implicit context: ActionPipelineContext): Option[String] =
    if (materialized) connection.catalog.getMaterializedViewDefinition(table.db.get, table.name)
    else connection.catalog.getViewDefinition(table.db.get, table.name)

  override def isSameViewQuery(existingDefinition: String, query: String)(implicit context: ActionPipelineContext): Boolean = {
    val engine = anyViewEngine
    Try {
      // compare with the query as the database would store it, if supported
      val definition = connection.catalog.getViewDefinitionOfQuery(query).getOrElse(query)
      engine.normalizeQuery(existingDefinition) == engine.normalizeQuery(definition)
    } match {
      case Success(isSame) => isSame
      case Failure(e) =>
        logger.info(s"($id) definition of $viewKind ${table.fullName} can not be compared, it is replaced: ${e.getMessage}")
        false
    }
  }

  override def createOrReplaceView(query: String)(implicit context: ActionPipelineContext): Unit = {
    if (materialized) anyViewEngine.createOrReplaceMaterializedView(query)
    else anyViewEngine.createOrReplaceView(query)
    tableDataObject.resetCachedIsTableExisting()
    tableDataObject.resetCachedSchema()
  }

  /**
   * Listing virtual partitions by a "select distinct partition-columns" query on the view.
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = tableDataObject.listPartitions

  /**
   * Set state for incremental output, which is applied when reading the view, see JdbcTableDataObject.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = tableDataObject.setState(state)

  override def getState: Option[String] = tableDataObject.getState

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = tableDataObject.isDbExisting

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = tableDataObject.isTableExisting

  /**
   * Drop the view.
   */
  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    if (isTableExisting) connection.execJdbcStatement(s"DROP ${viewKind.toUpperCase} ${table.fullName}")
    tableDataObject.resetCachedIsTableExisting()
    tableDataObject.resetCachedSchema()
  }
}

/**
 * An engine specific implementation of creating the view of a [[JdbcViewDataObject]], see [[DataObjectEngine]].
 * Implementations must have a public constructor with the JdbcViewDataObject as single parameter.
 * Reading the view is implemented by the [[JdbcTableEngine]]s.
 */
trait JdbcViewEngine extends DataObjectEngine {

  /**
   * Validate the DataFrame to be written as view in init phase, without changing the database.
   */
  def initDataFrame(df: GenericDataFrame)(implicit context: ActionPipelineContext): Unit

  /**
   * The query of the DataFrame in the SQL dialect of the database.
   */
  def renderQuery(df: GenericDataFrame)(implicit context: ActionPipelineContext): String

  /**
   * Create or replace the view with the given query in the SQL dialect of the database.
   */
  def createOrReplaceView(query: String)(implicit context: ActionPipelineContext): Unit

  /**
   * Normalize the query of a view, to compare an existing view with a new query, see [[ViewDataObject.isSameViewQuery]].
   * The query can also be a `CREATE VIEW` statement, as some databases return the definition of a view like that.
   */
  def normalizeQuery(query: String)(implicit context: ActionPipelineContext): String

  /**
   * Throw an exception if the database does not support materialized views.
   */
  def checkMaterializedViewSupported()(implicit context: ActionPipelineContext): Unit

  /**
   * Create or replace the materialized view with the given query, keeping the privileges granted on it.
   */
  def createOrReplaceMaterializedView(query: String)(implicit context: ActionPipelineContext): Unit

  /**
   * Refresh the materialized view, i.e. store the current result of its query.
   */
  def refreshMaterializedView()(implicit context: ActionPipelineContext): Unit
}

object JdbcViewDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcViewDataObject = {
    extract[JdbcViewDataObject](config)
  }
}
