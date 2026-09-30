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
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.Type

/**
 * [[DataObject]] of a view in a database accessed through JDBC.
 *
 * Writing a DataFrame creates or replaces the view with the query of the DataFrame, i.e. `CREATE OR REPLACE VIEW ... AS
 * SELECT ...`, so no data is written. This needs an engine which can render its DataFrames as SQL, i.e. the SQL
 * engine of sdl-sql: the Action writing the view must use the JdbcTableConnection of the view as engine connection
 * (`engineConnectionId`), and all its inputs must be `JdbcTableDataObject`s or `JdbcViewDataObject`s of this
 * connection. The view is created in exec phase. In init phase the query is only validated by executing it on the
 * database without fetching any rows.
 *
 * As the view is evaluated on every read, its query must not depend on the current run. The Action writing the view
 * therefore ignores the partition values and filters of its inputs, and must not have an execution mode, see
 * [[ViewDataObject]].
 *
 * Reading a view is the same as reading a table, it is done by all engines supporting [[JdbcTableDataObject]], e.g.
 * Spark. So a view created by the SQL engine can be the input of a Spark Action in the same feed.
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
 * @param connectionId Id of the JdbcTableConnection of the database
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param jdbcFetchSize Number of rows to be fetched together by the Jdbc driver when reading the view
 * @param jdbcOptions Any jdbc options for reading the view according to [[https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html]].
 */
case class JdbcViewDataObject(override val id: DataObjectId,
                              override var table: Table,
                              connectionId: ConnectionId,
                              override val schemaMin: Option[GenericSchema] = None,
                              jdbcFetchSize: Int = 1000,
                              jdbcOptions: Map[String, String] = Map(),
                              override val metadata: Option[DataObjectMetadata] = None
                             )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject with CanWriteDataFrame with ViewDataObject {

  /**
   * Connection defines driver, url and db in central location
   */
  val connection: JdbcTableConnection = getConnection[JdbcTableConnection](connectionId)

  if (table.query.isDefined) throw ConfigurationException(s"($id) table.query is not supported for a view, the query of a view is defined by the Action writing it.", Some(s"dataObjects.$id.table.query"))

  // prepare final view name
  table = table.overrideCatalogAndDb(None, connection.db)
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  /**
   * Reading a view is the same as reading a table, so it is delegated to a JdbcTableDataObject for the view,
   * which supports all engines implementing [[JdbcTableEngine]].
   */
  @transient lazy val tableDataObject: JdbcTableDataObject = JdbcTableDataObject(id, table = table, connectionId = connectionId,
    schemaMin = schemaMin, jdbcFetchSize = jdbcFetchSize, jdbcOptions = jdbcOptions, metadata = metadata)

  @transient private lazy val viewEngines: Seq[JdbcViewEngine] =
    DataObjectEngine.createEngines[JdbcViewEngine, JdbcViewDataObject](this, classOf[JdbcViewDataObject])

  private def viewEngine(subFeedType: Type): JdbcViewEngine = viewEngines.find(_.subFeedType =:= subFeedType)
    .getOrElse(throw new IllegalStateException(s"($id) Can not create a view with subFeedType ${subFeedType.typeSymbol.name}." +
      s" Views are created by the SQL engine: add sdl-sql to the classpath, and use the connection $connectionId as engineConnectionId of the Action."))

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    // test connection and primary key columns of an existing view
    tableDataObject.prepare
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = getSubFeedSupportedTypes.head)(implicit context: ActionPipelineContext): GenericDataFrame =
    tableDataObject.getDataFrame(partitionValues, subFeedType)

  override def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed =
    DataFrameSubFeed.getCompanion(subFeedType).getSubFeed(getDataFrame(partitionValues, subFeedType), id, partitionValues)

  override def getSubFeedSupportedTypes: Seq[Type] = tableDataObject.getSubFeedSupportedTypes

  override def writeSubFeedSupportedTypes: Seq[Type] = viewEngines.map(_.subFeedType)

  private def validateWrite(partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions]): Unit = {
    require(partitionValues.isEmpty, s"($id) A view can not be written with partition values (${partitionValues.mkString(", ")})")
    saveModeOptions.map(_.saveMode).filter(_ != SDLSaveMode.Overwrite).foreach(saveMode =>
      throw ConfigurationException(s"($id) A view is always replaced, saveMode $saveMode is not supported."))
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    validateWrite(partitionValues, saveModeOptions)
    validateSchemaMin(df.schema, "write")
    validateSchemaHasPrimaryKeyCols(df.columns, "write")
    viewEngine(df.subFeedType).initDataFrame(df)
  }

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    validateWrite(partitionValues, saveModeOptions)
    validateSchemaMin(df.schema, "write")
    validateSchemaHasPrimaryKeyCols(df.columns, "write")
    val metrics = viewEngine(df.subFeedType).createOrReplaceView(df)
    tableDataObject.resetCachedIsTableExisting()
    tableDataObject.resetCachedSchema()
    metrics
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = tableDataObject.isDbExisting

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = tableDataObject.isTableExisting

  /**
   * Drop the view.
   */
  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    if (isTableExisting) connection.execJdbcStatement(s"DROP VIEW ${table.fullName}")
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
   * Create or replace the view with the query of the DataFrame.
   */
  def createOrReplaceView(df: GenericDataFrame)(implicit context: ActionPipelineContext): MetricsMap
}

object JdbcViewDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcViewDataObject = {
    extract[JdbcViewDataObject](config)
  }
}
