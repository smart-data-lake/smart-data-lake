/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import com.snowflake.snowpark.Session
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.smartdatalake.{SnowparkDataFrame, SnowparkSession, SnowparkStructType}
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SnowflakeTableConnection
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME


/**
 * [[DataObject]] of type SnowflakeTableDataObject.
 * Provides details to access Snowflake tables via an action
 *
 * @param id           unique name of this data object
 * @param schemaMin    An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param table        Snowflake table to be written by this output
 * @param saveMode     spark [[SDLSaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId The SnowflakeTableConnection to use for the table
 * @param comment      An optional comment to add to the table after writing a DataFrame to it
 * @param metadata     meta data
 */
case class SnowflakeTableDataObject(override val id: DataObjectId,
                                    override val schemaMin: Option[SnowparkStructType] = None,
                                    override var table: Table,
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    connectionId: ConnectionId,
                                    comment: String,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSnowparkTableDataObject
    with CanCreateSnowparkDataFrame
    with CanWriteSnowparkDataFrame {

  /**
   * Connection defines connection string, credentials and database schema/name
   */
  private val connection = getConnection[SnowflakeTableConnection](connectionId)
  private var _snowparkSession: Option[Session] = None

  val session: Session = {
    if (_snowparkSession.isEmpty) {
      _snowparkSession = Some(connection.getSnowparkSession(table.db.get))
    }
    _snowparkSession.get
  }

  // prepare table
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) A SnowFlake schema name must be added as the 'db' parameter of a SnowflakeTableDataObject.")
  }

  override def getSnowparkDataFrame()(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    session.table(table.name)
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query", q)).getOrElse("dbtable" -> (connection.database + "." + table.fullName)))
    val df = context.sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(queryOrTable)
      .load()
    validateSchemaMin(df, "read")
    df.colNamesLowercase
  }

  override def writeDataFrame(df: SnowparkDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): Unit = {
    validateSchemaMin(df, "write")
    writeDataFrame(df, createTableOnly = false, partitionValues, saveModeOptions)
  }

  /**
   * Writes DataFrame to Snowflake
   * Snowflake does not support explicit partitions, so any passed partition values are ignored
   */
  def writeDataFrame(df: SnowparkDataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])
                    (implicit context: ActionPipelineContext): Unit = {
    implicit val session: SnowparkSession = context.sparkSession
    val dfPrepared = if (createTableOnly) {
      DataFrameUtil.getEmptyDataFrame(df.schema)
    } else {
      df
    }

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    dfPrepared.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(Map("dbtable" -> (connection.database + "." + table.fullName)))
      .mode(finalSaveMode.asSparkSaveMode)
      .save()

    if (comment != null && !comment.isEmpty) {
      val sql = s"comment on table ${connection.database}.${table.fullName} is '${comment}';"
      connection.execSnowflakeStatement(sql)
    }
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW DATABASES LIKE '${connection.database}'"
    connection.execSnowflakeStatement(sql).next()
  }


  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW TABLES LIKE '${table.name}' IN SCHEMA ${connection.database}.${table.db.get}"
    connection.execSnowflakeStatement(sql).next()
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = throw new NotImplementedError()

  override def factory: FromConfigFactory[DataObject] = SnowflakeTableDataObject

}

object SnowflakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SnowflakeTableDataObject = {
    extract[SnowflakeTableDataObject](config)
  }
}



